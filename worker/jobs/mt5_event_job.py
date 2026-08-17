"""
worker/jobs/mt5_event_job.py
───────────────────────────────
Background polling job that detects positions closed by the MT5 terminal
(hard SL, server-side TP, stop-out, or manual) without a NATS signal, then
fires the Telegram notification and updates the SQLite positions table.
The NATS TRADE publisher (PositionCDC) propagates the status change
to the broker.
"""

from __future__ import annotations

import threading
from typing import Callable, Optional, Set, Union

from worker.gateways.forex.mt5.close_detector import (
  TerminalClosedEvent,
  TerminalCloseReason,
  scan_terminal_closed_positions,
)
from worker.gateways.message_presenter import pnl_line
from worker.icons import MANUAL, STOP, SUCCESS, UNKNOWN, WARNING
from worker.interfaces.db_protocol import TerminalCloseStoreProtocol
from worker.interfaces.message_sender_protocol import MessageSenderProtocol
from worker.logger import get_logger
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.services.notification_service import _box
from worker.settings import MT5_HEALTH_INTERVAL_WEEKEND, is_market_closed

log = get_logger("worker.jobs.mt5_event_job")

_POLL_INTERVAL = 5  # seconds

_REASON_ICON = {
  TerminalCloseReason.SL: STOP,
  TerminalCloseReason.TP: SUCCESS,
  TerminalCloseReason.STOP_OUT: WARNING,
  TerminalCloseReason.MANUAL: MANUAL,
}

#: Rendered in place of a price the platform did not give us.
_NO_PRICE = "—"


def _fmt_price(value: Optional[float]) -> str:
  """A price for display: rounded, or :data:`_NO_PRICE` when it is missing.

  ``sl``/``tp`` are read off the position's *opening order*, and
  ``history_orders_get`` can legitimately return nothing for it — pruned broker
  history, or a position opened outside the worker. They are ``Optional`` on
  :class:`TerminalClosedEvent` for exactly that reason, and ``round(None, 2)``
  raises, which used to abort the whole notification (and, before the per-event
  guard in :meth:`MT5EventJob._run`, every remaining close in the batch).
  """
  return _NO_PRICE if value is None else str(round(value, 2))


class MT5EventJob:
  """
  Daemon thread that polls MT5 every *poll_interval* seconds for positions
  closed by the terminal without a corresponding NATS signal.

  On each detected closure:
    4.1  Sends a Telegram notification.
    4.2  Writes a row to position_logs with author='terminal' and updates the
         positions table — the PositionCDC will then publish the status
         change to NATS TRADE so the broker can update its trades table.
  """

  def __init__(
    self,
    magic_numbers: Union[Set[int], Callable[[], Set[int]]],
    db_service: TerminalCloseStoreProtocol,
    notifier: MessageSenderProtocol,
    poll_interval: int = _POLL_INTERVAL,
    is_connected: Optional[Callable[[], bool]] = None,
  ) -> None:
    """*magic_numbers* should be a **callable** returning the magics this worker
    currently owns (``ForexExecutor.owned_magics``); a plain set is accepted and
    frozen for tests. *is_connected* reports the platform connection and gates
    polling — without it the job assumes the terminal is always reachable.
    """
    self._magics_fn: Callable[[], Set[int]] = (
      magic_numbers
      if callable(magic_numbers)
      else (lambda frozen=set(magic_numbers): frozen)
    )
    self._is_connected = is_connected
    self._db = db_service
    self._notifier = notifier
    self._poll_interval = poll_interval
    self._seen_tickets: Set[int] = set()
    self._warned_no_magics = False
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  # ── lifecycle ─────────────────────────────────────────────────────────── #

  def start(self, stop_event=None) -> None:
    """
    Start the background polling thread.

    *stop_event* can be a threading.Event or multiprocessing.Event — both
    expose the same .is_set() / .wait() interface.  When provided, the job
    shares the process-level shutdown signal so it stops cleanly with the rest
    of the worker.
    """
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run,
      name="mt5-event-job",
      daemon=True,
    )
    self._thread.start()
    log.info("[MT5EventJob] Started (poll_interval=%ds)", self._poll_interval)

  def stop(self) -> None:
    self._stop_event.set()

  # ── main loop ─────────────────────────────────────────────────────────── #

  def _run(self) -> None:
    while not self._stop_event.is_set():
      if not self._platform_ready():
        self._stop_event.wait(self._offline_interval())
        continue
      try:
        events = scan_terminal_closed_positions(
          self._current_magics(), self._seen_tickets
        )
      except Exception as exc:
        log.exception("[MT5EventJob] Unexpected error during scan: %s", exc)
        events = []
      for event in events:
        # Isolate each event. scan_terminal_closed_positions has already advanced
        # seen_tickets past this whole batch, so an exception escaping the loop
        # would silently drop every *remaining* close — and they never reappear,
        # because their tickets are no longer "disappeared" on the next scan.
        # (A failure inside _handle before the DB write leaves the row open, so
        # ForexReconcileJob still picks it up as its backstop.)
        try:
          self._handle(event)
        except Exception as exc:
          log.exception(
            "[MT5EventJob] Failed to handle terminal close | ticket=%s reason=%s: %s",
            event.source_ticket,
            event.close_reason.value,
            exc,
          )
      self._stop_event.wait(self._poll_interval)

  # ── Scan gating ───────────────────────────────────────────────────────── #

  def _platform_ready(self) -> bool:
    """Whether the terminal is reachable enough to be polled.

    Gated on the **live connection**, not on the FOREX weekend calendar. Once the
    health thread parks the connection for the weekend, positions_get() returns
    None and every scan would log a "terminal offline" warning — so skip quietly
    then. But a broker's 24/7 instruments (crypto CFDs such as BTCUSD) trade
    straight through that window with the terminal connected and orders filling;
    gating on the calendar made this job sleep through them, so a manual close
    over the weekend was never reported at all.
    """
    if self._is_connected is None or self._is_connected():
      return True
    log.debug("[MT5EventJob] Platform not connected — skipping scan.")
    return False

  def _offline_interval(self) -> int:
    """Wait before re-checking a disconnected terminal: the slow weekend cadence
    while the connection is parked for the market's weekly maintenance, otherwise
    the normal cadence so polling resumes as soon as the health thread
    reconnects."""
    return MT5_HEALTH_INTERVAL_WEEKEND if is_market_closed() else self._poll_interval

  def _current_magics(self) -> Set[int]:
    """The magics this worker owns, resolved fresh on every scan.

    The map is broker-owned and re-pushed on each WORKER_CONNECTED_ACK — including
    after a NATS reconnect — so a snapshot taken when the job was built goes stale
    and silently blinds close detection. An empty result means no ticket is ever
    tracked and this job is a no-op, which is worth one warning (repeated only
    after magics have come and gone again).
    """
    magics = set(self._magics_fn())
    if not magics:
      if not self._warned_no_magics:
        self._warned_no_magics = True
        log.warning(
          "[MT5EventJob] No owned magics — terminal-close detection is INACTIVE "
          "until the broker pushes a strategy_magic_map."
        )
    else:
      self._warned_no_magics = False
    return magics

  # ── per-event handler ─────────────────────────────────────────────────── #

  def _handle(self, event: TerminalClosedEvent) -> None:
    log.info(
      "[MT5EventJob] Handling terminal close | ticket=%s reason=%s",
      event.source_ticket,
      event.close_reason.value,
    )

    # Fetch position from DB to get strategy
    position = self._db.get_position(ref_source_id=event.source_ticket)
    if not position:
      log.warning(
        "[MT5EventJob] Position not found in DB | source_ticket=%s",
        event.source_ticket,
      )
      return
    strategy = position.get("strategy", "unknown")

    # 4.2 — Write to DB with author='terminal'
    self._db.log_position(
      strategy=strategy,
      ref_id=event.deal_ticket,
      ref_source_id=event.source_ticket,
      symbol=event.symbol,
      action=event.close_reason.value,
      volume=event.close_volume,
      price=event.close_price,
      sl=event.sl,
      tp1=event.tp,
      gateway_return_code=event.deal_reason,
      comment=f"Terminal close [{event.close_reason.value}]",
      author=LogAuthorEnum.TERMINAL.value,
    )
    self._db.update_position_status(
      ref_source_id=event.source_ticket,
      status=PositionStatusEnum.TERMINAL_CLOSED,
      ref_id=event.deal_ticket,
      closed_price=event.close_price,
      gateway_return_code=event.deal_reason,
      comment=f"Terminal close [{event.close_reason.value}]",
    )

    # 4.1 — Telegram
    acct = event.account
    acct_footer = (
      f"\n<b>Account:</b> {acct.login} ({acct.name})\n"
      f"<b>Balance:</b> {acct.balance:.2f}\n"
      f"<b>Equity:</b> {acct.equity:.2f}\n"
      f"<b>Leverage:</b> {acct.leverage}\n"
      f"<b>Margin:</b> {acct.margin:.2f}\n"
      f"<b>Free Margin:</b> {acct.free_margin:.2f}\n"
      f"<b>Margin Level:</b> {acct.margin_level:.2f}%\n"
      f"<b>Server:</b> {acct.server}\n"
      f"----------------------------------"
      if acct
      else ""
    )
    icon = _REASON_ICON.get(event.close_reason, UNKNOWN)
    msg = _box(
      f"{icon} <b>Terminal Close [{event.close_reason.value}]</b>\n\n"
      f"Symbol: <b>{event.symbol}</b>\n"
      f"Reason: <b>{event.close_reason.value}</b>\n"
      f"Close Price: <b>{_fmt_price(event.close_price)}</b>\n"
      f"Volume: <b>{event.close_volume}</b>\n"
      f"Position: <b>{event.source_ticket}</b>\n"
      f"Deal: <b>{event.deal_ticket}</b>\n"
      f"Entry Price: <b>{_fmt_price(event.entry_price)}</b>\n"
      f"SL: <b>{_fmt_price(event.sl)}</b>\n"
      f"TP: <b>{_fmt_price(event.tp)}</b>\n"
      f"{pnl_line(event.profit)}"
      f"----------------------------------\n"
      f"{acct_footer}"
    )
    self._notifier.send_message(msg)
