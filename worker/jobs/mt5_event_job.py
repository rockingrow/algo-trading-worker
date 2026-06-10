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
from typing import Set

from worker.gateways.mt5.close_detector import (
  TerminalClosedEvent,
  TerminalCloseReason,
  scan_terminal_closed_positions,
)
from worker.interfaces.db_protocol import TerminalCloseStoreProtocol
from worker.interfaces.message_sender_protocol import MessageSenderProtocol
from worker.logger import get_logger
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.services.notification_service import _box

log = get_logger("worker.jobs.mt5_event_job")

_POLL_INTERVAL = 5  # seconds

_REASON_ICON = {
  TerminalCloseReason.SL: "🛑",
  TerminalCloseReason.TP: "✅",
  TerminalCloseReason.STOP_OUT: "⚠️",
  TerminalCloseReason.MANUAL: "🖐",
}


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
    magic_numbers: Set[int],
    db_service: TerminalCloseStoreProtocol,
    notifier: MessageSenderProtocol,
    poll_interval: int = _POLL_INTERVAL,
  ) -> None:
    self._magics = set(magic_numbers)
    self._db = db_service
    self._notifier = notifier
    self._poll_interval = poll_interval
    self._seen_tickets: Set[int] = set()
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
      try:
        events = scan_terminal_closed_positions(self._magics, self._seen_tickets)
        for event in events:
          self._handle(event)
      except Exception as exc:
        log.exception("[MT5EventJob] Unexpected error during scan: %s", exc)
      self._stop_event.wait(self._poll_interval)

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
    icon = _REASON_ICON.get(event.close_reason, "❓")
    msg = _box(
      f"{icon} <b>Terminal Close [{event.close_reason.value}]</b>\n\n"
      f"Symbol: <b>{event.symbol}</b>\n"
      f"Reason: <b>{event.close_reason.value}</b>\n"
      f"Close Price: <b>{round(event.close_price, 2)}</b>\n"
      f"Volume: <b>{event.close_volume}</b>\n"
      f"Position: <b>{event.source_ticket}</b>\n"
      f"Deal: <b>{event.deal_ticket}</b>\n"
      f"Entry Price: <b>{round(event.entry_price, 2)}</b>\n"
      f"SL: <b>{round(event.sl, 2)}</b>\n"
      f"TP: <b>{round(event.tp, 2)}</b>\n"
      f"----------------------------------\n"
      f"{acct_footer}"
    )
    self._notifier.send_message(msg)
