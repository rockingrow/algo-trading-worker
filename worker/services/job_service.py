"""
worker/services/job_service.py
───────────────────────────────
Background polling job that detects positions closed by the MT5 terminal
(hard SL, server-side TP, stop-out, or manual) without a ZMQ signal, then
fires the Telegram notification and broker API callback.
"""

from __future__ import annotations

import threading
from typing import Set

from worker.logger import get_logger
from worker.mt5.jobs import (
  TerminalCloseReason,
  TerminalClosedEvent,
  scan_terminal_closed_positions,
)
from worker.services.callback_service import CallbackService
from worker.services.db_service import DBService
from worker.services.notification_service import TelegramNotification
from worker.schemas.job_schema import LogAuthorEnum

log = get_logger("worker.services.job_service")

_POLL_INTERVAL = 5  # seconds

_REASON_ICON = {
  TerminalCloseReason.SL: "🛑",
  TerminalCloseReason.TP: "✅",
  TerminalCloseReason.STOP_OUT: "⚠️",
  TerminalCloseReason.MANUAL: "🖐",
}


def _box(text: str) -> str:
  return f"<pre>{text.strip()}</pre>"


class MT5EventJob:
  """
  Daemon thread that polls MT5 every *poll_interval* seconds for positions
  closed by the terminal without a corresponding ZMQ signal.

  On each detected closure:
    4.1  Sends a Telegram notification.
    4.2  Writes a row to order_logs with author='terminal'.
         Calls the broker API callback (notify_closed / notify_partially_closed).
  """

  def __init__(
    self,
    magic_number: int,
    callback_service: CallbackService,
    db_service: DBService,
    notifier: TelegramNotification,
    poll_interval: int = _POLL_INTERVAL,
  ) -> None:
    self._magic = magic_number
    self._callback = callback_service
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
        events = scan_terminal_closed_positions(self._magic, self._seen_tickets)
        for event in events:
          self._handle(event)
      except Exception as exc:
        log.exception("[MT5EventJob] Unexpected error during scan: %s", exc)
      self._stop_event.wait(self._poll_interval)

  # ── per-event handler ─────────────────────────────────────────────────── #

  def _handle(self, event: TerminalClosedEvent) -> None:
    log.info(
      "[MT5EventJob] Handling terminal close | ticket=%d reason=%s",
      event.source_ticket,
      event.close_reason.value,
    )

    # 4.2 — Write to DB with author='terminal'
    self._db.log_order(
      ticket=event.deal_ticket,
      source_ticket=event.source_ticket,
      symbol=event.symbol,
      action=event.close_reason.value,
      volume=event.close_volume,
      price=event.close_price,
      sl=event.sl,
      tp1=event.tp,
      mt5_retcode=0,
      comment=f"Terminal close [{event.close_reason.value}]",
      author=LogAuthorEnum.TERMINAL.value,
    )

    # 4.2 — Broker API callback
    if event.close_reason in (TerminalCloseReason.SL, TerminalCloseReason.STOP_OUT):
      self._callback.notify_closed(
        ticket=str(event.source_ticket),
        price=event.close_price,
        quantity=event.close_volume,
        sl=event.sl,
      )
    elif event.close_reason == TerminalCloseReason.TP:
      self._callback.notify_closed(
        ticket=str(event.source_ticket),
        price=event.close_price,
        quantity=event.close_volume,
      )
    # MANUAL: no broker API callback — intent is ambiguous.

    # 4.1 — Telegram
    acct = event.account
    acct_footer = (
      f"\n<b>Account:</b> {acct.login}\n"
      f"<b>Balance:</b> {acct.balance:.2f}\n"
      f"<b>Equity:</b> {acct.equity:.2f}\n"
      f"<b>Leverage:</b> {acct.leverage}"
      if acct
      else ""
    )
    icon = _REASON_ICON.get(event.close_reason, "❓")
    msg = _box(
      f"{icon} <b>Terminal Close [{event.close_reason.value}]</b>\n\n"
      f"Symbol: <b>{event.symbol}</b>\n"
      f"Reason: <b>{event.close_reason.value}</b>\n"
      f"Close Price: <b>{event.close_price}</b>\n"
      f"Volume: <b>{event.close_volume}</b>\n"
      f"Position: <b>{event.source_ticket}</b>\n"
      f"Deal: <b>{event.deal_ticket}</b>\n"
      f"Entry Price: <b>{event.entry_price}</b>\n"
      f"SL: <b>{event.sl}</b>\n"
      f"TP: <b>{event.tp}</b>"
      f"{acct_footer}"
    )
    self._notifier.send_message(msg)
