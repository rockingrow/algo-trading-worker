"""
worker/jobs/cycle_notification_job.py
─────────────────────────────────────
Reader half of the one-message-per-trade Telegram notification: drains the
signal-cycle tables and brings every chat's message up to date.

Mirrors :class:`~worker.jobs.notification_job.NotificationJob` — daemon thread,
shared stop_event, poll loop, exponential backoff — but the delivery semantics
are the opposite of an outbox's. An outbox row is a message sent once and then
deleted; a cycle row is a conversation whose one message is **edited** every
time the trade moves, so nothing is ever deleted and "delivered" means "this
chat has been shown revision N".

Per due row:
  - No message id yet → sendMessage, store the id it returns.
  - Message id present → editMessageText.
  - Edit came back MISSING (deleted from the chat) → forget the id; the next
    pass posts a fresh message.
  - Anything else failed → increment attempts + set next_attempt_at. Rows that
    exhaust max_attempts are left as dead letters and skipped by the poll query,
    exactly like the outbox.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta

from worker.interfaces.db_protocol import CycleDispatchStoreProtocol
from worker.logger import get_logger
from worker.services.notification_service import EditOutcome, TelegramCycleNotification

log = get_logger("worker.jobs.cycle_notification_job")

_POLL_INTERVAL = 1  # seconds
_BACKOFF_SECONDS = [5, 30, 120, 600]  # indexed by attempt number (capped at last)


def _next_attempt_at(attempts: int) -> str:
  idx = min(attempts, len(_BACKOFF_SECONDS) - 1)
  dt = datetime.utcnow() + timedelta(seconds=_BACKOFF_SECONDS[idx])
  return dt.strftime("%Y-%m-%d %H:%M:%S")


class CycleNotificationJob:
  """Daemon thread that keeps each chat's cycle message in step with its cycle."""

  def __init__(
    self,
    db_service: CycleDispatchStoreProtocol,
    sender: TelegramCycleNotification,
    poll_interval: int = _POLL_INTERVAL,
  ) -> None:
    self._db = db_service
    self._sender = sender
    self._poll_interval = poll_interval
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  def start(self, stop_event=None) -> None:
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run,
      name="cycle-notification-dispatcher",
      daemon=True,
    )
    self._thread.start()
    log.info("[CycleNotificationJob] Started (poll_interval=%ds)", self._poll_interval)

  def stop(self) -> None:
    self._stop_event.set()

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        self._poll()
      except Exception as exc:
        log.exception("[CycleNotificationJob] Unexpected error during poll: %s", exc)
      self._stop_event.wait(self._poll_interval)

  def _poll(self) -> None:
    for row in self._db.get_due_cycle_deliveries():
      if row.get("message_id"):
        self._edit(row)
      else:
        self._send(row)

  def _send(self, row: dict) -> None:
    message_id = self._sender.send(row["chat_id"], row["message_text"])
    if message_id is None:
      self._fail(row, "sendMessage failed")
      return
    self._db.mark_cycle_delivered(row["chat_row_id"], message_id, row["revision"])
    log.debug(
      "[CycleNotificationJob] Sent cycle=%s chat=%s message_id=%s revision=%s",
      row["signal_uxid"],
      row["chat_id"],
      message_id,
      row["revision"],
    )

  def _edit(self, row: dict) -> None:
    outcome = self._sender.edit(row["chat_id"], row["message_id"], row["message_text"])
    if outcome is EditOutcome.OK:
      # message_id is unchanged by an edit — pass None so the stored id stands.
      self._db.mark_cycle_delivered(row["chat_row_id"], None, row["revision"])
      log.debug(
        "[CycleNotificationJob] Edited cycle=%s chat=%s revision=%s",
        row["signal_uxid"],
        row["chat_id"],
        row["revision"],
      )
      return

    if outcome is EditOutcome.MISSING:
      # The chat no longer has the message (deleted, or too old to edit). Drop
      # the id and leave the row due: the next pass re-posts the whole cycle,
      # which is complete on its own because the body is rendered in full.
      self._db.clear_cycle_message_id(row["chat_row_id"])
      log.warning(
        "[CycleNotificationJob] Message gone for cycle=%s chat=%s — will re-send",
        row["signal_uxid"],
        row["chat_id"],
      )
      return

    self._fail(row, "editMessageText failed")

  def _fail(self, row: dict, error: str) -> None:
    attempts = row.get("attempts", 0)
    next_at = _next_attempt_at(attempts)
    self._db.mark_cycle_delivery_failed(
      chat_row_id=row["chat_row_id"],
      error=f"{error} (attempt {attempts + 1})",
      next_attempt_at=next_at,
    )
    log.warning(
      "[CycleNotificationJob] Failed cycle=%s chat=%s attempts=%s next_attempt_at=%s",
      row["signal_uxid"],
      row["chat_id"],
      attempts + 1,
      next_at,
    )
