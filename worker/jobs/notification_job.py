"""
worker/jobs/notification_job.py
──────────────────────────────────
Dispatcher that drains the notifications outbox table and sends pending
messages via Telegram. Mirrors the PositionCDC / MT5EventJob pattern:
daemon thread, shared stop_event, poll loop.

Delivery semantics:
  - Success  → DELETE the row (hard-delete, no status column needed).
  - Failure  → increment attempts + set next_attempt_at (exponential backoff).
  - Dead     → rows where attempts >= max_attempts are left in place as
               dead-letter and skipped by the poll query.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Dict

from worker.interfaces.db_protocol import NotificationDispatchStoreProtocol
from worker.interfaces.message_sender_protocol import MessageSenderProtocol
from worker.logger import get_logger

log = get_logger("worker.jobs.notification_job")

_POLL_INTERVAL = 1  # seconds
_BACKOFF_SECONDS = [5, 30, 120, 600]  # indexed by attempt number (capped at last)


def _next_attempt_at(attempts: int) -> str:
  idx = min(attempts, len(_BACKOFF_SECONDS) - 1)
  dt = datetime.utcnow() + timedelta(seconds=_BACKOFF_SECONDS[idx])
  return dt.strftime("%Y-%m-%d %H:%M:%S")


class NotificationJob:
  """
  Daemon thread that polls the notifications table every *poll_interval*
  seconds and dispatches due rows via the appropriate TelegramNotification.

  *notifiers* maps channel value strings (e.g. 'INDIVIDUAL', 'COMMUNITY')
  to direct TelegramNotification senders — these are the actual senders,
  not outbox wrappers.
  """

  def __init__(
    self,
    db_service: NotificationDispatchStoreProtocol,
    notifiers: Dict[str, MessageSenderProtocol],
    poll_interval: int = _POLL_INTERVAL,
  ) -> None:
    self._db = db_service
    self._notifiers = notifiers
    self._poll_interval = poll_interval
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  def start(self, stop_event=None) -> None:
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run,
      name="notification-dispatcher",
      daemon=True,
    )
    self._thread.start()
    log.info("[NotificationJob] Started (poll_interval=%ds)", self._poll_interval)

  def stop(self) -> None:
    self._stop_event.set()

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        self._poll()
      except Exception as exc:
        log.exception("[NotificationJob] Unexpected error during poll: %s", exc)
      self._stop_event.wait(self._poll_interval)

  def _poll(self) -> None:
    rows = self._db.get_due_notifications()
    for row in rows:
      notifier = self._notifiers.get(row["channel"])
      if notifier is None:
        log.warning(
          "[NotificationJob] No notifier configured for channel=%s id=%s — skipping",
          row["channel"],
          row["id"],
        )
        continue

      success = notifier.send_message(row["message_text"])
      if success:
        self._db.delete_notification(row["id"])
        log.debug("[NotificationJob] Sent and deleted notification id=%s category=%s", row["id"], row.get("category"))
      else:
        next_at = _next_attempt_at(row["attempts"])
        self._db.mark_notification_failed(
          notification_id=row["id"],
          error=f"Send failed (attempt {row['attempts'] + 1})",
          next_attempt_at=next_at,
        )
        log.warning(
          "[NotificationJob] Failed id=%s attempts=%s next_attempt_at=%s",
          row["id"],
          row["attempts"] + 1,
          next_at,
        )
