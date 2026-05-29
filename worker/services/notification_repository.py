"""
worker/services/notification_repository.py
──────────────────────────────────────────
SQLite persistence for the ``notifications`` store-and-forward outbox.

Split out of the former monolithic ``DBService`` so the outbox has a single
reason to change, separate from position persistence (Single Responsibility).
"""

import sqlite3
from typing import Optional

from worker.db import _get_conn
from worker.logger import get_logger
from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationPlatformEnum,
)

logger = get_logger("worker.services.notification_repository")


class NotificationOutboxRepository:
  """Enqueues, fetches, and retires rows in the notification outbox."""

  def enqueue_notification(
    self,
    platform: NotificationPlatformEnum,
    channel: NotificationChannelEnum,
    message_text: str,
    category: Optional[str] = None,
    max_attempts: int = 5,
  ) -> None:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            INSERT INTO notifications (platform, channel, category, message_text, max_attempts)
            VALUES (?, ?, ?, ?, ?)
        """,
      (platform.value, channel.value, category, message_text, max_attempts),
    )
    conn.commit()
    conn.close()

  def get_due_notifications(self, limit: int = 20) -> list:
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute(
        """
                SELECT * FROM notifications
                WHERE attempts < max_attempts
                  AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
                ORDER BY id ASC
                LIMIT ?
            """,
        (limit,),
      )
      rows = cursor.fetchall()
      conn.close()
      return [dict(row) for row in rows]
    except Exception as e:
      logger.exception(f"Failed to fetch due notifications: {e}")
      return []

  def delete_notification(self, notification_id: int) -> None:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM notifications WHERE id = ?", (notification_id,))
    conn.commit()
    conn.close()

  def mark_notification_failed(self, notification_id: int, error: str, next_attempt_at: str) -> None:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            UPDATE notifications
            SET attempts = attempts + 1,
                last_error = ?,
                next_attempt_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
      (error, next_attempt_at, notification_id),
    )
    conn.commit()
    conn.close()
