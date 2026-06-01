"""
worker/services/db_service.py
─────────────────────────────
Backward-compatible facade over the persistence layer.

The actual SQL lives in :mod:`worker.db.repository`. ``DBService`` simply
composes the two repository classes and delegates, so existing call sites keep
working while new code can depend on the narrow repositories directly.
"""

from typing import Optional

from worker.db import db_init
from worker.db.repository import NotificationOutboxRepository, PositionRepository
from worker.logger import get_logger
from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationModeEnum,
  NotificationPlatformEnum,
)
from worker.schemas.position_schema import PositionStatusEnum

logger = get_logger("worker.services.db_service")


class DBService:
  """SQLite persistence facade for positions, position logs, and the outbox."""

  def __init__(
    self,
    positions: Optional[PositionRepository] = None,
    notifications: Optional[NotificationOutboxRepository] = None,
  ) -> None:
    self.positions = positions or PositionRepository()
    self.notifications = notifications or NotificationOutboxRepository()

  def initialize(self):
    db_init()

  # ── Position delegation ──────────────────────────────────────────────── #

  def log_position(self, *args, **kwargs):
    return self.positions.log_position(*args, **kwargs)

  def insert_position(self, *args, **kwargs):
    return self.positions.insert_position(*args, **kwargs)

  def update_position_status(
    self,
    source_ticket: int,
    status: PositionStatusEnum,
    new_ticket: Optional[int] = None,
    closed_price: Optional[float] = None,
    mt5_retcode: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ):
    return self.positions.update_position_status(
      source_ticket, status, new_ticket, closed_price, mt5_retcode, comment, message
    )

  def get_position(self, source_ticket: int):
    return self.positions.get_position(source_ticket)

  def get_pending_sync_positions(self) -> list:
    return self.positions.get_pending_sync_positions()

  def mark_position_synced(self, position_id: int, updated_at: str) -> bool:
    return self.positions.mark_position_synced(position_id, updated_at)

  def get_open_positions_by_strategy(self, strategy: str, symbol: str) -> list:
    return self.positions.get_open_positions_by_strategy(strategy, symbol)

  # ── Notification outbox delegation ───────────────────────────────────── #

  def enqueue_notification(
    self,
    platform: NotificationPlatformEnum,
    channel: NotificationChannelEnum,
    message_text: str,
    category: Optional[str] = None,
    mode: NotificationModeEnum = NotificationModeEnum.VERBOSE,
    max_attempts: int = 5,
  ) -> None:
    return self.notifications.enqueue_notification(
      platform, channel, message_text, category, mode, max_attempts
    )

  def get_due_notifications(self, limit: int = 20) -> list:
    return self.notifications.get_due_notifications(limit)

  def delete_notification(self, notification_id: int) -> None:
    return self.notifications.delete_notification(notification_id)

  def mark_notification_failed(
    self, notification_id: int, error: str, next_attempt_at: str
  ) -> None:
    return self.notifications.mark_notification_failed(
      notification_id, error, next_attempt_at
    )
