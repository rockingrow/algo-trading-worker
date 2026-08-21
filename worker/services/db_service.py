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
from worker.db.repository import (
  NotificationCycleRepository,
  NotificationOutboxRepository,
  PositionRepository,
)
from worker.logger import get_logger
from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationModeEnum,
  NotificationPlatformEnum,
)
from worker.schemas.position_schema import PositionStatusEnum

logger = get_logger("worker.services.db_service")


class DBService:
  """SQLite facade for positions, position logs, the outbox and signal cycles."""

  def __init__(
    self,
    positions: Optional[PositionRepository] = None,
    notifications: Optional[NotificationOutboxRepository] = None,
    cycles: Optional[NotificationCycleRepository] = None,
  ) -> None:
    self.positions = positions or PositionRepository()
    self.notifications = notifications or NotificationOutboxRepository()
    self.cycles = cycles or NotificationCycleRepository()

  def initialize(self):
    db_init()

  # ── Position delegation ──────────────────────────────────────────────── #

  def log_position(self, *args, **kwargs):
    return self.positions.log_position(*args, **kwargs)

  def insert_position(self, *args, **kwargs):
    return self.positions.insert_position(*args, **kwargs)

  def insert_rejected_position(self, *args, **kwargs):
    return self.positions.insert_rejected_position(*args, **kwargs)

  def update_position_status(
    self,
    ref_source_id: str,
    status: PositionStatusEnum,
    ref_id: Optional[str] = None,
    closed_price: Optional[float] = None,
    gateway_return_code: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ):
    return self.positions.update_position_status(
      ref_source_id, status, ref_id, closed_price, gateway_return_code, comment, message
    )

  def get_position(self, ref_source_id: str):
    return self.positions.get_position(ref_source_id)

  def get_pending_sync_positions(self) -> list:
    return self.positions.get_pending_sync_positions()

  def mark_position_synced(self, position_id: int, updated_at: str) -> bool:
    return self.positions.mark_position_synced(position_id, updated_at)

  def get_open_positions_by_strategy(self, strategy: str, symbol: str) -> list:
    return self.positions.get_open_positions_by_strategy(strategy, symbol)

  def get_open_positions_for_flat(
    self,
    strategy: Optional[str] = None,
    symbol: Optional[str] = None,
    ref_id: Optional[str] = None,
  ) -> list:
    return self.positions.get_open_positions_for_flat(strategy=strategy, symbol=symbol, ref_id=ref_id)

  def signal_exists(self, signal_id: str) -> bool:
    return self.positions.signal_exists(signal_id)

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

  # ── Signal-cycle notification delegation ─────────────────────────────── #

  def append_cycle_event(self, **kwargs):
    return self.cycles.append_event(**kwargs)

  def set_cycle_message_text(
    self, cycle_id: int, message_text: str, revision: int
  ) -> None:
    return self.cycles.set_message_text(cycle_id, message_text, revision)

  def ensure_cycle_chats(self, cycle_id: int, chat_ids) -> None:
    return self.cycles.ensure_cycle_chats(cycle_id, chat_ids)

  def get_due_cycle_deliveries(self, limit: int = 20) -> list:
    return self.cycles.get_due_cycle_deliveries(limit)

  def mark_cycle_delivered(
    self, chat_row_id: int, message_id: Optional[str], revision: int
  ) -> None:
    return self.cycles.mark_cycle_delivered(chat_row_id, message_id, revision)

  def mark_cycle_delivery_failed(
    self, chat_row_id: int, error: str, next_attempt_at: str
  ) -> None:
    return self.cycles.mark_cycle_delivery_failed(chat_row_id, error, next_attempt_at)

  def clear_cycle_message_id(self, chat_row_id: int) -> None:
    return self.cycles.clear_cycle_message_id(chat_row_id)
