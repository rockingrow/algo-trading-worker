"""
worker/interfaces/db_protocol.py
────────────────────────────────
Segregated persistence interfaces.

The old single ``DBServiceProtocol`` forced every collaborator to depend on the
full database surface (positions *and* the notification outbox) even when it
only touched one of them. It is now split into role-specific protocols so each
client depends on the narrowest contract it needs (Interface Segregation):

  * :class:`PositionStoreProtocol`     — what ``SignalHandler`` needs.
  * :class:`NotificationOutboxProtocol` — what the outbox enqueue path needs.

``DBServiceProtocol`` is kept as the union for code that genuinely needs both.
"""

from typing import Any, Dict, List, Optional, Protocol

from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationModeEnum,
  NotificationPlatformEnum,
)
from worker.schemas.position_schema import PositionStatusEnum


class PositionStoreProtocol(Protocol):
  """Position lookups + status updates used by SignalHandler and close jobs."""

  def get_open_positions_by_strategy(
    self, strategy: str, symbol: str
  ) -> List[Dict[str, Any]]: ...
  def update_position_status(
    self,
    ref_source_id: str,
    status: PositionStatusEnum,
    ref_id: Optional[str] = None,
    closed_price: Optional[float] = None,
    gateway_return_code: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ) -> None: ...


class PositionSyncStoreProtocol(Protocol):
  """Change-data-capture access used by PositionCDC."""

  def get_pending_sync_positions(self) -> List[Dict[str, Any]]: ...
  def mark_position_synced(self, position_id: int, updated_at: str) -> bool: ...


class TerminalCloseStoreProtocol(Protocol):
  """Position read + log + status update used by MT5EventJob."""

  def get_position(self, ref_source_id: str) -> Optional[Dict[str, Any]]: ...
  def update_position_status(
    self,
    ref_source_id: str,
    status: PositionStatusEnum,
    ref_id: Optional[str] = None,
    closed_price: Optional[float] = None,
    gateway_return_code: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ) -> None: ...
  def log_position(
    self,
    strategy: str,
    ref_id: Optional[str],
    ref_source_id: Optional[str],
    symbol: str,
    action: str,
    volume: float,
    price: float,
    sl: Optional[float],
    tp1: Optional[float],
    gateway_return_code: int,
    comment: str = ...,
    message: Optional[str] = ...,
    author: str = ...,
  ) -> None: ...


class NotificationOutboxProtocol(Protocol):
  """Store-and-forward notification enqueue used by WorkerContext / NATS client."""

  def enqueue_notification(
    self,
    platform: NotificationPlatformEnum,
    channel: NotificationChannelEnum,
    message_text: str,
    category: Optional[str] = None,
    mode: NotificationModeEnum = NotificationModeEnum.VERBOSE,
  ) -> None: ...


class NotificationDispatchStoreProtocol(Protocol):
  """Outbox draining used by the NotificationJob dispatcher."""

  def get_due_notifications(self, limit: int = 20) -> List[Dict[str, Any]]: ...
  def delete_notification(self, notification_id: int) -> None: ...
  def mark_notification_failed(
    self, notification_id: int, error: str, next_attempt_at: str
  ) -> None: ...


class DBServiceProtocol(PositionStoreProtocol, NotificationOutboxProtocol, Protocol):
  """Union interface for code that needs both position and outbox access."""
