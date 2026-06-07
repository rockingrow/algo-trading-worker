"""Verifies DBService is a thin facade delegating to the two repositories."""

from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationPlatformEnum,
)
from worker.schemas.position_schema import PositionStatusEnum
from worker.services.db_service import DBService


class RecordingRepo:
  def __init__(self):
    self.calls = []

  def __getattr__(self, name):
    def recorder(*args, **kwargs):
      self.calls.append((name, args, kwargs))
      return ("RESULT", name)

    return recorder


def make_service():
  positions = RecordingRepo()
  notifications = RecordingRepo()
  return DBService(positions=positions, notifications=notifications), positions, notifications


def test_position_methods_delegate_to_position_repo():
  svc, positions, _ = make_service()
  svc.get_open_positions_by_strategy("s", "XAUUSD")
  svc.get_open_positions_for_flat(strategy="s", symbol="XAUUSD")
  svc.update_position_status(source_ticket=1, status=PositionStatusEnum.TP1)
  svc.get_position(1)
  svc.get_pending_sync_positions()
  svc.mark_position_synced(1, "ts")
  names = [c[0] for c in positions.calls]
  assert names == [
    "get_open_positions_by_strategy",
    "get_open_positions_for_flat",
    "update_position_status",
    "get_position",
    "get_pending_sync_positions",
    "mark_position_synced",
  ]


def test_notification_methods_delegate_to_outbox_repo():
  svc, _, notifications = make_service()
  svc.enqueue_notification(
    platform=NotificationPlatformEnum.TELEGRAM,
    channel=NotificationChannelEnum.INDIVIDUAL,
    message_text="hi",
  )
  svc.get_due_notifications()
  svc.delete_notification(1)
  svc.mark_notification_failed(1, "err", "ts")
  names = [c[0] for c in notifications.calls]
  assert names == [
    "enqueue_notification",
    "get_due_notifications",
    "delete_notification",
    "mark_notification_failed",
  ]


def test_facade_returns_repo_result():
  svc, _, _ = make_service()
  assert svc.get_position(5) == ("RESULT", "get_position")
