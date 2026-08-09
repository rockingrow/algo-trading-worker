"""
worker/worker_context.py
────────────────────────
Market-agnostic composition root for a child worker process.

Instantiated inside the child process (NOT in the parent FastAPI process)
because DB connections, telegram notifiers, and the NotificationJob daemon
thread cannot cross the multiprocessing fork boundary.

Provides infrastructure that any market implementation (FOREX, CRYPTO, …)
will need:
  - DBService
  - Direct Telegram senders (for messages that must fire immediately, e.g.
    startup/shutdown banners, and for the NotificationJob dispatcher)
  - Outbox notifiers (for in-process notification calls that should be
    queued through SQLite for store-and-forward delivery)
  - NATS enqueue callback (used by the NATS client for connection events)
  - Helper to start the NotificationJob dispatcher

Market-specific concerns (MT5 bridge, executor, NATS subscriber URL/footer,
MT5-specific jobs) live in the per-market signal processor, which takes a
WorkerContext as a dependency.
"""

from __future__ import annotations

from typing import Callable

from worker.logger import get_logger
from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationModeEnum,
  NotificationPlatformEnum,
)
from worker.services.db_service import DBService
from worker.services.notification_service import OutboxNotifier, TelegramNotification
from worker.settings import settings

log = get_logger("worker.worker_context")


class WorkerContext:
  """Market-agnostic services shared across market signal processors."""

  def __init__(self, settings_dict: dict) -> None:
    self.settings = settings_dict

    self.db_service = DBService()
    self.db_service.initialize()

    channel_ids = settings_dict.get("telegram_chat_channel_id") or [
      settings_dict.get("telegram_chat_id")
    ]

    self.direct_notifier = TelegramNotification()
    self.direct_channel_notifier = TelegramNotification(chat_ids=channel_ids)

    self.notifier = OutboxNotifier(
      self._build_enqueue(NotificationChannelEnum.INDIVIDUAL, "MT5_MANAGEMENT")
    )
    self.channel_notifier = OutboxNotifier(
      self._build_enqueue(NotificationChannelEnum.COMMUNITY, "TRADE_EVENT")
    )

  def _build_enqueue(
    self, channel: NotificationChannelEnum, category: str
  ) -> Callable[[str], None]:
    def enqueue(message_text: str) -> None:
      # SILENT mode only suppresses COMMUNITY; INDIVIDUAL always delivered.
      if channel == NotificationChannelEnum.INDIVIDUAL:
        mode = NotificationModeEnum.VERBOSE
      else:
        mode = NotificationModeEnum(settings.logging.notification_mode.upper())
      self.db_service.enqueue_notification(
        platform=NotificationPlatformEnum.TELEGRAM,
        channel=channel,
        message_text=message_text,
        category=category,
        mode=mode,
      )

    return enqueue

  def nats_enqueue(self, message_text: str) -> None:
    """Callback for the NATS client to enqueue connection-event notifications."""
    self.db_service.enqueue_notification(
      platform=NotificationPlatformEnum.TELEGRAM,
      channel=NotificationChannelEnum.INDIVIDUAL,
      message_text=message_text,
      category="NATS_EVENT",
      mode=NotificationModeEnum.VERBOSE,  # INDIVIDUAL always delivered regardless of notification_mode
    )

  def start_notification_job(self, stop_event) -> None:
    from worker.jobs.notification_job import NotificationJob

    NotificationJob(
      db_service=self.db_service,
      notifiers={
        NotificationChannelEnum.INDIVIDUAL.value: self.direct_notifier,
        NotificationChannelEnum.COMMUNITY.value: self.direct_channel_notifier,
      },
    ).start(stop_event=stop_event)
