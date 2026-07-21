"""SILENT notification_mode must only suppress COMMUNITY; INDIVIDUAL always delivered."""

from unittest.mock import MagicMock, patch

from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationModeEnum,
)


def _run_with_mode(notification_mode: str):
  """Instantiate WorkerContext under patched settings and return (ctx, mock_db)."""
  mock_db = MagicMock()
  with patch("worker.context.DBService") as MockDBService, patch(
    "worker.context.TelegramNotification"
  ), patch("worker.context.settings") as mock_settings:
    mock_settings.logging.notification_mode = notification_mode
    mock_settings.telegram_enabled = False
    mock_settings.telegram_bot_token = "tok"
    mock_settings.telegram_chat_id = "-1"
    mock_settings.telegram_chat_channel_id = []
    MockDBService.return_value = mock_db

    from worker.context import WorkerContext

    ctx = WorkerContext({"telegram_chat_id": "-1", "telegram_chat_channel_id": []})

    # Return inside the patch so closures can still read mock_settings
    yield ctx, mock_db


def test_individual_uses_verbose_when_silent():
  mock_db = MagicMock()
  with patch("worker.context.DBService") as MockDBService, patch(
    "worker.context.TelegramNotification"
  ), patch("worker.context.settings") as mock_settings:
    mock_settings.logging.notification_mode = "SILENT"
    mock_settings.telegram_enabled = False
    mock_settings.telegram_bot_token = "tok"
    mock_settings.telegram_chat_id = "-1"
    mock_settings.telegram_chat_channel_id = []
    MockDBService.return_value = mock_db

    from worker.context import WorkerContext

    ctx = WorkerContext({"telegram_chat_id": "-1", "telegram_chat_channel_id": []})

    ctx.notifier.send_message("individual msg")

    call_kwargs = mock_db.enqueue_notification.call_args.kwargs
    assert call_kwargs["mode"] == NotificationModeEnum.VERBOSE
    assert call_kwargs["channel"] == NotificationChannelEnum.INDIVIDUAL


def test_community_uses_silent_when_silent():
  mock_db = MagicMock()
  with patch("worker.context.DBService") as MockDBService, patch(
    "worker.context.TelegramNotification"
  ), patch("worker.context.settings") as mock_settings:
    mock_settings.logging.notification_mode = "SILENT"
    mock_settings.telegram_enabled = False
    mock_settings.telegram_bot_token = "tok"
    mock_settings.telegram_chat_id = "-1"
    mock_settings.telegram_chat_channel_id = []
    MockDBService.return_value = mock_db

    from worker.context import WorkerContext

    ctx = WorkerContext({"telegram_chat_id": "-1", "telegram_chat_channel_id": []})

    ctx.channel_notifier.send_message("community msg")

    call_kwargs = mock_db.enqueue_notification.call_args.kwargs
    assert call_kwargs["mode"] == NotificationModeEnum.SILENT
    assert call_kwargs["channel"] == NotificationChannelEnum.COMMUNITY


def test_nats_enqueue_uses_verbose_when_silent():
  mock_db = MagicMock()
  with patch("worker.context.DBService") as MockDBService, patch(
    "worker.context.TelegramNotification"
  ), patch("worker.context.settings") as mock_settings:
    mock_settings.logging.notification_mode = "SILENT"
    mock_settings.telegram_enabled = False
    mock_settings.telegram_bot_token = "tok"
    mock_settings.telegram_chat_id = "-1"
    mock_settings.telegram_chat_channel_id = []
    MockDBService.return_value = mock_db

    from worker.context import WorkerContext

    ctx = WorkerContext({"telegram_chat_id": "-1", "telegram_chat_channel_id": []})

    ctx.nats_enqueue("nats event")

    call_kwargs = mock_db.enqueue_notification.call_args.kwargs
    assert call_kwargs["mode"] == NotificationModeEnum.VERBOSE
    assert call_kwargs["channel"] == NotificationChannelEnum.INDIVIDUAL


def test_both_channels_verbose_when_verbose():
  mock_db = MagicMock()
  with patch("worker.context.DBService") as MockDBService, patch(
    "worker.context.TelegramNotification"
  ), patch("worker.context.settings") as mock_settings:
    mock_settings.logging.notification_mode = "VERBOSE"
    mock_settings.telegram_enabled = False
    mock_settings.telegram_bot_token = "tok"
    mock_settings.telegram_chat_id = "-1"
    mock_settings.telegram_chat_channel_id = []
    MockDBService.return_value = mock_db

    from worker.context import WorkerContext

    ctx = WorkerContext({"telegram_chat_id": "-1", "telegram_chat_channel_id": []})

    ctx.notifier.send_message("individual msg")
    assert mock_db.enqueue_notification.call_args.kwargs["mode"] == NotificationModeEnum.VERBOSE

    mock_db.reset_mock()

    ctx.channel_notifier.send_message("community msg")
    assert mock_db.enqueue_notification.call_args.kwargs["mode"] == NotificationModeEnum.VERBOSE
