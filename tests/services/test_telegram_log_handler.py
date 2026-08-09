"""Tests for the Telegram error-log hook (worker.services.notification_service).

The handler drains its queue on a background thread, so each test enqueues via
``logger.error`` and then calls ``handler.stop()`` — which flushes the queue and
joins the worker thread — before asserting on what was sent. No sleeps needed.
"""

import logging

import pytest
from pydantic import SecretStr

import worker.services.notification_service as notification_service
from worker.services.notification_service import TelegramLogHandler


@pytest.fixture
def sent(monkeypatch):
  """Capture every message the worker thread would send to Telegram."""
  messages: list[str] = []

  def fake_send(self, message_text: str) -> bool:
    messages.append(message_text)
    return True

  monkeypatch.setattr(
    notification_service.TelegramNotification, "send_message", fake_send
  )
  return messages


def _make_logger(name: str, handler: TelegramLogHandler) -> logging.Logger:
  logger = logging.getLogger(name)
  logger.handlers = [handler]
  logger.setLevel(logging.DEBUG)
  logger.propagate = False
  return logger


def _spy_notifier_init(monkeypatch) -> list:
  """Record the chat_ids/bot_token every TelegramNotification is built with."""
  captured: list = []
  original_init = notification_service.TelegramNotification.__init__

  def spy_init(self, chat_ids=None, bot_token=None):
    captured.append((chat_ids, bot_token))
    original_init(self, chat_ids=chat_ids, bot_token=bot_token)

  monkeypatch.setattr(notification_service.TelegramNotification, "__init__", spy_init)

  def fake_send(self, message_text: str) -> bool:
    return True

  monkeypatch.setattr(
    notification_service.TelegramNotification, "send_message", fake_send
  )
  return captured


def test_error_is_forwarded(sent):
  handler = TelegramLogHandler()
  handler.start()
  logger = _make_logger("test.telegram.error", handler)

  logger.error("boom %s", 42)
  handler.stop()

  assert len(sent) == 1
  assert "boom 42" in sent[0]


def test_info_and_warning_are_not_forwarded(sent):
  handler = TelegramLogHandler()
  handler.start()
  logger = _make_logger("test.telegram.info", handler)

  logger.info("just fyi")
  logger.warning("heads up")
  handler.stop()

  assert sent == []


def test_notification_service_logs_do_not_recurse(sent):
  handler = TelegramLogHandler()
  handler.start()
  # A logger living under the notification-service module must be filtered out,
  # otherwise a failed send would log an error that triggers another send.
  logger = _make_logger("worker.services.notification_service", handler)

  logger.error("Failed to send Telegram message to 123: 400")
  handler.stop()

  assert sent == []


def test_duplicate_messages_are_deduplicated(sent):
  handler = TelegramLogHandler()
  handler.start()
  logger = _make_logger("test.telegram.dedup", handler)

  logger.error("same error")
  logger.error("same error")
  handler.stop()

  assert len(sent) == 1


def test_emit_without_start_lazily_forwards(sent):
  # No explicit start(): emit must lazily spin up the worker thread. This is the
  # path taken inside a forked FOREX child process, which never calls start().
  handler = TelegramLogHandler()
  logger = _make_logger("test.telegram.lazy", handler)

  logger.error("lazy boom")
  handler.stop()

  assert len(sent) == 1
  assert "lazy boom" in sent[0]


def test_worker_uses_dedicated_log_chat_id(monkeypatch):
  """When telegram_log_chat_id is set, the worker must target that chat."""
  monkeypatch.setattr(
    notification_service.settings.telegram, "log_chat_id", "999", raising=False
  )
  monkeypatch.setattr(
    notification_service.settings.telegram, "chat_id", "111", raising=False
  )
  captured = _spy_notifier_init(monkeypatch)

  handler = TelegramLogHandler()
  handler.start()
  logger = _make_logger("test.telegram.dedicated_chat", handler)

  logger.error("route me to the private chat")
  handler.stop()

  chat_ids = [c for c, _ in captured]
  assert ["999"] in chat_ids  # dedicated chat, not the management fallback


def test_worker_uses_dedicated_log_bot_token(monkeypatch):
  """When telegram_log_bot_token is set, the worker must send via that bot."""
  monkeypatch.setattr(
    notification_service.settings.telegram,
    "log_bot_token",
    SecretStr("log-bot-tok"),
    raising=False,
  )
  monkeypatch.setattr(
    notification_service.settings.telegram,
    "bot_token",
    SecretStr("main-bot-tok"),
    raising=False,
  )
  captured = _spy_notifier_init(monkeypatch)

  handler = TelegramLogHandler()
  handler.start()
  logger = _make_logger("test.telegram.dedicated_bot", handler)

  logger.error("send me via the dedicated bot")
  handler.stop()

  tokens = [t.get_secret_value() for _, t in captured if t is not None]
  assert "log-bot-tok" in tokens  # dedicated bot, not the shared main bot


def test_worker_falls_back_to_shared_bot_token(monkeypatch):
  """When telegram_log_bot_token is empty, the worker must reuse the shared bot."""
  monkeypatch.setattr(
    notification_service.settings.telegram, "log_bot_token", None, raising=False
  )
  monkeypatch.setattr(
    notification_service.settings.telegram,
    "bot_token",
    SecretStr("main-bot-tok"),
    raising=False,
  )
  captured = _spy_notifier_init(monkeypatch)

  handler = TelegramLogHandler()
  handler.start()
  logger = _make_logger("test.telegram.fallback_bot", handler)

  logger.error("send me via the shared bot")
  handler.stop()

  tokens = [t.get_secret_value() for _, t in captured if t is not None]
  assert "main-bot-tok" in tokens
