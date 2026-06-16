from typing import Callable

import requests

from worker.logger import get_logger
from worker.settings import settings

logger = get_logger("worker.services.notification_service")

def _box(text: str) -> str:
  return f"<pre>{text.strip()}</pre>"


class Notification:
  """Abstract base class for notification senders.

  Concrete senders implement :meth:`send_message` returning ``bool`` so they are
  freely substitutable (Liskov) and conform to
  :class:`~worker.interfaces.message_sender_protocol.MessageSenderProtocol`.
  """

  def send_message(self, message_text: str) -> bool:
    raise NotImplementedError("This method must be implemented by a subclass")


class TelegramNotification(Notification):
  """Sends HTML-formatted messages to one or more Telegram chat IDs via the Bot API."""

  def __init__(self, chat_ids: list[str] | None = None):
    self.enabled = settings.telegram_enabled
    self.bot_token = settings.telegram_bot_token
    self.chat_ids = chat_ids if chat_ids is not None else [settings.telegram_chat_id]

  def send_message(self, message_text: str) -> bool:
    if not self.enabled:
      logger.debug("Telegram notifications are disabled in settings.")
      return True

    if not self.bot_token or not self.chat_ids:
      logger.warning(
        "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set for notifications."
      )
      return False

    url = f"https://api.telegram.org/bot{self.bot_token.get_secret_value()}/sendMessage"
    success = True
    for chat_id in self.chat_ids:
      payload = {
        "chat_id": chat_id,
        "text": message_text,
        "parse_mode": "HTML",
      }
      try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
          logger.error(f"Failed to send Telegram message to {chat_id}: {response.text}")
          success = False
      except Exception as e:
        logger.exception(f"Exception sending Telegram message to {chat_id}: {e}")
        success = False
    return success


class OutboxNotifier(Notification):
  """Enqueues messages to the DB outbox instead of sending them directly.

  ``enqueue_fn`` is expected to be the closure returned by
  ``WorkerContext._build_enqueue()``, which calls
  ``DBService.enqueue_notification()`` → ``NotificationOutboxRepository``
  → INSERT into the ``notifications`` table.  The actual Telegram delivery
  is handled later by ``NotificationJob`` using a ``TelegramNotification``.

  Returns ``bool`` like every other sender so it is fully substitutable for a
  direct notifier (Liskov / MessageSenderProtocol).
  """

  def __init__(self, enqueue_fn: Callable[[str], None]) -> None:
    self._enqueue_fn = enqueue_fn

  def send_message(self, message_text: str) -> bool:
    self._enqueue_fn(message_text)
    return True
