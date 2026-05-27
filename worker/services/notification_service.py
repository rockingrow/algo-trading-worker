from typing import Callable

import requests

from worker.logger import get_logger
from worker.settings import settings

logger = get_logger("worker.services.notification_service")

def _box(text: str) -> str:
  return f"<pre>{text.strip()}</pre>"


class Notification:
  def send_message(self, message_text: str) -> bool:
    raise NotImplementedError("This method must be implemented by a subclass")


class TelegramNotification(Notification):
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

    url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
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


class OutboxNotifier:
  """Drop-in replacement for TelegramNotification that enqueues to the DB outbox."""

  def __init__(self, enqueue_fn: Callable[[str], None]) -> None:
    self._enqueue_fn = enqueue_fn

  def send_message(self, message_text: str) -> None:
    self._enqueue_fn(message_text)
