import logging
import os
import queue
import threading
import time
from typing import Callable

import requests
from pydantic import SecretStr

from worker import icons
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

  def __init__(
    self,
    chat_ids: list[str] | None = None,
    bot_token: SecretStr | None = None,
  ):
    self.enabled = settings.telegram.enabled
    self.bot_token = bot_token if bot_token is not None else settings.telegram.bot_token
    self.chat_ids = chat_ids if chat_ids is not None else [settings.telegram.chat_id]

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


# ── Telegram error-log hook ────────────────────────────────────────────────
#
# Forwards log records at ERROR level or above to Telegram. A standard
# ``logging.Handler`` cannot block: ``emit`` is synchronous and may run from any
# context (the FastAPI event loop, a market child process, a daemon thread), yet
# ``TelegramNotification.send_message`` performs a blocking ``requests`` call.
# ``TelegramLogHandler`` bridges the two with a queue + background **thread**:
# ``emit`` only formats the record and hands it to a bounded queue, and a
# long-lived worker thread performs the actual send. A thread (rather than the
# broker's asyncio task) is used because the worker's FOREX processor runs in a
# child process with no event loop at all.
#
# Two safeguards keep this from misbehaving in production:
#   * No recursion — a filter drops records emitted by the send path itself, so
#     a failing send can never trigger another send.
#   * No spam — identical messages are suppressed within a short dedup window and
#     the queue is bounded, dropping records when saturated rather than growing
#     without limit.

# Loggers whose records must never be forwarded, to avoid an infinite
# send → fail → log error → send loop. The notification path below logs under
# this module's logger name.
_EXCLUDED_PREFIXES = ("worker.services.notification_service",)

_QUEUE_MAXSIZE = 100
_STOP = object()  # sentinel enqueued by stop() to unblock the worker thread


class TelegramLogNotification(TelegramNotification):
  """Telegram channel dedicated to forwarded error logs.

  Targets the private log chat/bot when ``TELEGRAM_LOG_*`` is configured, and
  otherwise falls back to the shared management chat/bot."""

  def __init__(self) -> None:
    log_token = settings.telegram.log_bot_token
    bot_token = (
      log_token
      if log_token is not None and log_token.get_secret_value()
      else settings.telegram.bot_token
    )
    super().__init__(
      chat_ids=[settings.telegram.log_chat_id or settings.telegram.chat_id],
      bot_token=bot_token,
    )


class _RecursionFilter(logging.Filter):
  """Drop records emitted by the Telegram send path itself."""

  def filter(self, record: logging.LogRecord) -> bool:
    return not record.name.startswith(_EXCLUDED_PREFIXES)


class TelegramLogHandler(logging.Handler):
  """Logging handler that forwards ERROR+ records to a Telegram chat.

  ``emit`` never blocks and never raises: it formats the record, deduplicates,
  and hands it to a bounded queue drained by a background daemon thread that
  performs the synchronous :meth:`TelegramNotification.send_message`."""

  def __init__(self) -> None:
    super().__init__(level=logging.ERROR)
    self.addFilter(_RecursionFilter())
    self.setFormatter(
      logging.Formatter(fmt="[WORKER]\n%(levelname)s | %(name)s\n%(message)s")
    )
    self._lock = threading.Lock()
    self._queue: queue.Queue = queue.Queue(maxsize=_QUEUE_MAXSIZE)
    self._thread: threading.Thread | None = None
    self._pid: int | None = None  # process that owns the running worker thread
    # message -> monotonic timestamp of last forward, for dedup.
    self._recent: dict[str, float] = {}

  # ── lifecycle (called from the app lifespan / worker bootstrap) ──
  def start(self) -> None:
    """Ensure the background worker thread is running in this process."""
    self._ensure_worker()

  def stop(self) -> None:
    """Flush queued records and stop the worker thread cleanly."""
    with self._lock:
      thread = self._thread
      self._thread = None
      self._pid = None
    if thread is None or not thread.is_alive():
      return
    try:
      self._queue.put(_STOP, timeout=2)
    except queue.Full:
      pass  # worker is draining; it exits on process teardown regardless
    thread.join(timeout=5)

  # ── logging.Handler API ──────────────────────────────────────────
  def emit(self, record: logging.LogRecord) -> None:
    try:
      self._ensure_worker()
      message = self.format(record)
      if self._is_duplicate(message):
        return
      try:
        self._queue.put_nowait(message)
      except queue.Full:
        pass  # under an error storm, dropping is preferable to blocking
    except Exception:  # pragma: no cover — handlers must never raise
      self.handleError(record)

  # ── internals ────────────────────────────────────────────────────
  def _ensure_worker(self) -> None:
    """(Re)start the worker thread for the current process.

    A fresh queue/thread is created per process id so a forked child (whose
    inherited thread never actually runs) starts its own worker instead of
    enqueueing into a dead one."""
    pid = os.getpid()
    with self._lock:
      if self._pid == pid and self._thread is not None and self._thread.is_alive():
        return
      self._queue = queue.Queue(maxsize=_QUEUE_MAXSIZE)
      self._recent = {}
      self._pid = pid
      self._thread = threading.Thread(
        target=self._worker, args=(self._queue,), name="telegram-log", daemon=True
      )
      self._thread.start()

  def _is_duplicate(self, message: str) -> bool:
    """Return True if *message* was forwarded within the dedup window."""
    window = settings.telegram.log_dedup_window
    now = time.monotonic()
    with self._lock:
      # Prune stale entries so the dict cannot grow unbounded.
      self._recent = {m: ts for m, ts in self._recent.items() if now - ts < window}
      if message in self._recent:
        return True
      self._recent[message] = now
      return False

  def _worker(self, work_queue: queue.Queue) -> None:
    notifier = TelegramLogNotification()
    while True:
      message = work_queue.get()
      try:
        if message is _STOP:
          return
        notifier.send_message(f"{icons.ERROR_ALERT} {message}")
      except Exception:
        # Never surface failures back through logging (would risk recursion)
        # and never let the worker die.
        pass
      finally:
        work_queue.task_done()


# Shared singleton: every logger forwards through one queue/worker.
telegram_log_handler = TelegramLogHandler()
