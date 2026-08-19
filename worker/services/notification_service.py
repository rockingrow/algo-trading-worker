import logging
import os
import queue
import re
import threading
import time
from enum import Enum
from typing import Any, Callable, Iterable, NamedTuple

import requests
from pydantic import SecretStr

from worker import icons
from worker.logger import get_logger
from worker.settings import settings

logger = get_logger("worker.services.notification_service")


def _box(text: str) -> str:
  return f"<pre>{text.strip()}</pre>"


# ── Chat targets ───────────────────────────────────────────────────────────

# A chat id carrying a forum topic, e.g. "-1002173777783_924584". Only a
# *numeric* chat id may be suffixed this way: a channel username can itself
# contain underscores (``@my_group_2``), so splitting those would invent a
# topic out of part of the name.
_CHAT_TOPIC_RE = re.compile(r"(?P<chat>-?\d+)_(?P<topic>\d+)")


class ChatTarget(NamedTuple):
  """One Telegram destination: a chat, optionally a topic inside it.

  ``message_thread_id`` is the Bot API's name for a forum topic — "unique
  identifier for the target message thread (topic) of a forum; for forum
  supergroups and private chats of bots with forum topic mode enabled only".
  Sending without it lands the message in the group's *General* topic, which is
  why a group with topics enabled needs the id carried all the way down to the
  payload."""

  chat_id: str
  message_thread_id: int | None = None

  @property
  def label(self) -> str:
    """Identify this target in a log line."""
    if self.message_thread_id is None:
      return self.chat_id
    return f"{self.chat_id} (topic {self.message_thread_id})"


def parse_chat_targets(raw: str | Iterable[Any] | None) -> list[ChatTarget]:
  """Parse a chat-id setting into the chats (and topics) to deliver to.

  Accepts either a raw string or an already-split list, because the settings
  reach here both ways: ``TELEGRAM_WORKER_LOG_CHAT_IDS`` / ``TELEGRAM_LOG_CHAT_ID`` stay
  plain strings while ``TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS`` is split into a list by
  ``TelegramSettings.parse_channel_ids``. Either way each entry is
  comma-separated, so one setting can fan out to several groups:
  ``"-1001111111111,-1002173777783_924584,@public_channel"``.

  An entry of the form ``<chat id>_<topic id>`` addresses a *topic* inside a
  supergroup that has the Topics feature switched on — the shape Telegram
  itself shows in a topic link (``t.me/c/2173777783/924584``). It is split back
  into the chat and its ``message_thread_id``; everything else is passed
  through untouched, so plain ids (``-1001111111111``), user ids and
  ``@username`` handles keep working exactly as before.

  Blank entries are skipped and duplicates collapse, so a stray comma or a chat
  listed twice costs nothing (and never double-posts).
  """
  if raw is None:
    return []
  specs: Iterable[Any] = [raw] if isinstance(raw, str) else raw

  targets: list[ChatTarget] = []
  for spec in specs:
    if spec is None:
      continue  # e.g. a missing telegram_chat_id fallback in the settings dict
    for entry in str(spec).split(","):
      entry = entry.strip()
      if not entry:
        continue
      match = _CHAT_TOPIC_RE.fullmatch(entry)
      target = (
        ChatTarget(match["chat"], int(match["topic"])) if match else ChatTarget(entry)
      )
      if target not in targets:
        targets.append(target)
  return targets


class Notification:
  """Abstract base class for notification senders.

  Concrete senders implement :meth:`send_message` returning ``bool`` so they are
  freely substitutable (Liskov) and conform to
  :class:`~worker.interfaces.message_sender_protocol.MessageSenderProtocol`.
  """

  def send_message(self, message_text: str) -> bool:
    raise NotImplementedError("This method must be implemented by a subclass")


class TelegramNotification(Notification):
  """Sends HTML-formatted messages to one or more Telegram chats via the Bot API.

  ``chat_ids`` is resolved through :func:`parse_chat_targets`, so every chat-id
  setting may name several chats at once and may address a single topic inside
  a supergroup that has the Topics feature enabled
  (``-1002173777783_924584``)."""

  def __init__(
    self,
    chat_ids: str | list[str] | None = None,
    bot_token: SecretStr | None = None,
  ):
    self.enabled = settings.telegram.enabled
    self.bot_token = bot_token if bot_token is not None else settings.telegram.bot_token
    self.targets = parse_chat_targets(
      chat_ids if chat_ids is not None else settings.telegram.chat_id
    )

  def send_message(self, message_text: str) -> bool:
    """Deliver *message_text* (HTML) to every configured chat/topic.

    Returns True only when all of them took the message — never raises, since
    notifications are best-effort."""
    if not self.enabled:
      logger.debug("Telegram notifications are disabled in settings.")
      return True

    if not self.bot_token:
      logger.warning("TELEGRAM_BOT_TOKEN must be set for notifications.")
      return False

    if not self.targets:
      logger.warning("No usable Telegram chat id configured — nothing to notify.")
      return False

    url = f"https://api.telegram.org/bot{self.bot_token.get_secret_value()}/sendMessage"
    success = True
    # Each chat is sent on its own so one group the bot was kicked from (or one
    # deleted topic) never stops the others from being notified.
    for target in self.targets:
      payload: dict[str, Any] = {
        "chat_id": target.chat_id,
        "text": message_text,
        "parse_mode": "HTML",
      }
      # Only for topic targets: the Bot API answers "400 Bad Request: message
      # thread not found" when a group has no such thread.
      if target.message_thread_id is not None:
        payload["message_thread_id"] = target.message_thread_id
      try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
          logger.error(
            f"Failed to send Telegram message to {target.label}: {response.text}"
          )
          success = False
      except Exception as e:
        logger.exception(f"Exception sending Telegram message to {target.label}: {e}")
        success = False
    return success


class EditOutcome(Enum):
  """Result of trying to rewrite an already-sent cycle message.

  A tri-state, not a bool, because the dispatcher reacts differently to each: an
  ``OK`` edit advances the chat's delivered revision, a ``MISSING`` one drops the
  stored message id so the next pass posts a fresh message, and a ``FAILED`` one
  keeps the id and backs off — re-sending on a rate limit would leave the same
  trade in the chat twice.
  """

  OK = "OK"
  #: The message is gone or can no longer be edited — re-send to recover.
  MISSING = "MISSING"
  #: Transient failure — keep the message id and retry after the backoff.
  FAILED = "FAILED"


#: Bot API error fragments (lower-cased) that mean the message is unrecoverable.
_MESSAGE_GONE_MARKERS = (
  "message to edit not found",
  "message can't be edited",
  "message_id_invalid",
)


class TelegramCycleNotification:
  """Posts and edits the single Telegram message that reports one signal cycle.

  Deliberately *not* a :class:`Notification`: that contract is one-shot
  (``send_message(text) -> bool``) and has nowhere to put the ``message_id`` an
  edit needs, nor a way to address one chat at a time — a cycle owns a separate
  message per chat and rewrites each of them. Sharing the base class would have
  meant widening ``send_message`` for every sender that never edits anything.

  Each call names **one** chat, as the raw setting entry the cycle stored
  (``-1002173777783_924584``), and resolves it through :func:`parse_chat_targets`
  like every other sender — so a cycle lands in the same forum topic as the rest
  of the worker's notifications. Keeping the raw entry as the stored key is what
  gives two topics of one group their own message.

  Bodies arrive already boxed by the presenter, so nothing is wrapped here.
  """

  def __init__(self, bot_token: SecretStr | None = None) -> None:
    self.enabled = settings.telegram.enabled
    self.bot_token = bot_token if bot_token is not None else settings.telegram.bot_token

  def _api_url(self, method: str) -> str:
    return f"https://api.telegram.org/bot{self.bot_token.get_secret_value()}/{method}"

  def _target(self, chat: str) -> ChatTarget | None:
    """Resolve one stored chat entry into the chat (and topic) to deliver to.

    ``None`` means nothing can be sent — Telegram is off, the token is missing,
    or the entry parses to no chat at all.
    """
    if not self.enabled:
      logger.debug("Telegram notifications are disabled in settings.")
      return None
    if not self.bot_token:
      logger.warning("TELEGRAM_BOT_TOKEN must be set for cycle messages.")
      return None
    targets = parse_chat_targets(chat)
    if not targets:
      logger.warning("Unusable Telegram chat id for a cycle message: %r", chat)
      return None
    # One entry addresses one chat; parse_chat_targets returns a list because
    # the *settings* it usually parses are comma-separated lists.
    return targets[0]

  @staticmethod
  def _payload(target: ChatTarget, message_text: str) -> dict:
    payload: dict[str, Any] = {
      "chat_id": target.chat_id,
      "text": message_text,
      "parse_mode": "HTML",
      "disable_web_page_preview": True,
    }
    if target.message_thread_id is not None:
      payload["message_thread_id"] = target.message_thread_id
    return payload

  def send(self, chat: str, message_text: str) -> str | None:
    """Post *message_text* to *chat* and return Telegram's ``message_id``.

    ``None`` means there is no message to edit later — the send was skipped
    (disabled/misconfigured), failed, or succeeded with a response the id could
    not be read from. The caller must treat all three the same: nothing in this
    chat represents the cycle yet.
    """
    target = self._target(chat)
    if target is None:
      return None
    try:
      response = requests.post(
        self._api_url("sendMessage"),
        json=self._payload(target, message_text),
        timeout=5,
      )
      if response.status_code != 200:
        logger.error(
          "Telegram sendMessage failed for %s: %s", target.label, response.text
        )
        return None
      message_id = _result_field(response, "message_id")
      if message_id is None:
        logger.warning(
          "Telegram sendMessage to %s returned no message_id", target.label
        )
        return None
      return str(message_id)
    except Exception as exc:
      logger.exception("Exception on Telegram sendMessage to %s: %s", target.label, exc)
      return None

  def edit(self, chat: str, message_id: str, message_text: str) -> EditOutcome:
    """Rewrite the cycle's message in *chat* with *message_text*.

    An edit Telegram rejects because the body is byte-identical (``message is
    not modified``) counts as ``OK``: it is a no-op, and calling it a failure
    would make a redelivered signal re-post the whole cycle.
    """
    target = self._target(chat)
    if target is None:
      return EditOutcome.FAILED
    payload = self._payload(target, message_text)
    # editMessageText identifies the message by chat + message_id; the topic it
    # sits in was fixed when it was posted, and the Bot API takes no
    # message_thread_id here.
    payload.pop("message_thread_id", None)
    payload["message_id"] = message_id
    try:
      response = requests.post(
        self._api_url("editMessageText"), json=payload, timeout=5
      )
      if response.status_code == 200:
        return EditOutcome.OK
      body = (response.text or "").lower()
      if "message is not modified" in body:
        return EditOutcome.OK
      logger.error(
        "Telegram editMessageText failed for %s message_id=%s: %s",
        target.label,
        message_id,
        response.text,
      )
      if any(marker in body for marker in _MESSAGE_GONE_MARKERS):
        return EditOutcome.MISSING
      return EditOutcome.FAILED
    except Exception as exc:
      logger.exception(
        "Exception editing Telegram message %s in %s: %s",
        message_id,
        target.label,
        exc,
      )
      return EditOutcome.FAILED


def _result_field(response, key: str):
  """Best-effort read of one field from a Bot API response's ``result`` object."""
  try:
    body = response.json()
  except Exception:
    return None
  result = body.get("result") if isinstance(body, dict) else None
  return result.get(key) if isinstance(result, dict) else None


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
  otherwise falls back to the shared management chat/bot. The list/topic syntax
  of :func:`parse_chat_targets` applies here too — to ``TELEGRAM_LOG_CHAT_ID``
  and to the ``TELEGRAM_WORKER_LOG_CHAT_IDS`` it falls back to."""

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
