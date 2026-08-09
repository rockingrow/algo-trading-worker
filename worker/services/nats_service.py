import asyncio
import queue
import threading
import time
from typing import Callable, Generator, Optional

from worker.icons import BROKER, DISCONNECTED, RETRYING
from worker.logger import get_logger
from worker.nats_client import NatsClient
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.settings import settings
from worker.utils.logging import get_footer

logger = get_logger("worker.nats_service")

_Subject = str | NatsSubjectEnum


def _subject_str(s: _Subject) -> str:
  return s if isinstance(s, str) else s.value


class _CallbackDedup:
  """Suppresses duplicate async NATS callbacks within a time window.

  nats.py fires error_cb / disconnected_cb from multiple internal coroutines
  (reader, flusher, pinger) for the same connection event, producing duplicate
  log records that slip past the downstream TelegramLogHandler dedup."""

  def __init__(self) -> None:
    self._lock = threading.Lock()
    self._recent: dict[str, float] = {}

  def is_duplicate(self, key: str) -> bool:
    window = settings.telegram.log_dedup_window
    now = time.monotonic()
    with self._lock:
      self._recent = {k: ts for k, ts in self._recent.items() if now - ts < window}
      if key in self._recent:
        return True
      self._recent[key] = now
      return False


def _connection_name(account_id: Optional[str]) -> Optional[str]:
  """NATS connection name reported to the Broker server.

  ``account_id`` already has the "<market_type>-<id>" form (see
  Settings._validate_market_requirements), so the Broker can see which market
  and account is connecting in its /connz monitoring. Returns None when no
  account_id is configured, leaving the connection unnamed."""
  return str(account_id) if account_id else None


class NATSSubscriber:
  """NATS subscriber that listens on configured subjects and enqueues messages to an internal queue for synchronous consumption via listen()."""

  def __init__(
    self,
    url: str,
    subjects: list[_Subject],
    publish_subjects: list[NatsSubjectEnum] | None = None,
    token: Optional[str] = None,
    account_id: Optional[str] = None,
    account_footer_fn: Optional[Callable[[], str]] = None,
    enqueue_fn: Optional[Callable[[str], None]] = None,
  ):
    self.url = url
    self.subjects = subjects
    self.publish_subjects = publish_subjects or []
    self._account_footer_fn = account_footer_fn
    self._enqueue_fn = enqueue_fn
    # Set once the NATS subscriptions are live. A WORKER_CONNECTED handshake must
    # wait on this before publishing: NATS core does not replay, so the broker's
    # reply would be lost if we announced before the SYSTEM subscription existed.
    self._subscribed = threading.Event()
    self._msg_queue: queue.Queue[tuple[_Subject, str]] = queue.Queue()
    self._footer = get_footer(account_footer_fn)
    self._dedup = _CallbackDedup()
    self._client = NatsClient(
      url=url,
      token=token,
      name=_connection_name(account_id),
      error_cb=self._on_error,
      disconnected_cb=self._on_disconnect,
      reconnected_cb=self._on_reconnect,
    )

  def _notify(self, message_text: str) -> None:
    """Enqueue a management notification via the outbox (fast SQLite INSERT,
    safe to call directly from async NATS callbacks without blocking the loop)."""
    if self._enqueue_fn is not None:
      self._enqueue_fn(message_text)

  async def _on_error(self, e) -> None:
    if self._dedup.is_duplicate(f"error:{e!r}"):
      return
    logger.error("NATS error: %r", e)

  async def _on_disconnect(self) -> None:
    if self._dedup.is_duplicate("disconnect"):
      return
    logger.warning("NATS disconnected from %s. Retrying...", self.url)
    self._notify(
      f"<pre>{DISCONNECTED} [Disconnected] NATS Broker\nEndpoint: {self.url}\n{RETRYING} Retrying connection...{self._footer}</pre>"
    )

  async def _on_reconnect(self) -> None:
    logger.info("NATS reconnected to %s", self.url)
    self._notify(
      f"<pre>{BROKER} [Connected] NATS Worker to Broker\nEndpoint: {self.url}{self._footer}</pre>"
    )

  def connect(self) -> None:
    subject_names = [_subject_str(s) for s in self.subjects]
    publish_subject_names = [_subject_str(s) for s in self.publish_subjects]

    async def body(nc, stop_event) -> None:
      pub_line = (
        f"\nPublishing Subjects: {', '.join(publish_subject_names)}"
        if publish_subject_names
        else ""
      )
      self._notify(
        f"<pre>{BROKER} [Connected] NATS Worker to Broker\nEndpoint: {self.url}\nListening Subjects: {', '.join(subject_names)}{pub_line}{self._footer}</pre>"
      )
      logger.info(
        "Connected to NATS at %s, listening=[%s] publishing=[%s]",
        self.url,
        ", ".join(subject_names),
        ", ".join(publish_subject_names),
      )

      async def message_handler(msg):
        try:
          subject: _Subject = NatsSubjectEnum(msg.subject)
        except ValueError:
          subject = msg.subject
        self._msg_queue.put((subject, msg.data.decode()))

      subs = [
        await nc.subscribe(_subject_str(subject), cb=message_handler)
        for subject in self.subjects
      ]
      logger.info("Subscribed to NATS subjects: [%s]", ", ".join(subject_names))
      # Subscriptions are registered on the server now — safe to announce.
      self._subscribed.set()

      while not stop_event.is_set():
        await asyncio.sleep(0.5)

      for sub in subs:
        await sub.unsubscribe()
      logger.info("NATS subscriber connection closed.")

    self._client.start(body, thread_name="nats-subscriber-loop")

  def wait_subscribed(self, timeout: Optional[float] = None) -> bool:
    """Block until the subscriptions are live (see ``_subscribed``). Returns True
    if subscribed within *timeout*, False on timeout. The flag is set once and
    never cleared, so this returns immediately after the first subscription."""
    return self._subscribed.wait(timeout)

  def close(self) -> None:
    logger.info("NATS subscriber stop requested. Closing...")
    self._client.stop()

  def listen(self, stop_event=None) -> Generator[tuple[_Subject, str], None, None]:
    logger.info(
      "Started listening for NATS messages on subjects: %s",
      ", ".join(_subject_str(s) for s in self.subjects),
    )
    while not self._client._stop_event.is_set():
      if stop_event is not None and stop_event.is_set():
        return
      try:
        subject, raw = self._msg_queue.get(timeout=0.5)
        logger.debug("Received NATS message on %s: %s", _subject_str(subject), raw)
        yield subject, raw
      except queue.Empty:
        continue
      except Exception as err:
        logger.exception("Unexpected error in NATS listener: %s", err)


class NATSPublisher:
  """Thread-safe NATS publisher. Owns a daemon thread that runs an asyncio
  event loop with a live NATS connection. Other threads call publish() to
  enqueue outgoing messages; the loop drains the queue and publishes them."""

  def __init__(
    self,
    url: str,
    publish_subjects: list[NatsSubjectEnum],
    token: Optional[str] = None,
    account_id: Optional[str] = None,
    on_reconnect: Optional[Callable[[], None]] = None,
  ):
    self.url = url
    self.publish_subjects = publish_subjects
    self._on_reconnect_callback = on_reconnect
    self._send_queue: queue.Queue[tuple[str, bytes]] = queue.Queue()
    self._dedup = _CallbackDedup()
    self._client = NatsClient(
      url=url,
      token=token,
      name=_connection_name(account_id),
      error_cb=self._on_error,
      disconnected_cb=self._on_disconnect,
      reconnected_cb=self._on_reconnect,
    )

  async def _on_error(self, e) -> None:
    if self._dedup.is_duplicate(f"error:{e!s}"):
      return
    logger.error("NATS publisher error: %s", e)

  async def _on_disconnect(self) -> None:
    if self._dedup.is_duplicate("disconnect"):
      return
    logger.warning("NATS publisher disconnected from %s. Retrying...", self.url)

  async def _on_reconnect(self) -> None:
    logger.info("NATS publisher reconnected to %s", self.url)
    # Re-announce presence so the broker re-pushes any per-worker init config
    # (magics, leverage, replay) after a broker/NATS restart. The handshake is
    # now a blocking request/reply with retries (see BaseSignalProcessor.
    # _announce_worker_connected), so it must run off this event loop's thread —
    # calling it here directly would deadlock the loop it needs to schedule its
    # own nc.request() onto.
    if self._on_reconnect_callback is not None:
      threading.Thread(
        target=self._run_reconnect_callback, daemon=True, name="nats-reconnect-announce"
      ).start()

  def _run_reconnect_callback(self) -> None:
    try:
      self._on_reconnect_callback()
    except Exception as exc:
      logger.exception("NATS publisher on_reconnect callback failed: %s", exc)

  def connect(self) -> None:
    subject_names = [s.value for s in self.publish_subjects]
    logger.info("Starting NATS publisher for subjects: %s", subject_names)

    async def body(nc, stop_event) -> None:
      logger.info(
        "NATS publisher connected to %s, publish_subjects=%s", self.url, subject_names
      )
      while not stop_event.is_set():
        try:
          subject, payload = self._send_queue.get_nowait()
        except queue.Empty:
          await asyncio.sleep(0.05)
          continue
        try:
          await nc.publish(subject, payload)
          logger.debug("Published NATS message on %s: %s", subject, payload)
        except Exception as exc:
          logger.exception("Failed to publish NATS message on %s: %s", subject, exc)
      logger.info("NATS publisher connection closed.")

    self._client.start(body, thread_name="nats-publisher-loop")

  def publish(self, subject: NatsSubjectEnum, data: str) -> None:
    """Thread-safe: enqueue a message to be published asynchronously."""
    self._send_queue.put((subject.value, data.encode()))

  def request(self, subject: NatsSubjectEnum, data: str, timeout: float = 5.0) -> str:
    """Thread-safe: synchronously publish a request and block for the broker's
    reply, bypassing the fire-and-forget queue (a reply has nowhere to go once
    enqueued). Schedules ``nc.request`` onto the publisher's own event loop via
    ``run_coroutine_threadsafe`` since this is called from other threads.

    Raises ``ConnectionError`` if the publisher has no live connection yet, or
    whatever ``nats.aio.client.Client.request`` raises (notably
    ``nats.errors.TimeoutError`` when the broker doesn't reply in time) — the
    caller decides whether/how to retry.
    """
    loop = self._client._loop
    nc = self._client.nc
    if loop is None or nc is None:
      raise ConnectionError("NATS publisher is not connected")
    future = asyncio.run_coroutine_threadsafe(
      nc.request(subject.value, data.encode(), timeout=timeout), loop
    )
    msg = future.result(timeout=timeout + 5)
    return msg.data.decode()

  def close(self) -> None:
    logger.info("NATS publisher stop requested. Closing...")
    self._client.stop()
