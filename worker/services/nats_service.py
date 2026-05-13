import asyncio
import queue
import threading
from typing import Callable, Generator, Optional

import nats

from worker.helpers.logging import get_footer
from worker.logger import get_logger
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.services.notification_service import TelegramNotification

logger = get_logger("worker.nats_service")


class NATSSubscriber:
  def __init__(
    self,
    url: str,
    subjects: list[NatsSubjectEnum],
    token: Optional[str] = None,
    account_footer_fn: Optional[Callable[[], str]] = None,
  ):
    self.url = url
    self.subjects = subjects
    self._token = token
    self._account_footer_fn = account_footer_fn
    self._stop_event = threading.Event()
    self._msg_queue: queue.Queue[tuple[NatsSubjectEnum, str]] = queue.Queue()
    self._loop_thread: Optional[threading.Thread] = None

  async def _run_loop(self) -> None:
    notification = TelegramNotification()
    footer = get_footer(self._account_footer_fn)
    subject_names = [s.value for s in self.subjects]

    async def message_handler(msg):
      try:
        subject = NatsSubjectEnum(msg.subject)
      except ValueError:
        logger.warning("Received message on unknown subject: %s", msg.subject)
        return
      self._msg_queue.put((subject, msg.data.decode()))

    async def error_cb(e):
      logger.error("NATS error: %s", e)

    async def disconnected_cb():
      logger.warning("NATS disconnected from %s. Retrying...", self.url)
      notification.send_message(
        f"<pre>🔴 [Disconnected] NATS Broker\nEndpoint: {self.url}\n⏳ Retrying connection...{footer}</pre>"
      )

    async def reconnected_cb():
      logger.info("NATS reconnected to %s", self.url)
      notification.send_message(
        f"<pre>🔌 [Connected] NATS Worker to Broker\nEndpoint: {self.url}{footer}</pre>"
      )

    connect_opts: dict = dict(
      error_cb=error_cb,
      disconnected_cb=disconnected_cb,
      reconnected_cb=reconnected_cb,
      max_reconnect_attempts=-1,
      reconnect_time_wait=5,
    )
    if self._token:
      connect_opts["token"] = self._token

    try:
      nc = await nats.connect(self.url, **connect_opts)
      logger.info("Connected to NATS at %s, subjects=%s", self.url, subject_names)
      notification.send_message(
        f"<pre>🔌 [Connected] NATS Worker to Broker\nEndpoint: {self.url}\nSubjects: {', '.join(subject_names)}{footer}</pre>"
      )

      subs = [
        await nc.subscribe(subject.value, cb=message_handler)
        for subject in self.subjects
      ]
      logger.info("Subscribed to NATS subjects: %s", subject_names)

      while not self._stop_event.is_set():
        await asyncio.sleep(0.5)

      for sub in subs:
        await sub.unsubscribe()
      await nc.drain()
      logger.info("NATS connection closed.")
    except Exception as exc:
      logger.exception("NATS fatal error: %s", exc)

  def connect(self) -> None:
    loop = asyncio.new_event_loop()
    self._loop_thread = threading.Thread(
      target=loop.run_until_complete,
      args=(self._run_loop(),),
      name="nats-loop",
      daemon=True,
    )
    self._loop_thread.start()
    logger.info("NATS connection thread started.")

  def close(self) -> None:
    logger.info("NATS stop requested. Closing...")
    self._stop_event.set()
    if self._loop_thread is not None:
      self._loop_thread.join(timeout=5)

  def listen(
    self, stop_event=None
  ) -> Generator[tuple[NatsSubjectEnum, str], None, None]:
    """Yield (subject, raw_json) tuples. Caller is responsible for parsing based on subject."""
    logger.info(
      "Started listening for NATS messages on subjects: %s",
      [s.value for s in self.subjects],
    )
    while not self._stop_event.is_set():
      if stop_event is not None and stop_event.is_set():
        return
      try:
        subject, raw = self._msg_queue.get(timeout=0.5)
        logger.debug("Received NATS message on %s: %s", subject.value, raw)
        yield subject, raw
      except queue.Empty:
        continue
      except Exception as err:
        logger.exception("Unexpected error in NATS listener: %s", err)


class NATSPublisher:
  """Thread-safe NATS publisher. Owns a daemon thread that runs an asyncio
  event loop with a live NATS connection. Other threads call publish() to
  enqueue outgoing messages; the loop drains the queue and publishes them."""

  def __init__(self, url: str, token: Optional[str] = None):
    self.url = url
    self._token = token
    self._stop_event = threading.Event()
    self._send_queue: queue.Queue[tuple[str, bytes]] = queue.Queue()
    self._loop_thread: Optional[threading.Thread] = None

  async def _run_loop(self) -> None:
    async def error_cb(e):
      logger.error("NATS publisher error: %s", e)

    async def disconnected_cb():
      logger.warning("NATS publisher disconnected from %s. Retrying...", self.url)

    async def reconnected_cb():
      logger.info("NATS publisher reconnected to %s", self.url)

    connect_opts: dict = dict(
      error_cb=error_cb,
      disconnected_cb=disconnected_cb,
      reconnected_cb=reconnected_cb,
      max_reconnect_attempts=-1,
      reconnect_time_wait=5,
    )
    if self._token:
      connect_opts["token"] = self._token

    try:
      nc = await nats.connect(self.url, **connect_opts)
      logger.info("NATS publisher connected to %s", self.url)

      while not self._stop_event.is_set():
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

      await nc.drain()
      logger.info("NATS publisher connection closed.")
    except Exception as exc:
      logger.exception("NATS publisher fatal error: %s", exc)

  def connect(self) -> None:
    loop = asyncio.new_event_loop()
    self._loop_thread = threading.Thread(
      target=loop.run_until_complete,
      args=(self._run_loop(),),
      name="nats-publisher-loop",
      daemon=True,
    )
    self._loop_thread.start()
    logger.info("NATS publisher thread started.")

  def publish(self, subject: NatsSubjectEnum, data: str) -> None:
    """Thread-safe: enqueue a message to be published asynchronously."""
    self._send_queue.put((subject.value, data.encode()))

  def close(self) -> None:
    logger.info("NATS publisher stop requested. Closing...")
    self._stop_event.set()
    if self._loop_thread is not None:
      self._loop_thread.join(timeout=5)
