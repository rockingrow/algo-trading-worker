import asyncio
import threading
from typing import Any, Awaitable, Callable, Optional

import nats

from worker.logger import get_logger

logger = get_logger("worker.nats_client")


class NatsClient:
  """Manages a single NATS connection lifecycle in a dedicated daemon thread.

  Usage: call start(body) where body is ``async def body(nc, stop_event)``.
  The body coroutine runs until stop_event is set, then drain() is called.
  """

  def __init__(
    self,
    url: str,
    token: Optional[str] = None,
    name: Optional[str] = None,
    error_cb: Optional[Callable[..., Awaitable[None]]] = None,
    disconnected_cb: Optional[Callable[[], Awaitable[None]]] = None,
    reconnected_cb: Optional[Callable[[], Awaitable[None]]] = None,
  ):
    self.url = url
    self._token = token
    self._name = name
    self._error_cb = error_cb
    self._disconnected_cb = disconnected_cb
    self._reconnected_cb = reconnected_cb
    self.nc: Optional[Any] = None
    self._loop: Optional[asyncio.AbstractEventLoop] = None
    self._stop_event = threading.Event()
    self._thread: Optional[threading.Thread] = None

  async def _connect(self) -> Any:
    opts: dict = {
      "max_reconnect_attempts": -1,
      "reconnect_time_wait": 5,
    }
    if self._error_cb:
      opts["error_cb"] = self._error_cb
    if self._disconnected_cb:
      opts["disconnected_cb"] = self._disconnected_cb
    if self._reconnected_cb:
      opts["reconnected_cb"] = self._reconnected_cb
    if self._token:
      opts["token"] = self._token
    # Connection name is surfaced to the NATS server (CONNECT protocol /
    # /connz monitoring), so the Broker can see which account is connecting.
    if self._name:
      opts["name"] = self._name
    return await nats.connect(self.url, **opts)

  async def _run(self, body: Callable[..., Awaitable[None]]) -> None:
    try:
      nc = await self._connect()
      self.nc = nc
      try:
        await body(nc, self._stop_event)
      finally:
        await nc.drain()
        self.nc = None
    except Exception as exc:
      logger.exception("NatsClient fatal error: %s", exc)

  def start(
    self,
    body: Callable[..., Awaitable[None]],
    thread_name: str = "nats-loop",
  ) -> None:
    loop = asyncio.new_event_loop()
    self._loop = loop
    self._thread = threading.Thread(
      target=loop.run_until_complete,
      args=(self._run(body),),
      name=thread_name,
      daemon=True,
    )
    self._thread.start()
    logger.info("NatsClient thread '%s' started.", thread_name)

  def stop(self) -> None:
    logger.info("NatsClient stop requested.")
    self._stop_event.set()
    if self._thread is not None:
      self._thread.join(timeout=5)
