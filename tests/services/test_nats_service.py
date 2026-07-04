"""Tests for NATSPublisher.request() — the request/reply handshake helper.

Exercises the thread-boundary bridge (asyncio.run_coroutine_threadsafe) without
a live NATS server: a background event loop plus a fake ``nc`` stand in for
the real nats.py client.
"""

import asyncio
import threading

import pytest

from worker.schemas.nats_schema import NatsSubjectEnum
from worker.services.nats_service import NATSPublisher


class _Msg:
  def __init__(self, data: bytes):
    self.data = data


class FakeNc:
  """Fake nats.py client: request() echoes back a canned reply."""

  def __init__(self, reply: bytes | None = None, exc: Exception | None = None):
    self._reply = reply
    self._exc = exc
    self.calls = []

  async def request(self, subject, data, timeout=5.0):
    self.calls.append((subject, data, timeout))
    if self._exc is not None:
      raise self._exc
    return _Msg(self._reply)


@pytest.fixture
def running_loop():
  """A real asyncio event loop running in its own daemon thread, mirroring how
  NatsClient runs its connection loop."""
  loop = asyncio.new_event_loop()

  def _run():
    asyncio.set_event_loop(loop)
    loop.run_forever()
    loop.close()

  thread = threading.Thread(target=_run, daemon=True)
  thread.start()
  yield loop
  loop.call_soon_threadsafe(loop.stop)
  thread.join(timeout=2)


def _publisher_with_fake_client(loop, nc) -> NATSPublisher:
  publisher = NATSPublisher(url="nats://fake", publish_subjects=[NatsSubjectEnum.SYSTEM])
  # Bypass the real connect(): wire the fake loop/nc directly onto the
  # underlying NatsClient, exactly as NatsClient._run() would after connecting.
  publisher._client._loop = loop
  publisher._client.nc = nc
  return publisher


def test_request_returns_decoded_reply(running_loop):
  nc = FakeNc(reply=b'{"action":"WORKER_CONNECTED_ACK"}')
  publisher = _publisher_with_fake_client(running_loop, nc)

  result = publisher.request(NatsSubjectEnum.SYSTEM, '{"action":"WORKER_CONNECTED"}', timeout=1)

  assert result == '{"action":"WORKER_CONNECTED_ACK"}'
  assert nc.calls == [("SYSTEM", b'{"action":"WORKER_CONNECTED"}', 1)]


def test_request_propagates_transport_errors(running_loop):
  nc = FakeNc(exc=TimeoutError("no reply"))
  publisher = _publisher_with_fake_client(running_loop, nc)

  with pytest.raises(TimeoutError):
    publisher.request(NatsSubjectEnum.SYSTEM, '{"action":"WORKER_CONNECTED"}', timeout=1)


def test_request_without_connection_raises_connection_error():
  publisher = NATSPublisher(url="nats://fake", publish_subjects=[NatsSubjectEnum.SYSTEM])
  # Never connected: _client._loop / _client.nc are both still None.
  with pytest.raises(ConnectionError):
    publisher.request(NatsSubjectEnum.SYSTEM, '{"action":"WORKER_CONNECTED"}', timeout=1)
