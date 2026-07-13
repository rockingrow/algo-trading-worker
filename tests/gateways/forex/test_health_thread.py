"""
tests/gateways/forex/test_health_thread.py
──────────────────────────────────────────
The FOREX health thread must treat a weekend (broker trade server offline for
weekly maintenance) differently from a weekday crash: **close** the MT5
connection once and stay idle at the slow weekend cadence until the market
reopens, then reconnect on the first weekday check — so it stops flooding the
logs and Telegram while the market is closed. A weekday disconnect still gets
the full aggressive relaunch/reconnect path.
"""

from datetime import datetime, timezone

import worker.gateways.forex.signal_processor as sp
from worker.settings import (
  MT5_HEALTH_INTERVAL,
  MT5_HEALTH_INTERVAL_WEEKEND,
  is_market_closed,
)


class _FakeStopEvent:
  """Runs the health loop for exactly `iterations` passes, then breaks."""

  def __init__(self, iterations: int = 1) -> None:
    self._remaining = iterations
    self.wait_intervals: list[float] = []

  def is_set(self) -> bool:
    if self._remaining <= 0:
      return True
    return False

  def wait(self, interval: float) -> bool:
    self.wait_intervals.append(interval)
    self._remaining -= 1
    return False  # never short-circuit; is_set() ends the loop next check


class _FakeGateway:
  def __init__(self, connected: bool = False, reconnect_result: bool = False) -> None:
    self.name = "MT5"
    self._connected = connected
    self._reconnect_result = reconnect_result
    self.reconnect_calls: list[dict] = []
    self.restart_calls = 0
    self.close_calls = 0

  def is_connected(self) -> bool:
    return self._connected

  def reconnect(self, max_attempts: int = 0, delay_seconds: float = 10.0) -> bool:
    self.reconnect_calls.append(
      {"max_attempts": max_attempts, "delay_seconds": delay_seconds}
    )
    return self._reconnect_result

  def restart_terminal(self, startup_wait: float = 15.0) -> bool:
    self.restart_calls += 1
    return False

  def close(self) -> None:
    self.close_calls += 1
    self._connected = False


def _weekend_sequence(monkeypatch, values):
  """Patch is_market_closed to yield each value in `values`, one per loop pass."""
  it = iter(values)
  monkeypatch.setattr(sp, "is_market_closed", lambda: next(it))


class _FakeNotifier:
  def __init__(self) -> None:
    self.messages: list[str] = []

  def send_message(self, msg: str) -> None:
    self.messages.append(msg)


# ── is_market_closed (window: Fri 22:00 UTC → Sun 22:00 UTC) ─────────────── #
# July 2026: 10th=Fri, 11th=Sat, 12th=Sun, 13th=Mon.


def test_friday_before_close_is_open():
  # Fri 21:00 UTC — market still open (closes at 22:00 UTC).
  assert is_market_closed(datetime(2026, 7, 10, 21, 0, tzinfo=timezone.utc)) is False


def test_friday_at_close_hour_is_closed():
  # Fri 22:00 UTC — the close boundary is inclusive.
  assert is_market_closed(datetime(2026, 7, 10, 22, 0, tzinfo=timezone.utc)) is True


def test_saturday_is_closed_all_day():
  assert is_market_closed(datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)) is True


def test_sunday_noon_utc7_is_closed():
  # Sun 12:00 UTC+7 == 05:00 UTC — the reported disconnect window.
  assert is_market_closed(datetime(2026, 7, 12, 5, 0, tzinfo=timezone.utc)) is True


def test_sunday_just_before_open_is_closed():
  # Sun 21:00 UTC — still closed until the 22:00 UTC reopen.
  assert is_market_closed(datetime(2026, 7, 12, 21, 0, tzinfo=timezone.utc)) is True


def test_sunday_at_open_hour_is_open():
  # Sun 22:00 UTC — market reopens; the open boundary is exclusive of "closed".
  assert is_market_closed(datetime(2026, 7, 12, 22, 0, tzinfo=timezone.utc)) is False


def test_wednesday_is_open():
  assert is_market_closed(datetime(2026, 7, 8, 5, 0, tzinfo=timezone.utc)) is False


def test_naive_datetime_is_treated_as_utc():
  # A naive Saturday must be read as UTC, not local system time.
  assert is_market_closed(datetime(2026, 7, 11, 12, 0)) is True


# ── _health_thread: weekend closes the connection ────────────────────────── #


def test_weekend_closes_connection_and_does_not_reconnect(monkeypatch):
  _weekend_sequence(monkeypatch, [True])
  gateway = _FakeGateway(connected=True)  # still connected when the weekend hits
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=1)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  # Backs off to the slow weekend cadence.
  assert stop.wait_intervals == [MT5_HEALTH_INTERVAL_WEEKEND]
  # Closes the connection exactly once and never retries while the market is shut.
  assert gateway.close_calls == 1
  assert gateway.reconnect_calls == []
  assert gateway.restart_calls == 0
  # One "Market Closed" notice.
  assert len(notifier.messages) == 1
  assert "Market Closed" in notifier.messages[0]


def test_weekend_closes_only_once_across_iterations(monkeypatch):
  _weekend_sequence(monkeypatch, [True, True, True])
  gateway = _FakeGateway(connected=True)
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=3)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  # Three weekend passes, but the connection is closed (and announced) only once.
  assert gateway.close_calls == 1
  assert len(notifier.messages) == 1
  assert stop.wait_intervals == [MT5_HEALTH_INTERVAL_WEEKEND] * 3


def test_reopen_after_weekend_reconnects(monkeypatch):
  # Pass 1: weekend → close. Pass 2: weekday → market reopened → reconnect.
  _weekend_sequence(monkeypatch, [True, False])
  gateway = _FakeGateway(connected=True, reconnect_result=True)
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=2)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  # Closed for the weekend, then reconnected through the normal path on reopen.
  assert gateway.close_calls == 1
  assert gateway.reconnect_calls and gateway.reconnect_calls[0]["max_attempts"] == 15
  assert gateway.restart_calls == 0  # reconnect succeeded, no terminal relaunch
  # Cadence: slow on the weekend pass, fast on the weekday reopen pass.
  assert stop.wait_intervals == [MT5_HEALTH_INTERVAL_WEEKEND, MT5_HEALTH_INTERVAL]


# ── _health_thread: weekday path unchanged ───────────────────────────────── #


def test_weekday_disconnect_uses_aggressive_path(monkeypatch):
  monkeypatch.setattr(sp, "is_market_closed", lambda: False)
  gateway = _FakeGateway(connected=False)  # reconnect keeps failing
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=1)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  # Normal fast cadence.
  assert stop.wait_intervals == [MT5_HEALTH_INTERVAL]
  # Weekday path retries hard (15 attempts) and then restarts the terminal.
  assert gateway.reconnect_calls[0]["max_attempts"] == 15
  assert gateway.restart_calls == 1


def test_connected_gateway_does_nothing(monkeypatch):
  monkeypatch.setattr(sp, "is_market_closed", lambda: False)
  gateway = _FakeGateway(connected=True)
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=1)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  assert gateway.reconnect_calls == []
  assert gateway.restart_calls == 0
  assert notifier.messages == []
