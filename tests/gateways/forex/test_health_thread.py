"""
tests/gateways/forex/test_health_thread.py
──────────────────────────────────────────
The FOREX health thread must treat a weekend disconnect (broker trade server
offline for weekly maintenance) differently from a weekday crash: back off to a
slower cadence, skip the kill/relaunch storm, and notify only once — so it stops
flooding the logs and Telegram while the market is closed.
"""

from datetime import datetime, timedelta, timezone

import worker.gateways.forex.signal_processor as sp
from worker.settings import (
  MARKET_TZ_OFFSET_HOURS,
  MT5_HEALTH_INTERVAL,
  MT5_HEALTH_INTERVAL_WEEKEND,
  is_market_weekend,
)

_TZ = timezone(timedelta(hours=MARKET_TZ_OFFSET_HOURS))


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
  def __init__(self, connected: bool = False) -> None:
    self.name = "MT5"
    self._connected = connected
    self.reconnect_calls: list[dict] = []
    self.restart_calls = 0

  def is_connected(self) -> bool:
    return self._connected

  def reconnect(self, max_attempts: int = 0, delay_seconds: float = 10.0) -> bool:
    self.reconnect_calls.append(
      {"max_attempts": max_attempts, "delay_seconds": delay_seconds}
    )
    return self._connected

  def restart_terminal(self, startup_wait: float = 15.0) -> bool:
    self.restart_calls += 1
    return False


class _FakeNotifier:
  def __init__(self) -> None:
    self.messages: list[str] = []

  def send_message(self, msg: str) -> None:
    self.messages.append(msg)


# ── is_market_weekend ─────────────────────────────────────────────────────── #


def test_sunday_noon_utc7_is_weekend():
  # Sun 2026-07-12 12:00 UTC+7 == 05:00 UTC — the reported disconnect window.
  moment = datetime(2026, 7, 12, 5, 0, tzinfo=timezone.utc)
  assert is_market_weekend(moment) is True


def test_wednesday_is_not_weekend():
  moment = datetime(2026, 7, 8, 5, 0, tzinfo=timezone.utc)
  assert is_market_weekend(moment) is False


def test_friday_evening_utc7_still_weekday():
  # Fri 23:00 UTC+7 == Fri 16:00 UTC — still a weekday in market-local time.
  moment = datetime(2026, 7, 10, 16, 0, tzinfo=timezone.utc)
  assert is_market_weekend(moment) is False


def test_naive_datetime_is_treated_as_market_local():
  # A naive Saturday noon must be read in market-local time, not assumed UTC.
  moment = datetime(2026, 7, 11, 12, 0)
  assert is_market_weekend(moment.replace(tzinfo=_TZ)) is True


# ── _health_thread: weekend backoff ──────────────────────────────────────── #


def test_weekend_disconnect_backs_off_and_skips_terminal_restart(monkeypatch):
  monkeypatch.setattr(sp, "is_market_weekend", lambda: True)
  gateway = _FakeGateway(connected=False)
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=1)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  # Backs off to the slow weekend cadence.
  assert stop.wait_intervals == [MT5_HEALTH_INTERVAL_WEEKEND]
  # A single lightweight reconnect attempt — never the 15-attempt storm.
  assert gateway.reconnect_calls == [{"max_attempts": 1, "delay_seconds": 0}]
  # Never kills/relaunches the terminal while the server is down for maintenance.
  assert gateway.restart_calls == 0


def test_weekend_notice_sent_only_once(monkeypatch):
  monkeypatch.setattr(sp, "is_market_weekend", lambda: True)
  gateway = _FakeGateway(connected=False)
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=3)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  # Three disconnected passes, but only one "Market Closed" Telegram message.
  assert len(notifier.messages) == 1
  assert "Market Closed" in notifier.messages[0]
  assert len(gateway.reconnect_calls) == 3


# ── _health_thread: weekday path unchanged ───────────────────────────────── #


def test_weekday_disconnect_uses_aggressive_path(monkeypatch):
  monkeypatch.setattr(sp, "is_market_weekend", lambda: False)
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
  monkeypatch.setattr(sp, "is_market_weekend", lambda: False)
  gateway = _FakeGateway(connected=True)
  notifier = _FakeNotifier()
  stop = _FakeStopEvent(iterations=1)

  sp._health_thread(gateway, notifier, lambda: "", stop, sp.log)

  assert gateway.reconnect_calls == []
  assert gateway.restart_calls == 0
  assert notifier.messages == []
