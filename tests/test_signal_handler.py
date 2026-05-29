from types import SimpleNamespace

import pytest
from conftest import make_signal

from worker.core.market_strategy import BaseMarketStrategy
from worker.core.signal_handler import SignalHandler
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalActionEnum


class FakeStrategy(BaseMarketStrategy):
  def __init__(self, open_positions=None, entry_ok=True, cleanup_ok=True):
    self.calls = []
    self._open = open_positions if open_positions is not None else []
    self._entry_ok = entry_ok
    self._cleanup_ok = cleanup_ok

  def entry(self, signal):
    self.calls.append("entry")
    return {"success": self._entry_ok, "retcode": 0, "ticket": 1, "volume": 1, "price": 2}

  def handle_tp1(self, signal):
    self.calls.append("tp1")
    return {"success": True, "retcode": 0, "volume": 1, "price": 2}

  def handle_full_close(self, signal):
    self.calls.append("full_close")
    return {"success": True, "retcode": 0, "volume": 1, "price": 2}

  def get_open_positions(self, symbol):
    return list(self._open)

  def close_all_positions(self, symbol, reason="CLOSE"):
    self.calls.append(f"close_all:{reason}")
    return {"success": self._cleanup_ok, "retcode": 0, "price": 1999.0}


class FakeStore:
  def __init__(self, positions=None):
    self._positions = positions if positions is not None else []
    self.status_updates = []

  def get_open_positions_by_strategy(self, strategy, symbol):
    return list(self._positions)

  def update_position_status(self, **kwargs):
    self.status_updates.append(kwargs)


@pytest.mark.parametrize(
  "action,expected",
  [
    (SignalActionEnum.LONG, "entry"),
    (SignalActionEnum.SHORT, "entry"),
    (SignalActionEnum.TP1, "tp1"),
    (SignalActionEnum.TP2, "full_close"),
    (SignalActionEnum.SL, "full_close"),
    (SignalActionEnum.R_SL, "full_close"),
    (SignalActionEnum.FLAT, "full_close"),
  ],
)
def test_dispatch_routes_every_action(action, expected):
  open_pos = [SimpleNamespace(ticket=9)]
  strat = FakeStrategy(open_positions=open_pos)
  store = FakeStore(positions=[{"source_ticket": 9, "ticket": 9, "status": "OPENED"}])
  handler = SignalHandler(strat, store)
  handler.handle(make_signal(action))
  assert strat.calls[-1] == expected


def test_flat_routes_through_handler():
  """Regression: FLAT must go through the handler, not a special pre-branch."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(positions=[{"source_ticket": 9, "ticket": 9, "status": "OPENED"}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.FLAT))
  assert res["success"] is True
  assert res["source_ticket"] == 9


def test_entry_no_stale_position():
  strat = FakeStrategy(open_positions=[])
  store = FakeStore()
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.LONG))
  assert res["success"] is True
  assert "close_all:STALE_CLEANUP" not in strat.calls


def test_entry_force_closes_stale_and_marks_db():
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(positions=[{"source_ticket": 9, "ticket": 9, "volume": 1.0}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.LONG))
  assert "close_all:STALE_CLEANUP" in strat.calls
  assert store.status_updates[0]["status"] == PositionStatusEnum.FORCED_CLOSED
  assert res["forced_closed"][0]["source_ticket"] == 9


def test_entry_aborts_when_cleanup_fails():
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)], cleanup_ok=False)
  store = FakeStore(positions=[{"source_ticket": 9, "ticket": 9}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.LONG))
  assert res["success"] is False
  assert "entry" not in strat.calls  # never reached entry


def test_exit_returns_failure_when_no_db_record():
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(positions=[])  # nothing tracked
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.TP2))
  assert res["success"] is False
  assert "full_close" not in strat.calls


def test_exit_returns_failure_when_no_live_mt5_position():
  strat = FakeStrategy(open_positions=[])  # gone from MT5
  store = FakeStore(positions=[{"source_ticket": 9, "ticket": 9, "status": "OPENED"}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.SL))
  assert res["success"] is False
  assert "full_close" not in strat.calls
