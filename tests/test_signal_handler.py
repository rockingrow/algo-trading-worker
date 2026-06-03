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

  def get_open_positions(self, symbol, strategy=None):
    self.calls.append(f"get_open:{strategy}")
    return list(self._open)

  def close_all_positions(self, symbol, reason="CLOSE", strategy=None):
    self.calls.append(f"close_all:{reason}:{strategy}")
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
    (SignalActionEnum.FLAT, "close_all:FLAT:strat-1"),
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


def test_flat_closes_mt5_when_no_db_record():
  """FLAT must close MT5 positions even when the DB has no tracked record."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=99)])
  store = FakeStore(positions=[])  # empty DB
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.FLAT))
  assert res["success"] is True
  assert "close_all:FLAT:strat-1" in strat.calls


def test_flat_syncs_stale_db_record_when_no_mt5_positions():
  """FLAT must mark stale DB records as FLATTED when MT5 has no open positions."""
  strat = FakeStrategy(open_positions=[], cleanup_ok=False)
  store = FakeStore(positions=[{"source_ticket": 7, "ticket": 7, "status": "OPENED"}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.FLAT))
  assert res["success"] is False
  assert store.status_updates[0]["source_ticket"] == 7
  assert store.status_updates[0]["status"] == PositionStatusEnum.FLATTED


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
  assert "close_all:STALE_CLEANUP:strat-1" in strat.calls
  assert store.status_updates[0]["status"] == PositionStatusEnum.FORCED_CLOSED
  assert res["forced_closed"][0]["source_ticket"] == 9


def test_entry_scopes_preflight_to_signal_strategy():
  """Stale check + force-close must be scoped to the signal's strategy so a
  concurrent strategy on the same symbol is never touched."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(positions=[{"source_ticket": 9, "ticket": 9, "volume": 1.0}])
  handler = SignalHandler(strat, store)
  handler.handle(make_signal(SignalActionEnum.SHORT, strategy="strat-short"))
  assert "get_open:strat-short" in strat.calls
  assert "close_all:STALE_CLEANUP:strat-short" in strat.calls


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


def test_get_db_position_heals_duplicate_active_rows():
  """If the DB has > 1 OPENED/TP1 row for the same strategy+symbol, the handler
  must keep the oldest and immediately mark the rest FORCED_CLOSED."""
  dup_rows = [
    {"source_ticket": 10, "ticket": 10, "status": "OPENED"},
    {"source_ticket": 11, "ticket": 11, "status": "OPENED"},  # duplicate
    {"source_ticket": 12, "ticket": 12, "status": "TP1"},     # duplicate
  ]
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=10)])
  store = FakeStore(positions=dup_rows)
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.TP2))

  # Only source_ticket=10 (oldest) is used; 11 and 12 are healed.
  assert res.get("source_ticket") == 10
  healed_tickets = {u["source_ticket"] for u in store.status_updates}
  assert healed_tickets == {11, 12}
  assert all(
    u["status"] == PositionStatusEnum.FORCED_CLOSED
    for u in store.status_updates
    if u["source_ticket"] in {11, 12}
  )
