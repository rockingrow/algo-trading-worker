"""
Tests for Mt5SignalProcessor._handle_admin_message (NATS ADMIN subject handler).

FakeProc wires up the minimal interface that _handle_admin_message touches so
the method can be exercised without instantiating the full processor (no MT5
bridge, no real DB, no NATS).
"""

import json
from types import SimpleNamespace

from worker.mt5.signal_processor import Mt5SignalProcessor
from worker.schemas.position_schema import PositionStatusEnum

# ── Fakes ─────────────────────────────────────────────────────────────────── #


def _db_pos(ticket=1, source_ticket=1, strategy="strat-A", symbol="XAUUSD"):
  return {
    "ticket": ticket,
    "source_ticket": source_ticket,
    "strategy": strategy,
    "symbol": symbol,
    "status": "OPENED",
  }


def _mt5_pos(ticket=1, type_=0, volume=0.5, symbol="XAUUSDc"):
  return SimpleNamespace(ticket=ticket, type=type_, volume=volume, symbol=symbol, magic=42)


class FakeDbService:
  def __init__(self, flat_positions=None):
    self.flat_calls = []
    self.status_updates = []
    self._flat = list(flat_positions or [])

  def get_open_positions_for_flat(self, strategy=None, symbol=None):
    self.flat_calls.append({"strategy": strategy, "symbol": symbol})
    return list(self._flat)

  def update_position_status(self, **kwargs):
    self.status_updates.append(kwargs)


class FakeExecutor:
  def __init__(self, all_positions=None, close_result=None):
    self._all = list(all_positions or [])
    self._close_result = close_result or {
      "success": True,
      "retcode": 10009,
      "ticket": 999,
      "price": 2000.0,
      "volume": 0.5,
      "comment": "Closed [FLAT]",
    }
    self.closed = []

  def get_all_open_positions(self):
    return list(self._all)

  def close_single_position(self, pos, reason="FLAT"):
    self.closed.append(pos)
    return dict(self._close_result)


class FakeProc:
  """Minimal stand-in for Mt5SignalProcessor for _handle_admin_message tests."""

  def __init__(
    self,
    *,
    account_id="100",
    db_positions=None,
    mt5_positions=None,
    close_result=None,
  ):
    self.settings = {"mt5_login": account_id}
    self._footer = "FOOTER"
    self.bridge = SimpleNamespace(
      is_connected=lambda: True,
      get_account_footer=lambda: "FOOTER",
    )
    self.executor = FakeExecutor(all_positions=mt5_positions, close_result=close_result)
    self.db = FakeDbService(flat_positions=db_positions)
    self.notifications = []
    self.ctx = SimpleNamespace(
      db_service=self.db,
      notifier=SimpleNamespace(send_message=lambda m: None),
      channel_notifier=SimpleNamespace(
        send_message=lambda m: self.notifications.append(m)
      ),
    )

  _handle_admin_message = Mt5SignalProcessor._handle_admin_message


def _payload(**fields):
  data = {"action": "FLAT", "timestamp": "2026-06-02T08:00:00+00:00"}
  data.update(fields)
  return json.dumps(data)


# ── account_id filtering ───────────────────────────────────────────────────── #


def test_wrong_account_id_silently_skips():
  proc = FakeProc(account_id="100", db_positions=[_db_pos()])
  proc._handle_admin_message(_payload(account_id="999"))
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_matching_account_id_proceeds():
  proc = FakeProc(
    account_id="100",
    db_positions=[_db_pos(ticket=1)],
    mt5_positions=[_mt5_pos(ticket=1)],
  )
  proc._handle_admin_message(_payload(account_id="100"))
  assert len(proc.executor.closed) == 1


def test_absent_account_id_closes_all():
  proc = FakeProc(
    db_positions=[_db_pos(ticket=1)],
    mt5_positions=[_mt5_pos(ticket=1)],
  )
  proc._handle_admin_message(_payload())
  assert len(proc.executor.closed) == 1


# ── DB filter forwarding ───────────────────────────────────────────────────── #


def test_strategy_filter_forwarded_to_db():
  proc = FakeProc()
  proc._handle_admin_message(_payload(strategy="my_strat"))
  assert proc.db.flat_calls[0] == {"strategy": "my_strat", "symbol": None}


def test_symbol_filter_forwarded_to_db():
  proc = FakeProc()
  proc._handle_admin_message(_payload(symbol="XAUUSD"))
  assert proc.db.flat_calls[0] == {"strategy": None, "symbol": "XAUUSD"}


def test_no_filters_queries_all():
  proc = FakeProc()
  proc._handle_admin_message(_payload())
  assert proc.db.flat_calls[0] == {"strategy": None, "symbol": None}


# ── Empty DB result short-circuits ────────────────────────────────────────── #


def test_no_db_positions_skips_close():
  proc = FakeProc(db_positions=[], mt5_positions=[_mt5_pos(ticket=1)])
  proc._handle_admin_message(_payload())
  assert proc.executor.closed == []


# ── Successful close ──────────────────────────────────────────────────────── #


def test_successful_close_updates_db_status():
  proc = FakeProc(
    db_positions=[_db_pos(ticket=1, source_ticket=1)],
    mt5_positions=[_mt5_pos(ticket=1)],
  )
  proc._handle_admin_message(_payload())
  assert len(proc.db.status_updates) == 1
  upd = proc.db.status_updates[0]
  assert upd["source_ticket"] == 1
  assert upd["status"] == PositionStatusEnum.FLATTED
  assert upd["closed_price"] == 2000.0


def test_successful_close_sends_notification():
  proc = FakeProc(
    db_positions=[_db_pos(ticket=1)],
    mt5_positions=[_mt5_pos(ticket=1)],
  )
  proc._handle_admin_message(_payload())
  assert len(proc.notifications) == 1
  assert "Admin FLAT" in proc.notifications[0]


def test_multiple_positions_all_closed():
  db_positions = [_db_pos(ticket=i, source_ticket=i) for i in (1, 2, 3)]
  mt5_positions = [_mt5_pos(ticket=i) for i in (1, 2, 3)]
  proc = FakeProc(db_positions=db_positions, mt5_positions=mt5_positions)
  proc._handle_admin_message(_payload())
  assert len(proc.executor.closed) == 3
  assert len(proc.db.status_updates) == 3
  assert all(u["status"] == PositionStatusEnum.FLATTED for u in proc.db.status_updates)


# ── Failed close ──────────────────────────────────────────────────────────── #


def test_failed_close_skips_db_update_and_notification():
  proc = FakeProc(
    db_positions=[_db_pos(ticket=1)],
    mt5_positions=[_mt5_pos(ticket=1)],
    close_result={"success": False, "retcode": -1, "comment": "requote"},
  )
  proc._handle_admin_message(_payload())
  assert proc.db.status_updates == []
  assert proc.notifications == []


# ── DB-only position (already closed in MT5) ──────────────────────────────── #


def test_db_position_absent_from_mt5_still_marked_flatted():
  proc = FakeProc(
    db_positions=[_db_pos(ticket=1, source_ticket=1)],
    mt5_positions=[],  # position already gone from MT5
  )
  proc._handle_admin_message(_payload())
  assert len(proc.db.status_updates) == 1
  assert proc.db.status_updates[0]["source_ticket"] == 1
  assert proc.db.status_updates[0]["status"] == PositionStatusEnum.FLATTED
  assert proc.executor.closed == []  # nothing to close in MT5


# ── Malformed / invalid payloads ──────────────────────────────────────────── #


def test_malformed_json_is_ignored():
  proc = FakeProc()
  proc._handle_admin_message("not json {{{")
  assert proc.db.flat_calls == []


def test_invalid_action_enum_is_ignored():
  proc = FakeProc()
  proc._handle_admin_message(
    json.dumps({"action": "NUKE", "timestamp": "2026-06-02T08:00:00+00:00"})
  )
  assert proc.db.flat_calls == []
