"""
Tests for the shared ``BaseSignalProcessor._handle_admin_message`` (NATS ADMIN
FLAT subject).

The FLAT skeleton lives on the broker-agnostic base; only the DB-row↔live-close
*match key* differs per market (MT5 → ticket, a CEX → resolved symbol). These
tests exercise the base method against a fake ``self`` (no MetaTrader5 / exchange
import, so they run on any platform) and cover BOTH match styles — including the
invariant that an *attempted-but-failed* close leaves the DB row OPEN.
"""

import json
from types import SimpleNamespace

from worker.gateways.processor import BaseSignalProcessor
from worker.schemas.position_schema import PositionStatusEnum


def _db_pos(ref_id=1, ref_source_id=1, strategy="strat-A", symbol="XAUUSD"):
  return {
    "ref_id": ref_id,
    "ref_source_id": ref_source_id,
    "strategy": strategy,
    "symbol": symbol,
    "status": "OPENED",
  }


def _pos(ticket=1, symbol="XAUUSD", volume=0.5):
  return SimpleNamespace(ticket=ticket, symbol=symbol, volume=volume)


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
  def __init__(self, positions=None, close_result=None, results_by_key=None, match_style="ticket"):
    self._positions = list(positions or [])
    self._default = close_result or {
      "success": True, "retcode": 0, "ticket": 999, "price": 2000.0,
      "volume": 0.5, "comment": "Closed [FLAT]",
    }
    self._results_by_key = results_by_key or {}
    self._match_style = match_style
    self.closed = []

  def get_all_open_positions(self, strategy=None):
    return list(self._positions)

  def get_open_positions(self, symbol, strategy=None):
    return list(self._positions)

  def close_single_position(self, pos, reason="FLAT"):
    self.closed.append(pos)
    key = pos.symbol if self._match_style == "symbol" else pos.ticket
    return dict(self._results_by_key.get(key, self._default))

  def get_symbol(self, symbol):
    return symbol  # tests already use resolved symbols


class FakePresenter:
  @staticmethod
  def admin_flat_closed(db_pos, result, footer):
    return f"Admin FLAT {db_pos['symbol']}"


class FakeProc:
  """Minimal stand-in providing exactly the hooks the base FLAT handler touches."""

  def __init__(
    self, *, account_id="100", market_type="FOREX", gateway_value="MT5",
    db_positions=None, positions=None,
    close_result=None, results_by_key=None, match_style="ticket", connected=True,
  ):
    self.name = "TEST"
    self.presenter = FakePresenter
    self._account_id = account_id
    self._market_type = market_type
    self._gateway_value = gateway_value
    self._connected = connected
    self._match_style = match_style
    self.executor = FakeExecutor(
      positions=positions, close_result=close_result,
      results_by_key=results_by_key, match_style=match_style,
    )
    self.db = FakeDbService(flat_positions=db_positions)
    self.notifications = []
    self.ctx = SimpleNamespace(
      db_service=self.db,
      channel_notifier=SimpleNamespace(
        send_message=lambda m: self.notifications.append(m)
      ),
    )

  def _ensure_connected(self):
    return self._connected

  def _account_footer(self):
    return "FOOTER"

  def _flat_match_key(self, pos):
    return pos.symbol if self._match_style == "symbol" else pos.ticket

  def _flat_db_match_keys(self, db_pos):
    if self._match_style == "symbol":
      return {self.executor.get_symbol(db_pos["symbol"])}
    return {db_pos.get("ref_id"), db_pos.get("ref_source_id")}

  _handle_admin_message = BaseSignalProcessor._handle_admin_message
  _flat_targets_this_worker = BaseSignalProcessor._flat_targets_this_worker
  _close_live_positions_for_flat = BaseSignalProcessor._close_live_positions_for_flat
  _reconcile_flat_db = BaseSignalProcessor._reconcile_flat_db


def _payload(**fields):
  data = {"action": "FLAT", "timestamp": "2026-06-02T08:00:00+00:00"}
  data.update(fields)
  return json.dumps(data)


# ── account_id routing ──────────────────────────────────────────────────────


def test_wrong_account_id_silently_skips():
  proc = FakeProc(account_id="100", db_positions=[_db_pos()])
  proc._handle_admin_message(_payload(account_id="999"))
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_matching_account_id_proceeds():
  proc = FakeProc(account_id="100", db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)])
  proc._handle_admin_message(_payload(account_id="100"))
  assert len(proc.executor.closed) == 1


def test_absent_account_id_closes_all():
  proc = FakeProc(db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)])
  proc._handle_admin_message(_payload())
  assert len(proc.executor.closed) == 1


# ── composite (market, gateway, account_id) routing ─────────────────────────
# account_id alone is not a unique worker identity — it can collide across
# markets/gateways (e.g. an email account_id also matching a CRYPTO gateway
# name), so the broker now sends market/gateway alongside account_id and a
# FLAT must match all three before this worker acts on it.


def test_matching_account_id_wrong_market_skips():
  proc = FakeProc(account_id="100", market_type="FOREX", db_positions=[_db_pos()])
  proc._handle_admin_message(_payload(account_id="100", market="CRYPTO"))
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_matching_account_id_wrong_gateway_skips():
  proc = FakeProc(account_id="100", gateway_value="MT5", db_positions=[_db_pos()])
  proc._handle_admin_message(_payload(account_id="100", gateway="BINANCE"))
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_matching_composite_key_proceeds():
  proc = FakeProc(
    account_id="100", market_type="FOREX", gateway_value="MT5",
    db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)],
  )
  proc._handle_admin_message(
    _payload(account_id="100", market="FOREX", gateway="MT5")
  )
  assert len(proc.executor.closed) == 1


def test_colliding_account_id_across_gateways_only_matches_owner():
  # Same raw account_id ("shared@example.com") used on two different gateways —
  # only the worker whose (market, gateway) also matches should act.
  mt5_proc = FakeProc(
    account_id="shared@example.com", market_type="FOREX", gateway_value="MT5",
    db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)],
  )
  binance_proc = FakeProc(
    account_id="shared@example.com", market_type="CRYPTO", gateway_value="BINANCE",
    db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)],
  )
  payload = _payload(
    account_id="shared@example.com", market="CRYPTO", gateway="BINANCE",
  )
  mt5_proc._handle_admin_message(payload)
  binance_proc._handle_admin_message(payload)
  assert mt5_proc.executor.closed == []
  assert len(binance_proc.executor.closed) == 1


# ── DB filter forwarding ─────────────────────────────────────────────────────


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


# ── broker-first: live positions closed even with no DB records ──────────────


def test_live_positions_closed_even_with_no_db_records():
  proc = FakeProc(db_positions=[], positions=[_pos(ticket=1)])
  proc._handle_admin_message(_payload())
  assert len(proc.executor.closed) == 1
  assert proc.db.status_updates == []


# ── successful close reconciles the DB ───────────────────────────────────────


def test_successful_close_updates_db_status():
  proc = FakeProc(db_positions=[_db_pos(ref_id=1, ref_source_id=1)], positions=[_pos(ticket=1)])
  proc._handle_admin_message(_payload())
  assert len(proc.db.status_updates) == 1
  upd = proc.db.status_updates[0]
  assert upd["ref_source_id"] == 1
  assert upd["status"] == PositionStatusEnum.FLATTED
  assert upd["closed_price"] == 2000.0


def test_successful_close_sends_notification():
  proc = FakeProc(db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)])
  proc._handle_admin_message(_payload())
  assert len(proc.notifications) == 1
  assert "Admin FLAT" in proc.notifications[0]


def test_multiple_positions_all_closed():
  db_positions = [_db_pos(ref_id=i, ref_source_id=i, symbol=f"S{i}") for i in (1, 2, 3)]
  positions = [_pos(ticket=i, symbol=f"S{i}") for i in (1, 2, 3)]
  proc = FakeProc(db_positions=db_positions, positions=positions)
  proc._handle_admin_message(_payload())
  assert len(proc.executor.closed) == 3
  assert len(proc.db.status_updates) == 3
  assert all(u["status"] == PositionStatusEnum.FLATTED for u in proc.db.status_updates)


# ── failed close MUST leave the DB row OPEN (regression: was wrongly FLATTED) ─


def test_failed_close_ticket_style_leaves_db_open():
  proc = FakeProc(
    db_positions=[_db_pos(ref_id=1, ref_source_id=1)],
    positions=[_pos(ticket=1)],
    close_result={"success": False, "retcode": -1, "comment": "requote"},
  )
  proc._handle_admin_message(_payload())
  assert proc.db.status_updates == []  # still live → not marked FLATTED
  assert proc.notifications == []


def test_failed_close_symbol_style_leaves_db_open():
  proc = FakeProc(
    match_style="symbol",
    db_positions=[_db_pos(symbol="BTCUSDT")],
    positions=[_pos(symbol="BTCUSDT")],
    close_result={"success": False, "retcode": -1, "comment": "insufficient margin"},
  )
  proc._handle_admin_message(_payload())
  assert proc.db.status_updates == []  # still live on the exchange → not FLATTED
  assert proc.notifications == []


# ── crypto symbol matching reconciles a successful close ─────────────────────


def test_symbol_style_successful_close_updates_db():
  proc = FakeProc(
    match_style="symbol",
    db_positions=[_db_pos(symbol="BTCUSDT")],
    positions=[_pos(symbol="BTCUSDT")],
  )
  proc._handle_admin_message(_payload())
  assert len(proc.db.status_updates) == 1
  assert proc.db.status_updates[0]["status"] == PositionStatusEnum.FLATTED


# ── DB-only position (already closed on the broker) is synced FLATTED ─────────


def test_db_position_absent_from_broker_still_marked_flatted():
  proc = FakeProc(db_positions=[_db_pos(ref_id=1, ref_source_id=1)], positions=[])
  proc._handle_admin_message(_payload())
  assert len(proc.db.status_updates) == 1
  assert proc.db.status_updates[0]["ref_source_id"] == 1
  assert proc.db.status_updates[0]["status"] == PositionStatusEnum.FLATTED
  assert proc.executor.closed == []


# ── malformed / invalid payloads ─────────────────────────────────────────────


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


def test_not_connected_short_circuits():
  proc = FakeProc(db_positions=[_db_pos()], positions=[_pos()], connected=False)
  proc._handle_admin_message(_payload())
  assert proc.executor.closed == []
  assert proc.db.flat_calls == []
