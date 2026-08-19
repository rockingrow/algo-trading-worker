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
from worker.schemas.cycle_schema import CycleOutcomeEnum, CycleStatusEnum
from worker.schemas.position_schema import PositionStatusEnum


def _db_pos(
  ref_id=1, ref_source_id=1, strategy="strat-A", symbol="XAUUSD", signal_uxid=None
):
  return {
    "ref_id": ref_id,
    "ref_source_id": ref_source_id,
    "strategy": strategy,
    "symbol": symbol,
    "status": "OPENED",
    "signal_uxid": signal_uxid,
  }


def _pos(ticket=1, symbol="XAUUSD", volume=0.5):
  return SimpleNamespace(ticket=ticket, symbol=symbol, volume=volume)


class FakeDbService:
  def __init__(self, flat_positions=None):
    self.flat_calls = []
    self.status_updates = []
    self.logs = []
    self._flat = list(flat_positions or [])

  def get_open_positions_for_flat(self, strategy=None, symbol=None):
    self.flat_calls.append({"strategy": strategy, "symbol": symbol})
    return list(self._flat)

  def update_position_status(self, **kwargs):
    self.status_updates.append(kwargs)

  def log_position(self, **kwargs):
    self.logs.append(kwargs)


class FakeExecutor:
  def __init__(
    self, positions=None, close_result=None, results_by_key=None, match_style="ticket"
  ):
    self._positions = list(positions or [])
    self._default = close_result or {
      "success": True,
      "retcode": 0,
      "ticket": 999,
      "price": 2000.0,
      "volume": 0.5,
      "comment": "Closed [FLAT]",
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

  @staticmethod
  def signals_blocked(footer):
    return "Signals Blocked"

  @staticmethod
  def signals_allowed(footer):
    return "Signals Allowed"


class FakeCycleNotifier:
  """Stand-in for CycleNotifier — owns an action only when it has a uxid."""

  def __init__(self):
    self.recorded = []

  def record(self, *, signal_uxid, strategy, symbol, event, status=None):
    if not signal_uxid:
      return False
    self.recorded.append((signal_uxid, strategy, symbol, event, status))
    return True


class FakeProc:
  """Minimal stand-in providing exactly the hooks the base FLAT handler touches."""

  def __init__(
    self,
    *,
    account_id="100",
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=None,
    positions=None,
    close_result=None,
    results_by_key=None,
    match_style="ticket",
    connected=True,
  ):
    self.name = "TEST"
    self.presenter = FakePresenter
    self._account_id = account_id
    self._market_type = market_type
    self._gateway_value = gateway_value
    self._connected = connected
    self._match_style = match_style
    self.executor = FakeExecutor(
      positions=positions,
      close_result=close_result,
      results_by_key=results_by_key,
      match_style=match_style,
    )
    self.db = FakeDbService(flat_positions=db_positions)
    self.notifications = []
    self._signals_blocked = False
    self.cycle = FakeCycleNotifier()
    self.ctx = SimpleNamespace(
      db_service=self.db,
      channel_notifier=SimpleNamespace(
        send_message=lambda m: self.notifications.append(m)
      ),
      cycle_notifier=self.cycle,
    )

  def _ensure_connected(self):
    return self._connected

  def _account_footer(self):
    return "FOOTER"

  def _current_footer(self):
    return "FOOTER"

  def _flat_match_key(self, pos):
    return pos.symbol if self._match_style == "symbol" else pos.ticket

  def _flat_db_match_keys(self, db_pos):
    if self._match_style == "symbol":
      return {self.executor.get_symbol(db_pos["symbol"])}
    return {db_pos.get("ref_id"), db_pos.get("ref_source_id")}

  _handle_admin_message = BaseSignalProcessor._handle_admin_message
  _handle_admin_flat = BaseSignalProcessor._handle_admin_flat
  _handle_signal_control = BaseSignalProcessor._handle_signal_control
  _set_signals_blocked = BaseSignalProcessor._set_signals_blocked
  _flat_targets_this_worker = BaseSignalProcessor._flat_targets_this_worker
  _close_live_positions_for_flat = BaseSignalProcessor._close_live_positions_for_flat
  _reconcile_flat_db = BaseSignalProcessor._reconcile_flat_db
  _log_flat_event = BaseSignalProcessor._log_flat_event

  _notify_cycle_or_send = BaseSignalProcessor._notify_cycle_or_send
  _private_admin_subject = BaseSignalProcessor._private_admin_subject


def _payload(**fields):
  data = {"action": "FLAT", "timestamp": "2026-06-02T08:00:00+00:00"}
  data.update(fields)
  return json.dumps(data)


# ── public ADMIN subject routing (market / gateway filters) ──────────────────
# The public ADMIN subject is fanned out to every worker. It carries NO
# account_id (account-scoped FLATs go on the private subject); a worker filters
# only on the optional market / gateway dimensions.


def test_public_no_filters_closes_all():
  proc = FakeProc(db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)])
  proc._handle_admin_message(_payload())
  assert len(proc.executor.closed) == 1


def test_public_account_id_field_is_ignored():
  # A stray account_id on the public subject is not a routing filter — it is
  # ignored, so a matching market/gateway still proceeds.
  proc = FakeProc(
    account_id="100",
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=[_db_pos(ref_id=1)],
    positions=[_pos(ticket=1)],
  )
  proc._handle_admin_message(_payload(account_id="999"))
  assert len(proc.executor.closed) == 1


def test_public_wrong_market_skips():
  proc = FakeProc(market_type="FOREX", db_positions=[_db_pos()])
  proc._handle_admin_message(_payload(market="CRYPTO"))
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_public_wrong_gateway_skips():
  proc = FakeProc(gateway_value="MT5", db_positions=[_db_pos()])
  proc._handle_admin_message(_payload(gateway="BINANCE"))
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_public_matching_market_gateway_proceeds():
  proc = FakeProc(
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=[_db_pos(ref_id=1)],
    positions=[_pos(ticket=1)],
  )
  proc._handle_admin_message(_payload(market="FOREX", gateway="MT5"))
  assert len(proc.executor.closed) == 1


# ── private ADMIN subject routing (market / gateway / account_id required) ────
# The private subject is ADMIN.<market>.<gateway>.<account_id>; the worker
# re-validates that all three fields match its own identity before acting.


def test_private_subject_is_market_gateway_account():
  proc = FakeProc(account_id="100", market_type="FOREX", gateway_value="MT5")
  assert proc._private_admin_subject == "ADMIN.FOREX.MT5.100"


def test_private_matching_identity_proceeds():
  proc = FakeProc(
    account_id="100",
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=[_db_pos(ref_id=1)],
    positions=[_pos(ticket=1)],
  )
  proc._handle_admin_message(
    _payload(account_id="100", market="FOREX", gateway="MT5"), private=True
  )
  assert len(proc.executor.closed) == 1


def test_private_wrong_account_id_skips():
  proc = FakeProc(
    account_id="100",
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=[_db_pos()],
  )
  proc._handle_admin_message(
    _payload(account_id="999", market="FOREX", gateway="MT5"), private=True
  )
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_private_missing_required_field_dropped():
  # market / gateway / account_id are mandatory on the private subject — a
  # payload missing any is rejected at validation and nothing is closed.
  proc = FakeProc(
    account_id="100",
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=[_db_pos()],
  )
  proc._handle_admin_message(
    _payload(market="FOREX", gateway="MT5"),
    private=True,  # no account_id
  )
  assert proc.db.flat_calls == []
  assert proc.executor.closed == []


def test_private_numeric_account_id_is_coerced_to_str():
  # Broker publishes a private ADMIN FLAT with ``account_id`` as an int
  # (e.g. MT5 login 413652379) — regression for a silently-dropped FLAT where
  # pydantic v2's non-coercing ``str`` field rejected the numeric payload,
  # logging a ValidationError but never closing the position. The private
  # schema now normalises int → str so the identity re-check compares equal
  # to the worker's stringified ``_account_id``.
  proc = FakeProc(
    account_id="413652379",
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=[_db_pos(ref_id=1)],
    positions=[_pos(ticket=1)],
  )
  raw = json.dumps(
    {
      "action": "FLAT",
      "timestamp": "2026-08-08T21:29:02+00:00",
      "strategy": None,
      "symbol": None,
      "account_id": 413652379,  # <-- int, not str
      "market": "FOREX",
      "gateway": "MT5",
    }
  )
  proc._handle_admin_message(raw, private=True)
  assert len(proc.executor.closed) == 1


def test_private_colliding_account_id_across_gateways_only_matches_owner():
  # Same raw account_id ("shared@example.com") used on two different gateways —
  # only the worker whose (market, gateway) also matches should act.
  mt5_proc = FakeProc(
    account_id="shared@example.com",
    market_type="FOREX",
    gateway_value="MT5",
    db_positions=[_db_pos(ref_id=1)],
    positions=[_pos(ticket=1)],
  )
  binance_proc = FakeProc(
    account_id="shared@example.com",
    market_type="CRYPTO",
    gateway_value="BINANCE",
    db_positions=[_db_pos(ref_id=1)],
    positions=[_pos(ticket=1)],
  )
  payload = _payload(
    account_id="shared@example.com",
    market="CRYPTO",
    gateway="BINANCE",
  )
  mt5_proc._handle_admin_message(payload, private=True)
  binance_proc._handle_admin_message(payload, private=True)
  assert mt5_proc.executor.closed == []
  assert len(binance_proc.executor.closed) == 1


# ── BLOCK_SIGNAL / ALLOW_SIGNAL (private subject only) ───────────────────────


def _ctrl_payload(action, **fields):
  data = {"action": action, "timestamp": "2026-06-02T08:00:00+00:00"}
  data.update(fields)
  return json.dumps(data)


def test_block_signal_sets_flag_and_notifies():
  proc = FakeProc(account_id="100", market_type="FOREX", gateway_value="MT5")
  proc._handle_admin_message(
    _ctrl_payload("BLOCK_SIGNAL", account_id="100", market="FOREX", gateway="MT5"),
    private=True,
  )
  assert proc._signals_blocked is True
  assert proc.notifications == ["Signals Blocked"]


def test_allow_signal_clears_flag_and_notifies():
  proc = FakeProc(account_id="100", market_type="FOREX", gateway_value="MT5")
  proc._signals_blocked = True
  proc._handle_admin_message(
    _ctrl_payload("ALLOW_SIGNAL", account_id="100", market="FOREX", gateway="MT5"),
    private=True,
  )
  assert proc._signals_blocked is False
  assert proc.notifications == ["Signals Allowed"]


def test_block_signal_repeat_is_noop_no_duplicate_notification():
  proc = FakeProc(account_id="100", market_type="FOREX", gateway_value="MT5")
  proc._signals_blocked = True
  proc._handle_admin_message(
    _ctrl_payload("BLOCK_SIGNAL", account_id="100", market="FOREX", gateway="MT5"),
    private=True,
  )
  assert proc._signals_blocked is True
  assert proc.notifications == []  # already blocked → no repeat alert


def test_block_signal_ignored_on_public_subject():
  proc = FakeProc(account_id="100", market_type="FOREX", gateway_value="MT5")
  proc._handle_admin_message(
    _ctrl_payload("BLOCK_SIGNAL", market="FOREX", gateway="MT5"), private=False
  )
  assert proc._signals_blocked is False  # signal control is private-only
  assert proc.notifications == []


def test_block_signal_wrong_account_id_skipped():
  proc = FakeProc(account_id="100", market_type="FOREX", gateway_value="MT5")
  proc._handle_admin_message(
    _ctrl_payload("BLOCK_SIGNAL", account_id="999", market="FOREX", gateway="MT5"),
    private=True,
  )
  assert proc._signals_blocked is False
  assert proc.notifications == []


def test_block_signal_missing_identity_dropped():
  # market / gateway / account_id are required on the private subject.
  proc = FakeProc(account_id="100", market_type="FOREX", gateway_value="MT5")
  proc._handle_admin_message(
    _ctrl_payload("BLOCK_SIGNAL", market="FOREX", gateway="MT5"),  # no account_id
    private=True,
  )
  assert proc._signals_blocked is False
  assert proc.notifications == []


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
  proc = FakeProc(
    db_positions=[_db_pos(ref_id=1, ref_source_id=1)], positions=[_pos(ticket=1)]
  )
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


# ── the stored entry signal survives a FLAT (so the broker can still match) ───
#
# Regression: the FLAT wrote its own ADMIN payload over the row's
# gateway_message, which is where the *entry signal* JSON lives. PositionCDC
# parses that column for the signal_id / sl / tp1 / tp2 the broker matches the
# TRADE event on, so the published update carried signal_id=null and the order
# was never updated on the broker side — on the private and the public
# (broadcast) subject alike. The payload now goes to the position_logs audit
# trail instead.


def test_flat_does_not_overwrite_the_stored_entry_signal():
  proc = FakeProc(
    db_positions=[_db_pos(ref_id=1, ref_source_id=1)], positions=[_pos(ticket=1)]
  )
  proc._handle_admin_message(_payload())
  assert "message" not in proc.db.status_updates[0]


def test_private_flat_does_not_overwrite_the_stored_entry_signal():
  proc = FakeProc(
    account_id="100",
    db_positions=[_db_pos(ref_id=1, ref_source_id=1)],
    positions=[_pos(ticket=1)],
  )
  proc._handle_admin_message(
    _payload(market="FOREX", gateway="MT5", account_id="100"), private=True
  )
  assert len(proc.db.status_updates) == 1
  assert "message" not in proc.db.status_updates[0]


def test_flat_payload_is_kept_in_the_audit_log():
  raw = _payload()
  proc = FakeProc(
    db_positions=[_db_pos(ref_id=1, ref_source_id=1)], positions=[_pos(ticket=1)]
  )
  proc._handle_admin_message(raw)
  assert len(proc.db.logs) == 1
  log = proc.db.logs[0]
  assert log["message"] == raw
  assert log["ref_source_id"] == 1
  assert log["action"] == PositionStatusEnum.FLATTED.value


def test_db_only_position_also_logs_the_flat_payload():
  raw = _payload()
  proc = FakeProc(db_positions=[_db_pos(ref_id=1, ref_source_id=1)], positions=[])
  proc._handle_admin_message(raw)
  assert [log["message"] for log in proc.db.logs] == [raw]


# ── Admin FLAT on a signal cycle ─────────────────────────────────────────── #
#
# A FLAT is the last action of the position's own trade, so it closes out that
# trade's message instead of arriving as an unrelated one. The cycle key comes
# off the position row — no signal is involved in an admin directive.


def test_admin_flat_closes_out_the_positions_cycle():
  proc = FakeProc(
    db_positions=[_db_pos(ref_id=1, signal_uxid="9f2c4b7e18a3d605")],
    positions=[_pos(ticket=1)],
  )
  proc._handle_admin_message(_payload())
  assert proc.notifications == []  # no standalone message
  (uxid, strategy, symbol, event, status) = proc.cycle.recorded[0]
  assert uxid == "9f2c4b7e18a3d605"
  assert (strategy, symbol) == ("strat-A", "XAUUSD")
  assert event.action == "FLAT"
  assert event.outcome == CycleOutcomeEnum.ADMIN_FLAT
  assert status is CycleStatusEnum.FLATTED


def test_admin_flat_on_a_position_with_no_cycle_still_notifies():
  # A position opened before signal_uxid existed has no cycle to join.
  proc = FakeProc(db_positions=[_db_pos(ref_id=1)], positions=[_pos(ticket=1)])
  proc._handle_admin_message(_payload())
  assert proc.cycle.recorded == []
  assert len(proc.notifications) == 1
  assert "Admin FLAT" in proc.notifications[0]
