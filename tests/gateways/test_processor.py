"""Tests for the Template-Method BaseSignalProcessor.

Verifies the base enforces the broker hooks (abstract methods) and that the
shared `_process_message` algorithm runs identically regardless of market.
"""

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from helpers import make_signal

from worker.gateways import processor as processor_module
from worker.gateways.config import ExecutionConfig
from worker.gateways.processor import BaseSignalProcessor, parse_strategy_subjects
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.signal_schema import SignalActionEnum
from worker.schemas.system_schema import SystemActionEnum
from worker.settings import MAX_RETRY_TIMEOUT


class FakePresenter:
  @staticmethod
  def startup(settings_dict, footer):
    return "startup"

  @staticmethod
  def shutdown(footer):
    return "shutdown"

  @staticmethod
  def force_closed(symbol, strategy, fc, footer):
    return f"force_closed:{symbol}"

  @staticmethod
  def order_filled(
    signal, result, pos_ticket, footer, risk_info=None, settings_dict=None
  ):
    return f"filled:{signal.action.value}:{pos_ticket}"

  @staticmethod
  def order_failed(signal, result, footer):
    return f"failed:{signal.action.value}"

  @staticmethod
  def order_rejected(signal, reason, footer):
    return f"order_rejected:{signal.action.value}:{reason}"

  @staticmethod
  def signal_rejected(reason, footer):
    return f"rejected:{reason}"

  @staticmethod
  def admin_flat_closed(db_pos, result, footer):
    return "admin_flat"

  @staticmethod
  def signals_blocked(footer):
    return "signals_blocked"

  @staticmethod
  def signals_allowed(footer):
    return "signals_allowed"


class FakeDb:
  def __init__(self):
    self.logged = []
    self.inserted = []
    self.updated = []
    self.rejected = []
    self.open_positions = []
    self.known_signal_ids = set()

  def log_position(self, **kw):
    self.logged.append(kw)

  def insert_position(self, **kw):
    self.inserted.append(kw)

  def insert_rejected_position(self, **kw):
    self.rejected.append(kw)

  def update_position_status(self, **kw):
    self.updated.append(kw)

  def get_open_positions_for_flat(self, strategy=None, symbol=None):
    return [
      p
      for p in self.open_positions
      if (strategy is None or p.get("strategy") == strategy)
      and (symbol is None or p.get("symbol") == symbol)
    ]

  def signal_exists(self, signal_id):
    return signal_id in self.known_signal_ids


class FakeProcessor(BaseSignalProcessor):
  """Concrete processor wired entirely with fakes (bypasses the real __init__)."""

  name = "FAKE"
  presenter = FakePresenter

  def __init__(self, handle_result, *, connected=True):
    # Intentionally skip super().__init__ — inject fakes directly so the shared
    # algorithm can be exercised without a broker/NATS/factory.
    self.db = FakeDb()
    self.notifications = []
    self.mgmt_notifications = []
    self.ctx = SimpleNamespace(
      db_service=self.db,
      channel_notifier=SimpleNamespace(
        send_message=lambda m: self.notifications.append(m)
      ),
      notifier=SimpleNamespace(
        send_message=lambda m: self.mgmt_notifications.append(m)
      ),
      direct_notifier=SimpleNamespace(send_message=lambda m: None),
    )
    self.handler = SimpleNamespace(handle=lambda sig: handle_result)
    self.settings = {}
    self.config = ExecutionConfig(
      volume_decision_enabled=True,
      capital=1000.0,
      risk_percentage=2.0,
      use_account_equity=False,
    )
    self._market_type = "FAKE_MKT"
    self._connected = connected
    self.admin_calls = []
    self.system_calls = []
    self.magic_calls = []

  # Hooks
  def _build_executor(self):  # pragma: no cover - unused (init bypassed)
    return None

  def _connect_broker(self):  # pragma: no cover
    return True

  def _disconnect_broker(self):  # pragma: no cover
    pass

  def _account_footer(self):
    return "FOOTER"

  @property
  def _account_id(self):
    return "ACC"

  def _magic_for(self, strategy):
    self.magic_calls.append(strategy)
    return 777

  def _position_cdc_kwargs(self):  # pragma: no cover
    return {}

  def _start_broker_jobs(self, stop_event):  # pragma: no cover
    pass

  def _ensure_connected(self):
    return self._connected

  def _flat_match_key(self, pos):  # pragma: no cover - admin routing overridden below
    return getattr(pos, "ticket", None)

  def _flat_db_match_keys(self, db_pos):  # pragma: no cover
    return {db_pos.get("ref_id"), db_pos.get("ref_source_id")}

  # Override the (now concrete) shared FLAT handler to assert routing only.
  def _handle_admin_message(self, raw, *, private=False):
    self.admin_calls.append((raw, private))

  # Record dispatched runtime SYSTEM actions (base parses the envelope + ensures
  # connection + routes by identity, then calls this hook).
  def _handle_system_action(self, action, data):
    self.system_calls.append((action, data))


# ── Abstract enforcement ──────────────────────────────────────────────────── #


def test_cannot_instantiate_base_directly():
  with pytest.raises(TypeError):
    BaseSignalProcessor(None, {})  # abstract methods unimplemented


def test_incomplete_subclass_is_abstract():
  class Incomplete(BaseSignalProcessor):
    name = "X"
    presenter = FakePresenter

    # Implement everything EXCEPT the FLAT match-key hooks.
    def _build_executor(self):
      return None

    def _connect_broker(self):
      return True

    def _disconnect_broker(self):
      pass

    def _account_footer(self):
      return ""

    @property
    def _account_id(self):
      return ""

    def _magic_for(self, strategy):
      return None

    def _position_cdc_kwargs(self):
      return {}

    def _start_broker_jobs(self, stop_event):
      pass

  with pytest.raises(TypeError):
    Incomplete(None, {})


# ── Shared _process_message ────────────────────────────────────────────────── #


def test_entry_signal_inserts_position_with_market_and_magic():
  result = {"success": True, "ticket": 555, "price": 30000.0, "volume": 0.02}
  proc = FakeProcessor(result)
  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )

  assert len(proc.db.inserted) == 1
  row = proc.db.inserted[0]
  assert row["strategy_code"] == 777  # came from the broker hook (_magic_for)
  assert row["market_type"] == "FAKE_MKT"
  assert row["action"] == "long"
  assert proc.notifications == ["filled:LONG:555"]
  # _magic_for is called twice: once by the unknown-strategy guard, once by
  # _persist_success when stamping the OPENED row's strategy_code.
  assert proc.magic_calls == ["strat-1", "strat-1"]


def test_exit_signal_updates_status():
  result = {
    "success": True,
    "ticket": 9,
    "source_ticket": 5,
    "price": 31000.0,
    "volume": 0.02,
  }
  proc = FakeProcessor(result)
  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.SL).model_dump_json()
  )

  assert proc.db.inserted == []
  assert len(proc.db.updated) == 1
  assert proc.db.updated[0]["ref_source_id"] == 5
  assert proc.notifications == ["filled:SL:5"]


def test_blocked_signals_are_skipped():
  # BLOCK_SIGNAL sets _signals_blocked; every incoming signal is then skipped
  # (no order, no DB write, no notification) until ALLOW_SIGNAL clears it.
  result = {"success": True, "ticket": 555, "price": 30000.0, "volume": 0.02}
  proc = FakeProcessor(result)
  proc._signals_blocked = True
  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )
  assert proc.db.inserted == []
  assert proc.db.logged == []
  assert proc.notifications == []


def test_signals_resume_after_unblock():
  result = {"success": True, "ticket": 555, "price": 30000.0, "volume": 0.02}
  proc = FakeProcessor(result)
  proc._signals_blocked = True
  proc._signals_blocked = False  # ALLOW_SIGNAL cleared the gate
  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )
  assert len(proc.db.inserted) == 1
  assert proc.notifications == ["filled:LONG:555"]


def test_zero_fill_price_falls_back_to_signal_price():
  # Binance testnet can return price=0 on a filled MARKET order (entries AND
  # closes such as FLAT). The processor must substitute signal.price so neither
  # the DB row nor the notification records a misleading 0.0.
  result = {
    "success": True,
    "ticket": 9,
    "source_ticket": 5,
    "price": 0.0,
    "volume": 0.02,
  }
  proc = FakeProcessor(result)
  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.FLAT, price=65000.0).model_dump_json(),
  )
  # Both the position log and the close-status update record the signal price.
  assert proc.db.logged[0]["price"] == 65000.0
  assert proc.db.updated[0]["closed_price"] == 65000.0


def test_zero_fill_price_with_no_signal_price_stays_zero():
  # No fallback available (signal carries no price) → leave the result untouched
  # rather than inventing a price.
  result = {
    "success": True,
    "ticket": 9,
    "source_ticket": 5,
    "price": 0.0,
    "volume": 0.02,
  }
  proc = FakeProcessor(result)
  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.FLAT, price=None).model_dump_json(),
  )
  assert proc.db.logged[0]["price"] == 0.0


def test_failed_result_sends_failure_notification():
  proc = FakeProcessor({"success": False, "retcode": -1, "comment": "boom"})
  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )
  assert proc.db.inserted == []
  assert proc.notifications == ["failed:LONG"]


# ── MAX_OPEN_ORDERS exposure guard ─────────────────────────────────────────── #


def _open_row(strategy, symbol):
  return {
    "strategy": strategy,
    "symbol": symbol,
    "status": "OPENED",
    "ref_source_id": f"{strategy}:{symbol}",
  }


def _capturing_handler(proc, result):
  """Replace the handler with one that records the signals it was asked to
  execute, so a test can assert the broker path was (not) reached."""
  seen = []
  proc.handler = SimpleNamespace(handle=lambda sig: seen.append(sig) or result)
  return seen


def test_entry_rejected_when_at_max_open_orders():
  proc = FakeProcessor({"success": True, "ticket": 1})
  proc.settings = {"max_open_orders": 2}
  proc.db.open_positions = [_open_row("s1", "AAA"), _open_row("s2", "BBB")]
  seen = _capturing_handler(proc, {"success": True})

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.LONG, symbol="CCC").model_dump_json(),
  )

  # Broker execution never runs for a rejected entry.
  assert seen == []
  # Not tracked as an OPENED position...
  assert proc.db.inserted == []
  # ...but recorded as a REJECTED row so PositionCDC forwards it to the broker.
  assert len(proc.db.rejected) == 1
  rej = proc.db.rejected[0]
  assert rej["symbol"] == "CCC"
  assert rej["action"] == "long"
  assert "Max open orders reached (2/2)" in rej["comment"]
  # Audit-logged and reported on the community channel.
  assert len(proc.db.logged) == 1
  assert proc.notifications == ["order_rejected:LONG:" + rej["comment"]]


def test_entry_allowed_when_below_max_open_orders():
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {"max_open_orders": 5}
  proc.db.open_positions = [_open_row("s1", "AAA")]

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.LONG, symbol="CCC").model_dump_json(),
  )

  # Normal entry path: inserted as OPENED, no REJECTED row, filled notification.
  assert proc.db.rejected == []
  assert len(proc.db.inserted) == 1
  assert proc.notifications == ["filled:LONG:1"]


def test_entry_rejected_when_symbol_already_open_same_strategy():
  # One-open-order-per-symbol: an entry on a symbol this same strategy already
  # holds is REJECTED (no re-entry/scale-in while an order is live on the symbol).
  proc = FakeProcessor({"success": True, "ticket": 1})
  proc.db.open_positions = [_open_row("strat-1", "XAUUSD")]
  seen = _capturing_handler(proc, {"success": True})

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(
      SignalActionEnum.LONG, symbol="XAUUSD", strategy="strat-1"
    ).model_dump_json(),
  )

  # Broker execution never runs; recorded as a REJECTED row + audit log + notice.
  assert seen == []
  assert proc.db.inserted == []
  assert len(proc.db.rejected) == 1
  rej = proc.db.rejected[0]
  assert rej["symbol"] == "XAUUSD"
  assert "open position on symbol" in rej["comment"]
  assert len(proc.db.logged) == 1
  assert proc.notifications == ["order_rejected:LONG:" + rej["comment"]]


def test_entry_rejected_when_symbol_open_under_other_strategy():
  # An order open on the symbol under a *different* strategy also blocks a new
  # entry (regardless of the MAX_OPEN_ORDERS cap, which is disabled here).
  proc = FakeProcessor({"success": True, "ticket": 1})
  proc.db.open_positions = [_open_row("strat-1", "XAUUSD")]
  seen = _capturing_handler(proc, {"success": True})

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(
      SignalActionEnum.LONG, symbol="XAUUSD", strategy="strat-2"
    ).model_dump_json(),
  )

  assert seen == []
  assert len(proc.db.rejected) == 1
  assert "strat-1" in proc.db.rejected[0]["comment"]


def test_entry_allowed_when_symbol_open_under_other_strategy_and_multi_allowed():
  # FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL: a different strategy's position on
  # the symbol no longer blocks — the market keeps them isolated via magic.
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.db.open_positions = [_open_row("strat-1", "XAUUSD")]
  seen = _capturing_handler(proc, {"success": True, "ticket": 1})
  proc.handler.strategy = SimpleNamespace(allows_multi_strategy_per_symbol=True)

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(
      SignalActionEnum.LONG, symbol="XAUUSD", strategy="strat-2"
    ).model_dump_json(),
  )

  assert len(seen) == 1
  assert proc.db.rejected == []


def test_entry_rejected_when_symbol_open_under_same_strategy_even_if_multi_allowed():
  # The toggle exempts *other* strategies only — this strategy's own open
  # position on the symbol still blocks a fresh entry.
  proc = FakeProcessor({"success": True, "ticket": 1})
  proc.db.open_positions = [_open_row("strat-1", "XAUUSD")]
  seen = _capturing_handler(proc, {"success": True})
  proc.handler.strategy = SimpleNamespace(allows_multi_strategy_per_symbol=True)

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(
      SignalActionEnum.LONG, symbol="XAUUSD", strategy="strat-1"
    ).model_dump_json(),
  )

  assert seen == []
  assert len(proc.db.rejected) == 1


def test_entry_allowed_when_symbol_has_no_open_order():
  # A different symbol with no open order proceeds normally even while another
  # symbol is held.
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.db.open_positions = [_open_row("strat-1", "XAUUSD")]

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.LONG, symbol="BTCUSDT").model_dump_json(),
  )

  assert proc.db.rejected == []
  assert len(proc.db.inserted) == 1
  assert proc.notifications == ["filled:LONG:1"]


def test_exit_signal_not_gated_by_max_open_orders():
  # An exit (SL) must still be processed when the worker is at its cap so a
  # position can always be closed.
  proc = FakeProcessor(
    {"success": True, "ticket": 9, "source_ticket": 5, "price": 1990.0, "volume": 0.1}
  )
  proc.settings = {"max_open_orders": 1}
  proc.db.open_positions = [_open_row("s1", "AAA"), _open_row("s2", "BBB")]

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.SL).model_dump_json()
  )

  assert proc.db.rejected == []
  assert len(proc.db.updated) == 1
  assert proc.notifications == ["filled:SL:5"]


def test_max_open_orders_zero_disables_cap():
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {"max_open_orders": 0}
  proc.db.open_positions = [_open_row("s1", "AAA"), _open_row("s2", "BBB")]

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.LONG, symbol="CCC").model_dump_json(),
  )

  assert proc.db.rejected == []
  assert len(proc.db.inserted) == 1


# ── Audit trail records the stop actually placed ───────────────────────────── #


def test_log_position_records_the_broker_side_stop():
  # The broker widened the stop to satisfy its minimum distance. The audit row
  # must hold what the position really carries, not what the signal asked for —
  # it is the record of the risk actually taken.
  proc = FakeProcessor(
    {
      "success": True,
      "ticket": 1,
      "price": 2000.0,
      "volume": 0.33,
      "sl": 1999.39,
      "tp": 2000.11,
    }
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.LONG, sl=1999.45, tp2=2000.05).model_dump_json(),
  )

  assert proc.db.logged[0]["sl"] == 1999.39


def test_log_position_falls_back_to_the_signal_stop():
  # Exits carry no stop of their own, so the signal's value still lands in the
  # audit row rather than a null.
  proc = FakeProcessor(
    {"success": True, "ticket": 9, "source_ticket": 5, "price": 1990.0, "volume": 0.1}
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.SL, sl=1990.0).model_dump_json(),
  )

  assert proc.db.logged[0]["sl"] == 1990.0


def test_log_position_keeps_tp1_as_the_signal_partial_target():
  # tp1 is the partial-close level, a different concept from the full-exit TP
  # resting at the broker — result["tp"] must not overwrite it.
  proc = FakeProcessor(
    {
      "success": True,
      "ticket": 1,
      "price": 2000.0,
      "volume": 0.1,
      "sl": 1990.0,
      "tp": 2050.0,
    }
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.LONG, tp1=2020.0, tp2=2050.0).model_dump_json(),
  )

  assert proc.db.logged[0]["tp1"] == 2020.0


# ── Stale-signal guard ─────────────────────────────────────────────────────── #


def _quoting_handler(proc, quote, result):
  """Give the processor a handler whose market strategy reports *quote* as the
  live entry price, and record the signals it was asked to execute."""
  seen = []
  proc.handler = SimpleNamespace(
    handle=lambda sig: seen.append(sig) or result,
    strategy=SimpleNamespace(entry_price=lambda sig: quote),
  )
  return seen


def test_entry_rejected_when_market_already_past_tp():
  # The XPDUSD failure: signal planned at 1375 with TP 1377, but the ask has
  # already run to 1388. The entry must never reach the broker.
  proc = FakeProcessor({"success": True, "ticket": 1})
  seen = _quoting_handler(proc, 1388.0, {"success": True})

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(
      SignalActionEnum.LONG, symbol="XPDUSD", price=1375.0, sl=1370.0, tp2=1377.0
    ).model_dump_json(),
  )

  assert seen == []
  assert proc.db.inserted == []
  assert len(proc.db.rejected) == 1
  rej = proc.db.rejected[0]
  assert rej["symbol"] == "XPDUSD"
  assert "already reached its take-profit" in rej["comment"]
  assert proc.notifications == ["order_rejected:LONG:" + rej["comment"]]


def test_entry_rejected_when_market_already_past_sl():
  proc = FakeProcessor({"success": True, "ticket": 1})
  seen = _quoting_handler(proc, 1985.0, {"success": True})

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )

  assert seen == []
  assert len(proc.db.rejected) == 1
  assert "already reached its stop-loss" in proc.db.rejected[0]["comment"]


def test_entry_rejected_when_drift_exceeds_limit():
  proc = FakeProcessor({"success": True, "ticket": 1})
  proc.settings = {"max_entry_drift_r_percent": 50}
  seen = _quoting_handler(proc, 2006.0, {"success": True})

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )

  assert seen == []
  assert len(proc.db.rejected) == 1
  assert "price moved too far" in proc.db.rejected[0]["comment"]


def test_entry_allowed_when_quote_is_close_to_the_signal():
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2002.0, "volume": 0.1})
  _quoting_handler(
    proc, 2002.0, {"success": True, "ticket": 1, "price": 2002.0, "volume": 0.1}
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )

  assert proc.db.rejected == []
  assert len(proc.db.inserted) == 1
  assert proc.notifications == ["filled:LONG:1"]


def test_exit_signal_not_gated_by_stale_guard():
  # An exit must always be executable, however far the market has run — a TP2
  # exit is *expected* to arrive with the price through the target.
  proc = FakeProcessor(
    {"success": True, "ticket": 9, "source_ticket": 5, "price": 2050.0, "volume": 0.1}
  )
  _quoting_handler(
    proc,
    2060.0,
    {
      "success": True,
      "ticket": 9,
      "source_ticket": 5,
      "price": 2050.0,
      "volume": 0.1,
    },
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.TP2).model_dump_json()
  )

  assert proc.db.rejected == []
  assert proc.notifications == ["filled:TP2:5"]


def test_entry_proceeds_when_no_live_quote_is_available():
  # A missing quote must not block trading — the guard simply has nothing to
  # judge on and defers to the broker.
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  _quoting_handler(
    proc, None, {"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1}
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )

  assert proc.db.rejected == []
  assert len(proc.db.inserted) == 1


def test_entry_proceeds_when_quote_lookup_raises():
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})

  def _boom(_sig):
    raise RuntimeError("broker unreachable")

  proc.handler = SimpleNamespace(
    handle=lambda sig: {
      "success": True,
      "ticket": 1,
      "price": 2000.0,
      "volume": 0.1,
    },
    strategy=SimpleNamespace(entry_price=_boom),
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )

  assert proc.db.rejected == []
  assert len(proc.db.inserted) == 1


def test_cheaper_db_guards_run_before_the_quote_lookup():
  # The staleness guard needs a broker round-trip, so an entry already rejected
  # by a DB-only guard must not pay for one.
  proc = FakeProcessor({"success": True, "ticket": 1})
  proc.db.open_positions = [_open_row("strat-1", "XAUUSD")]
  quote_calls = []

  def _quote(_sig):
    quote_calls.append(_sig)
    return 2000.0

  proc.handler = SimpleNamespace(
    handle=lambda sig: {"success": True},
    strategy=SimpleNamespace(entry_price=_quote),
  )

  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )

  assert quote_calls == []
  assert len(proc.db.rejected) == 1
  assert "already has an open order" in proc.db.rejected[0]["comment"]


def test_unknown_strategy_signal_is_skipped_with_alert():
  # A FOREX signal for a strategy that isn't in STRATEGY_MAGIC_MAP used to bubble
  # a KeyError out of the executor as an unhandled "manual reconciliation may be
  # required" error. It must now be skipped cleanly with an operator alert.
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  seen = _capturing_handler(proc, {"success": True})

  def _raise_key_error(strategy):
    proc.magic_calls.append(strategy)
    raise KeyError(f"Strategy '{strategy}' not found in strategy_magic_map.")

  proc._magic_for = _raise_key_error

  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.LONG, strategy="MT5_MULTI_M5_V1").model_dump_json(),
  )

  # Broker execution never runs; nothing recorded in the DB.
  assert seen == []
  assert proc.db.inserted == []
  assert proc.db.logged == []
  assert proc.db.rejected == []
  assert proc.notifications == []
  # Operator is alerted via the management notifier (routes to Telegram).
  assert len(proc.mgmt_notifications) == 1
  assert "MT5_MULTI_M5_V1" in proc.mgmt_notifications[0]


def test_admin_subject_routed_to_hook():
  proc = FakeProcessor({"success": True})
  proc._process_message(NatsSubjectEnum.ADMIN, '{"action":"FLAT"}')
  assert proc.admin_calls == [('{"action":"FLAT"}', False)]
  assert proc.db.logged == []  # not treated as a signal


def test_private_admin_subject_routed_to_hook():
  proc = FakeProcessor({"success": True})
  # Give the worker a full identity so it has a private ADMIN subject:
  # ADMIN.<market>.<gateway>.<account_id>.
  proc._gateway_setting_key = "gateway"
  proc.settings = {"gateway": "FAKE_GW"}
  subject = proc._private_admin_subject
  assert subject == "ADMIN.FAKE_MKT.FAKE_GW.ACC"
  proc._process_message(subject, '{"action":"FLAT"}')
  assert proc.admin_calls == [('{"action":"FLAT"}', True)]
  assert proc.db.logged == []  # not treated as a signal


# A runtime SYSTEM broadcast: the broker's leverage settings changed after this
# worker was already connected, so it cannot ride in the handshake ACK.
def _leverage_broadcast(account_id="FAKE_MKT-FAKE_GW-acct-1", **fields):
  import json as _json

  return _json.dumps(
    {
      "action": SystemActionEnum.CRYPTO_LEVERAGE_INIT.value,
      "timestamp": "2026-06-30T00:00:00+00:00",
      "account_id": account_id,
      **fields,
    }
  )


def test_system_subject_parsed_and_dispatched_to_action_hook():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1"}

  proc._process_message(
    NatsSubjectEnum.SYSTEM,
    _leverage_broadcast(symbols=["BTC", "ETH"], default_leverage=10),
  )

  assert len(proc.system_calls) == 1
  action, data = proc.system_calls[0]
  assert action == SystemActionEnum.CRYPTO_LEVERAGE_INIT
  assert data["symbols"] == ["BTC", "ETH"]
  assert proc.db.logged == []  # not treated as a signal


def test_system_matching_account_id_dispatched():
  proc = FakeProcessor({"success": True})
  # account_id is already "<market>-<gateway>-<id>" by the time Settings hands
  # it over (see Settings._validate_market_requirements) — processor compares as-is.
  proc.settings = {"account_id": "FAKE_MKT-acct-1"}

  proc._process_message(
    NatsSubjectEnum.SYSTEM, _leverage_broadcast("FAKE_MKT-acct-1", symbols=["BTC"])
  )

  assert len(proc.system_calls) == 1


def test_system_wrong_account_id_silently_skips():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-acct-1"}

  proc._process_message(
    NatsSubjectEnum.SYSTEM, _leverage_broadcast("FAKE_MKT-other", symbols=["BTC"])
  )

  assert proc.system_calls == []


def test_system_malformed_json_dropped_without_dispatch():
  proc = FakeProcessor({"success": True})
  proc._process_message(NatsSubjectEnum.SYSTEM, "not json {{")
  assert proc.system_calls == []


def test_system_invalid_envelope_dropped_without_dispatch():
  proc = FakeProcessor({"success": True})
  # Unknown action fails SystemActionEnum validation → dropped before dispatch.
  proc._process_message(
    NatsSubjectEnum.SYSTEM, '{"action":"NOPE","timestamp":"2026-06-30T00:00:00+00:00"}'
  )
  assert proc.system_calls == []


def test_worker_connected_payload_includes_identity_market_and_gateway():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1", "gw_key": "FAKE_GW"}
  proc._gateway_setting_key = "gw_key"
  import json as _json

  payload = _json.loads(proc._worker_connected_payload())
  assert payload["action"] == SystemActionEnum.WORKER_CONNECTED.value
  assert payload["account_id"] == "FAKE_MKT-FAKE_GW-acct-1"
  assert payload["market"] == "FAKE_MKT"  # FakeProcessor._market_type
  assert payload["gateway"] == "FAKE_GW"
  assert payload["timestamp"]  # auto-stamped


def test_worker_connected_payload_none_without_account_id():
  proc = FakeProcessor({"success": True})
  proc.settings = {}  # no account_id derived yet
  assert proc._worker_connected_payload() is None


# ── WORKER_CONNECTED handshake (request/reply) ─────────────────────────────── #


class FakePublisher:
  """Records each request() call; returns/raises the next queued response."""

  def __init__(self, responses):
    self.responses = list(responses)
    self.requests = []

  def request(self, subject, data, timeout=5.0):
    self.requests.append((subject, data, timeout))
    resp = self.responses.pop(0)
    if isinstance(resp, Exception):
      raise resp
    return resp


def _connected_proc():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1", "gw_key": "FAKE_GW"}
  proc._gateway_setting_key = "gw_key"
  proc.subscriber = None
  return proc


@pytest.fixture(autouse=True)
def _no_handshake_jitter(monkeypatch):
  """Pin the pre-request jitter to 0 so handshake tests are deterministic and
  don't pay a real 0-0.5s sleep."""
  monkeypatch.setattr(processor_module.random, "uniform", lambda a, b: 0)


def test_announce_worker_connected_ack_needs_no_dispatch():
  proc = _connected_proc()
  proc.publisher = FakePublisher(
    [
      '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
      '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}'
    ]
  )
  proc._announce_worker_connected()

  assert len(proc.publisher.requests) == 1
  subject, payload, timeout = proc.publisher.requests[0]
  assert subject == NatsSubjectEnum.SYSTEM
  import json as _json

  assert _json.loads(payload)["action"] == SystemActionEnum.WORKER_CONNECTED.value
  assert timeout == 5


def test_announce_worker_connected_error_reply_logged_without_retry():
  proc = _connected_proc()
  proc.publisher = FakePublisher(
    [
      '{"action":"WORKER_CONNECTED_ERROR","timestamp":"2026-06-30T00:00:00+00:00",'
      '"account_id":"FAKE_MKT-FAKE_GW-acct-1","reason":"missing settings"}'
    ]
  )
  proc._announce_worker_connected()

  # The broker responded (not a timeout) — a config problem on its side isn't
  # fixed by retrying, so the handshake attempt ends here, logged for an operator.
  assert len(proc.publisher.requests) == 1
  assert "strategy_magic_map" not in proc.settings  # nothing applied


def test_announce_worker_connected_retries_with_backoff_on_timeout(monkeypatch):
  sleeps = []
  monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
  proc = _connected_proc()
  proc.publisher = FakePublisher(
    [
      TimeoutError("no reply"),
      TimeoutError("no reply"),
      '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
      '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}',
    ]
  )
  proc._announce_worker_connected()

  assert len(proc.publisher.requests) == 3
  assert sleeps == [0, 5, 10]  # leading 0 = jitter (pinned), then backoff schedule


def test_announce_worker_connected_retries_indefinitely_until_success(monkeypatch):
  sleeps = []
  monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
  proc = _connected_proc()
  proc.publisher = FakePublisher(
    [
      TimeoutError("no reply"),
      TimeoutError("no reply"),
      TimeoutError("no reply"),
      TimeoutError("no reply"),
      '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
      '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}',
    ]
  )
  proc._announce_worker_connected()

  assert len(proc.publisher.requests) == 5
  # Leading 0 = jitter (pinned); backoff caps at its last configured value
  # rather than growing unbounded.
  assert sleeps == [0, 5, 10, 20, 20]


def test_announce_worker_connected_escalates_to_error_after_threshold(monkeypatch):
  monkeypatch.setattr(time, "sleep", lambda s: None)
  levels = []
  monkeypatch.setattr(
    processor_module.log, "warning", lambda *a, **k: levels.append("WARNING")
  )
  monkeypatch.setattr(
    processor_module.log, "error", lambda *a, **k: levels.append("ERROR")
  )
  proc = _connected_proc()
  proc.publisher = FakePublisher(
    [
      TimeoutError("no reply"),
      TimeoutError("no reply"),
      TimeoutError("no reply"),
      '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
      '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}',
    ]
  )
  proc._announce_worker_connected()

  # First two timeouts stay WARNING; from _HANDSHAKE_ALERT_THRESHOLD (3) onward
  # they escalate to ERROR so TelegramLogHandler forwards an operator alert.
  assert levels == ["WARNING", "WARNING", "ERROR"]


def test_system_not_connected_short_circuits():
  proc = FakeProcessor({"success": True}, connected=False)
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1"}

  proc._process_message(NatsSubjectEnum.SYSTEM, _leverage_broadcast())

  assert proc.system_calls == []


def test_invalid_signal_notifies_operator_and_skips_execution():
  """A signal that fails validation must not silently vanish: it never reaches
  the handler/DB, and the operator is told *which* field was wrong via the
  management channel (never the community channel or the raw payload)."""
  proc = FakeProcessor({"success": True})
  # Broker's nested-`position` format → `action` is missing at the top level.
  raw = (
    '{"strategy":"S","symbol":"X","timestamp":"2026-01-01T00:00:00",'
    '"position":{"action":"LONG"}}'
  )
  proc._process_message(NatsSubjectEnum.SIGNAL, raw)

  # Never executed / persisted.
  assert proc.db.logged == []
  assert proc.db.inserted == []
  # Nothing leaked to the community channel.
  assert proc.notifications == []
  # Operator alerted on the management channel, naming the offending field.
  assert len(proc.mgmt_notifications) == 1
  assert proc.mgmt_notifications[0].startswith("rejected:")
  assert "action" in proc.mgmt_notifications[0]


def test_malformed_json_signal_is_dropped_without_notification():
  """Non-JSON payloads are logged and dropped — no notification path is taken
  (only the ValidationError branch was wired for alerts)."""
  proc = FakeProcessor({"success": True})
  proc._process_message(NatsSubjectEnum.SIGNAL, "not-json{")
  assert proc.db.logged == []
  assert proc.mgmt_notifications == []
  assert proc.notifications == []


def test_not_connected_short_circuits():
  proc = FakeProcessor({"success": True}, connected=False)
  proc._process_message(
    NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json()
  )
  assert proc.db.logged == []
  assert proc.notifications == []


def test_malformed_json_ignored():
  proc = FakeProcessor({"success": True})
  proc._process_message(NatsSubjectEnum.SIGNAL, "not json {{")
  assert proc.db.logged == []


# ── Scale-in (averaging) adjustment ────────────────────────────────────────── #


def _capture_signal_processor():
  """A FakeProcessor whose handler records the (possibly scaled) signal it sees."""
  result = {"success": True, "ticket": 1, "price": 66000.0, "volume": 0.5}
  proc = FakeProcessor(result)
  seen = []
  proc.handler = SimpleNamespace(handle=lambda sig: seen.append(sig) or result)
  return proc, seen


def test_scale_position_signal_passed_to_handler_verbatim():
  # The broker already scaled SL/TP/quantity; the processor must NOT re-scale —
  # the handler sees the payload values unchanged.
  proc, seen = _capture_signal_processor()
  raw = make_signal(
    SignalActionEnum.LONG,
    quantity=0.5,
    sl=64000.0,
    tp1=65000.0,
    tp2=67000.0,
    is_scale_position=True,
    scaling={"tp": 1.1, "sl": 0.9, "quantity": 2.0},
  ).model_dump_json()
  proc._process_message(NatsSubjectEnum.SIGNAL, raw)

  assert len(seen) == 1
  sig = seen[0]
  assert sig.tp1 == 65000.0
  assert sig.tp2 == 67000.0
  assert sig.sl == 64000.0
  assert sig.quantity == 0.5
  # The scaling metadata is preserved for the executor's self-sizing path.
  assert sig.is_scale_position is True
  assert sig.scale_quantity_factor() == 2.0


def test_non_scale_position_signal_is_untouched():
  proc, seen = _capture_signal_processor()
  raw = make_signal(
    SignalActionEnum.LONG, tp1=65000.0, scaling={"tp": 1.1}
  ).model_dump_json()
  proc._process_message(NatsSubjectEnum.SIGNAL, raw)

  assert seen[0].tp1 == 65000.0  # scaling ignored without is_scale_position=True
  assert seen[0].scale_quantity_factor() == 1.0


# ── Strategy subject parsing / WORKER_CONNECTED strategies ─────────────────── #


def test_parse_strategy_subjects_keeps_only_strategy_names():
  # Control subjects (ADMIN/SYSTEM/SIGNAL/TRADE) are filtered out; blank and
  # duplicate entries are dropped; order of first occurrence is preserved.
  assert parse_strategy_subjects(
    "MT5_GOLD,ADMIN,MT5_FX, ,MT5_GOLD,SYSTEM,CRYPTO_ETH"
  ) == [
    "MT5_GOLD",
    "MT5_FX",
    "CRYPTO_ETH",
  ]


def test_parse_strategy_subjects_empty_string_returns_empty_list():
  assert parse_strategy_subjects("") == []
  assert parse_strategy_subjects("  ,  , ") == []


def test_worker_connected_payload_includes_strategies_from_settings():
  proc = FakeProcessor({"success": True})
  proc.settings = {
    "account_id": "FAKE_MKT-FAKE_GW-acct-1",
    "gw_key": "FAKE_GW",
    "nats_subjects": "MT5_GOLD,MT5_FX,ADMIN,SYSTEM",
  }
  proc._gateway_setting_key = "gw_key"

  import json as _json

  payload = _json.loads(proc._worker_connected_payload())
  # The subscribed strategies (nats_subjects minus control subjects) are shipped
  # so the broker knows which strategies' recent signals to include in a
  # replay in the ACK for this worker.
  assert payload["strategies"] == ["MT5_GOLD", "MT5_FX"]


def test_worker_connected_payload_strategies_defaults_to_empty_when_unset():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1", "gw_key": "FAKE_GW"}
  proc._gateway_setting_key = "gw_key"

  import json as _json

  payload = _json.loads(proc._worker_connected_payload())
  assert payload["strategies"] == []


# ── WORKER_CONNECTED_ACK payload ───────────────────────────────────────────── #
#
# Both connect-time config sections (magic map + signal replay) now ride inside
# the single ACK: a NATS request inbox resolves on the first message and
# unsubscribes, so anything the broker sent after it was silently dropped.


def _ack_payload(
  *,
  account_id="FAKE_MKT-FAKE_GW-acct-1",
  settings=None,
  strategy_magic_map=None,
  crypto_leverage_init=None,
  retry_signals=None,
):
  """Serialise a WORKER_CONNECTED_ACK. A section left as ``None`` is omitted from
  the JSON entirely (the broker has nothing to push for it)."""
  import json as _json

  payload = {
    "action": SystemActionEnum.WORKER_CONNECTED_ACK.value,
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "account_id": account_id,
  }
  if settings is not None:
    payload["settings"] = settings
  if strategy_magic_map is not None:
    payload["strategy_magic_map"] = strategy_magic_map
  if crypto_leverage_init is not None:
    payload["crypto_leverage_init"] = crypto_leverage_init
  if retry_signals is not None:
    payload["retry_signals"] = [s.model_dump(mode="json") for s in retry_signals]
  return _json.dumps(payload)


def _fresh_signal(action=SignalActionEnum.LONG, **overrides):
  """A signal whose ``timestamp`` is well within MAX_RETRY_TIMEOUT so it is
  eligible for replay unless the test overrides ``timestamp`` explicitly."""
  overrides.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
  return make_signal(action, **overrides)


# ── ACK: retry_signals replay ──────────────────────────────────────────────── #


def _retry_proc():
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1", "gw_key": "FAKE_GW"}
  proc._gateway_setting_key = "gw_key"
  proc.subscriber = None
  return proc


def _replay(proc, signals):
  """Deliver *signals* the way the broker does — as the ACK's replay section."""
  proc.publisher = FakePublisher([_ack_payload(retry_signals=signals)])
  proc._announce_worker_connected()


def test_retry_signals_executes_fresh_signal_via_process_signal():
  proc = _retry_proc()
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="sig-fresh", symbol="AAA")

  _replay(proc, [sig])

  # The eligible signal went through the normal pipeline: it was persisted as
  # an OPENED position and a fill notification was sent to the channel.
  assert len(proc.db.inserted) == 1
  assert proc.db.inserted[0]["symbol"] == "AAA"
  assert proc.notifications == ["filled:LONG:1"]


def test_retry_signals_skips_signal_already_processed():
  proc = _retry_proc()
  proc.db.known_signal_ids.add("dup-1")
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="dup-1", symbol="AAA")

  _replay(proc, [sig])

  # A signal we've already processed must not be re-executed — no DB writes,
  # no channel notification.
  assert proc.db.inserted == []
  assert proc.db.logged == []
  assert proc.notifications == []


def test_retry_signals_skips_signal_older_than_max_retry_timeout():
  proc = _retry_proc()
  # Stamp the signal well past MAX_RETRY_TIMEOUT so the age-gate drops it.
  stale_ts = datetime.now(timezone.utc) - timedelta(seconds=MAX_RETRY_TIMEOUT + 30)
  sig = _fresh_signal(
    SignalActionEnum.LONG,
    signal_id="stale-1",
    symbol="AAA",
    timestamp=stale_ts.isoformat(),
  )

  _replay(proc, [sig])

  assert proc.db.inserted == []
  assert proc.notifications == []


def test_retry_signals_mixed_batch_only_executes_eligible():
  proc = _retry_proc()
  proc.db.known_signal_ids.add("dup")
  fresh = _fresh_signal(SignalActionEnum.LONG, signal_id="fresh", symbol="AAA")
  duplicate = _fresh_signal(SignalActionEnum.LONG, signal_id="dup", symbol="BBB")
  stale = _fresh_signal(
    SignalActionEnum.LONG,
    signal_id="stale",
    symbol="CCC",
    timestamp=(
      datetime.now(timezone.utc) - timedelta(seconds=MAX_RETRY_TIMEOUT + 5)
    ).isoformat(),
  )

  _replay(proc, [fresh, duplicate, stale])

  # Only the fresh (non-dup, in-window) signal reaches the broker path.
  assert [row["symbol"] for row in proc.db.inserted] == ["AAA"]
  assert proc.notifications == ["filled:LONG:1"]


def test_retry_signals_empty_batch_is_a_no_op():
  proc = _retry_proc()

  _replay(proc, [])

  assert proc.db.inserted == []
  assert proc.notifications == []


def test_ack_without_retry_signals_section_is_a_no_op():
  """An ACK that omits ``retry_signals`` entirely (the common case — nothing to
  catch up on) completes the handshake without touching the signal pipeline."""
  proc = _retry_proc()
  proc.publisher = FakePublisher([_ack_payload()])

  proc._announce_worker_connected()

  assert proc.db.inserted == []
  assert proc.notifications == []


def test_retry_signals_marks_executed_signal_id_in_db():
  """The processed signal_id is stored in position_logs + positions so a
  subsequent replay carrying the same id dedups against it."""
  proc = _retry_proc()
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="sig-persist", symbol="AAA")

  _replay(proc, [sig])

  assert proc.db.logged[0]["signal_id"] == "sig-persist"
  assert proc.db.inserted[0]["signal_id"] == "sig-persist"


def test_retry_signals_handles_batch_of_ten_mixed_signals():
  """A realistic-size replay batch (10 signals) mixes executable, duplicate,
  stale, and broker-failure entries. The whole batch must be drained in one
  pass — no early abort, no cross-signal interference — with each signal
  landing in the correct bucket."""
  proc = _retry_proc()
  proc.db.known_signal_ids.update({"dup-1", "dup-2"})

  fresh = [
    _fresh_signal(SignalActionEnum.LONG, signal_id=f"fresh-{i}", symbol=f"SYM{i}")
    for i in range(5)
  ]
  duplicates = [
    _fresh_signal(SignalActionEnum.LONG, signal_id="dup-1", symbol="DUPA"),
    _fresh_signal(SignalActionEnum.LONG, signal_id="dup-2", symbol="DUPB"),
  ]
  stale_ts = (
    datetime.now(timezone.utc) - timedelta(seconds=MAX_RETRY_TIMEOUT + 30)
  ).isoformat()
  stale = [
    _fresh_signal(
      SignalActionEnum.LONG, signal_id="stale-1", symbol="STA", timestamp=stale_ts
    ),
    _fresh_signal(
      SignalActionEnum.LONG, signal_id="stale-2", symbol="STB", timestamp=stale_ts
    ),
  ]
  failing = _fresh_signal(SignalActionEnum.LONG, signal_id="boom", symbol="BOOM")

  signals = fresh + duplicates + stale + [failing]
  assert len(signals) == 10

  # Every signal shares the same fake handler; make only "boom" raise so the
  # rest of the batch proves it isn't affected by one broker-side failure.
  def handle(sig):
    if sig.signal_id == "boom":
      raise RuntimeError("broker unreachable")
    return {"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1}

  proc.handler = SimpleNamespace(handle=handle)

  _replay(proc, signals)

  # The 5 fresh, eligible entries all executed and were persisted + notified.
  assert sorted(row["symbol"] for row in proc.db.inserted) == [
    f"SYM{i}" for i in range(5)
  ]
  assert len(proc.notifications) == 5
  # Duplicates, stale entries, and the failing signal never reach persistence.
  assert not any(
    row["symbol"] in ("DUPA", "DUPB", "STA", "STB", "BOOM") for row in proc.db.inserted
  )


# ── ACK: strategy_magic_map ────────────────────────────────────────────────── #


def _magic_map_proc(nats_subjects="MT5_GOLD,MT5_FX,ADMIN,SYSTEM"):
  """A connected FakeProcessor that records every executor-refresh call."""
  proc = FakeProcessor({"success": True})
  proc.settings = {
    "account_id": "FAKE_MKT-FAKE_GW-acct-1",
    "gw_key": "FAKE_GW",
    "nats_subjects": nats_subjects,
  }
  proc._gateway_setting_key = "gw_key"
  proc.subscriber = None
  proc.executor_magic_maps = []
  proc._set_executor_magic_map = lambda mapping: proc.executor_magic_maps.append(
    mapping
  )
  return proc


def _push_magic_map(proc, mapping):
  """Deliver *mapping* the way the broker does — as the ACK's magic-map section."""
  proc.publisher = FakePublisher([_ack_payload(strategy_magic_map=mapping)])
  proc._announce_worker_connected()


def test_strategy_magic_map_stored_in_settings_and_propagated_to_executor():
  # A subscribed-strategy map is stored under the same settings key the legacy
  # .env value used, and pushed to the already-built executor.
  proc = _magic_map_proc()

  _push_magic_map(proc, {"MT5_GOLD": 20260409, "MT5_FX": 20260410})

  assert proc.settings["strategy_magic_map"] == {
    "MT5_GOLD": 20260409,
    "MT5_FX": 20260410,
  }
  assert proc.executor_magic_maps == [{"MT5_GOLD": 20260409, "MT5_FX": 20260410}]


def test_strategy_magic_map_filters_to_subscribed_strategies():
  # Only entries for strategies in NATS_SUBJECTS are kept — a magic for a
  # strategy this worker does not subscribe to is ignored, so both settings and
  # the executor see just the subscribed subset.
  proc = _magic_map_proc(nats_subjects="MT5_GOLD,ADMIN,SYSTEM")

  _push_magic_map(proc, {"MT5_GOLD": 20260409, "MT5_OTHER": 999})

  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}
  assert proc.executor_magic_maps == [{"MT5_GOLD": 20260409}]


def test_strategy_magic_map_empty_clears():
  # An explicit empty map clears the worker's magics (still propagated to the
  # executor) — distinct from omitting the section, which leaves them untouched.
  proc = _magic_map_proc()

  _push_magic_map(proc, {})

  assert proc.settings["strategy_magic_map"] == {}
  assert proc.executor_magic_maps == [{}]


def test_ack_without_magic_map_section_leaves_existing_map_untouched():
  """Omitting ``strategy_magic_map`` means "nothing to push", not "clear it" —
  a reconnect ACK that arrives without the section must not wipe the magics the
  worker is already trading under."""
  proc = _magic_map_proc()
  proc.settings["strategy_magic_map"] = {"MT5_GOLD": 20260409}
  proc.publisher = FakePublisher([_ack_payload()])

  proc._announce_worker_connected()

  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}
  assert proc.executor_magic_maps == []


def test_strategy_magic_map_unparseable_leaves_settings_untouched():
  # Nothing salvageable in the push → the section is skipped entirely rather
  # than stored as an empty map, so settings and the executor stay untouched
  # (and the handshake never crashes).
  proc = _magic_map_proc()

  _push_magic_map(proc, {"MT5_GOLD": "not-an-int"})

  assert "strategy_magic_map" not in proc.settings
  assert proc.executor_magic_maps == []


# ── ACK: both sections in one payload ──────────────────────────────────────── #


def test_ack_applies_every_config_section_before_replaying_signals():
  """Ordering is load-bearing: a replayed FOREX entry is routed by its strategy's
  magic and a replayed CRYPTO entry is sized against the exchange's leverage, so
  both config sections must land before the replay runs — otherwise the replay
  fires orders against config that hasn't been applied yet."""
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {
    "account_id": "FAKE_MKT-FAKE_GW-acct-1",
    "gw_key": "FAKE_GW",
    "nats_subjects": "MT5_GOLD,ADMIN,SYSTEM",
  }
  proc._gateway_setting_key = "gw_key"
  proc.subscriber = None

  order = []
  proc._set_executor_magic_map = lambda mapping: order.append(("magic_map", mapping))
  proc._apply_market_init = lambda ack: order.append(
    ("market_init", ack.crypto_leverage_init.default_leverage)
  )
  original_process_signal = proc._process_signal

  def tracking_process_signal(signal):
    order.append(("signal", signal.signal_id))
    return original_process_signal(signal)

  proc._process_signal = tracking_process_signal

  sig = _fresh_signal(
    SignalActionEnum.LONG, signal_id="ack-all", symbol="AAA", strategy="MT5_GOLD"
  )
  proc.publisher = FakePublisher(
    [
      _ack_payload(
        strategy_magic_map={"MT5_GOLD": 20260409},
        crypto_leverage_init={"symbols": ["BTC"], "default_leverage": 10},
        retry_signals=[sig],
      )
    ]
  )

  proc._announce_worker_connected()

  assert order == [
    ("magic_map", {"MT5_GOLD": 20260409}),
    ("market_init", 10),
    ("signal", "ack-all"),
  ]
  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}
  assert proc.db.inserted[0]["symbol"] == "AAA"


# ── ACK: the settings section restores runtime toggles ─────────────────────── #


def _settings_proc():
  """A connected FakeProcessor with the signal gate at its post-restart default
  (unblocked) — the state a worker always boots into, since the toggles the ADMIN
  actions flip live in memory only."""
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1", "gw_key": "FAKE_GW"}
  proc._gateway_setting_key = "gw_key"
  proc.subscriber = None
  proc._signals_blocked = False
  return proc


def test_ack_settings_restore_the_signal_block_set_before_the_restart():
  """A worker restarting under an ADMIN BLOCK_SIGNAL comes back unblocked — the
  gate is in-memory only. The broker owns that state and re-pushes it in the ACK,
  so the worker must land back in the blocked state it was left in rather than
  quietly resuming trading on an account an operator had stopped."""
  proc = _settings_proc()
  proc.publisher = FakePublisher([_ack_payload(settings={"signal_blocked": True})])

  proc._announce_worker_connected()

  assert proc._signals_blocked is True
  # Restoring goes through the ADMIN setter, so the operator gets the same alert
  # they would have for a live BLOCK_SIGNAL.
  assert proc.notifications == ["signals_blocked"]


def test_ack_settings_signal_blocked_false_leaves_a_fresh_worker_alone():
  """``false`` matches the state a fresh worker already boots into, so the shared
  ADMIN setter treats it as the no-op it is: no flip, no duplicate alert."""
  proc = _settings_proc()
  proc.publisher = FakePublisher([_ack_payload(settings={"signal_blocked": False})])

  proc._announce_worker_connected()

  assert proc._signals_blocked is False
  assert proc.notifications == []


def test_ack_settings_signal_blocked_false_clears_a_block_still_in_effect():
  """A reconnect (not a restart) keeps the in-memory gate, so an explicit
  ``false`` from the broker is a real change and must lift the block — same path,
  same alert, as an ADMIN ALLOW_SIGNAL."""
  proc = _settings_proc()
  proc._signals_blocked = True
  proc.publisher = FakePublisher([_ack_payload(settings={"signal_blocked": False})])

  proc._announce_worker_connected()

  assert proc._signals_blocked is False
  assert proc.notifications == ["signals_allowed"]


def test_ack_without_settings_section_leaves_the_toggles_untouched():
  """Omitting the section says nothing about the toggles — a worker mid-session
  keeps whatever ADMIN last set rather than being reset to the defaults."""
  proc = _settings_proc()
  proc._signals_blocked = True
  proc.publisher = FakePublisher([_ack_payload(strategy_magic_map={})])

  proc._announce_worker_connected()

  assert proc._signals_blocked is True
  assert proc.notifications == []


def test_ack_settings_restore_the_block_before_the_replay_runs():
  """The replay shares ``_process_signal`` with live traffic, so a restored block
  has to land first: applying it afterwards would let the batch fire the very
  orders the block exists to prevent."""
  proc = _settings_proc()
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="blocked-replay", symbol="AAA")
  proc.publisher = FakePublisher(
    [_ack_payload(settings={"signal_blocked": True}, retry_signals=[sig])]
  )

  proc._announce_worker_connected()

  assert proc._signals_blocked is True
  assert proc.db.inserted == []
  assert proc.notifications == ["signals_blocked"]


def test_ack_unparseable_setting_does_not_cost_the_other_sections():
  """Settings are salvaged like every other section — a malformed value is
  dropped on its own while the rest of the payload still applies."""
  proc = _magic_map_proc()
  proc._signals_blocked = False
  proc.publisher = FakePublisher(
    [
      _ack_payload(
        settings={"signal_blocked": "not-a-bool"},
        strategy_magic_map={"MT5_GOLD": 20260409},
      )
    ]
  )

  proc._announce_worker_connected()

  assert proc._signals_blocked is False
  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}


def test_ack_unknown_setting_is_reported_but_costs_nothing():
  """A newer broker pushing a toggle this worker has no field for keeps the known
  ones working; the unknown key is recorded so the version skew is visible."""
  proc = _settings_proc()
  proc.publisher = FakePublisher(
    [_ack_payload(settings={"signal_blocked": True, "future_toggle": 1})]
  )

  proc._announce_worker_connected()

  assert proc._signals_blocked is True
  from worker.schemas.inbox_schema import WorkerConnectedAckSchema

  ack = WorkerConnectedAckSchema(
    **{
      "action": SystemActionEnum.WORKER_CONNECTED_ACK.value,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "account_id": "FAKE_MKT-FAKE_GW-acct-1",
      "settings": {"signal_blocked": True, "future_toggle": 1},
    }
  )
  assert ack.settings.signal_blocked is True
  assert any("future_toggle" in reason for reason in ack.dropped)


# ── ACK: the replay is gated on the config it depends on ───────────────────── #


def _gated_proc(blocked_reason):
  """A processor whose market blocks (or allows) the replay — standing in for
  ForexSignalProcessor._replay_blocked_reason without the MT5 stack."""
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {
    "account_id": "FAKE_MKT-FAKE_GW-acct-1",
    "gw_key": "FAKE_GW",
    "nats_subjects": "MT5_GOLD,ADMIN,SYSTEM",
  }
  proc._gateway_setting_key = "gw_key"
  proc.subscriber = None
  proc._replay_blocked_reason = lambda: blocked_reason
  return proc


def test_replay_skipped_when_the_market_has_no_usable_config():
  """Ordering alone doesn't guarantee the magic map *landed*: an ACK that omits it
  (or whose map was dropped by the salvage pass) leaves a first-connect FOREX
  worker with an empty map, and every replayed entry would then be rejected as an
  unknown strategy. Skip the batch instead, so the operator gets one line naming
  the root cause."""
  proc = _gated_proc("no strategy_magic_map")
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="gated", symbol="AAA")

  proc.publisher = FakePublisher([_ack_payload(retry_signals=[sig])])
  proc._announce_worker_connected()

  assert proc.db.inserted == []  # nothing executed
  assert proc.notifications == []  # and no per-signal rejection spam


def test_replay_runs_once_the_market_reports_its_config_is_ready():
  # Same ACK, nothing blocking → the batch replays as usual.
  proc = _gated_proc(None)
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="ungated", symbol="AAA")

  proc.publisher = FakePublisher([_ack_payload(retry_signals=[sig])])
  proc._announce_worker_connected()

  assert [p["symbol"] for p in proc.db.inserted] == ["AAA"]


def test_replay_gate_is_checked_after_every_config_section_is_applied():
  """The gate asks "did the config land?", so it must run *after* the sections —
  checking it earlier would block a replay whose magic map is in the very same
  ACK."""
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {
    "account_id": "FAKE_MKT-FAKE_GW-acct-1",
    "gw_key": "FAKE_GW",
    "nats_subjects": "MT5_GOLD,ADMIN,SYSTEM",
  }
  proc._gateway_setting_key = "gw_key"
  proc.subscriber = None
  # Mirrors ForexSignalProcessor._replay_blocked_reason: ready once the map is in
  # settings — which only the ACK's own magic-map section puts there.
  proc._replay_blocked_reason = lambda: (
    None if proc.settings.get("strategy_magic_map") else "no strategy_magic_map"
  )

  sig = _fresh_signal(
    SignalActionEnum.LONG, signal_id="same-ack", symbol="AAA", strategy="MT5_GOLD"
  )
  proc.publisher = FakePublisher(
    [_ack_payload(strategy_magic_map={"MT5_GOLD": 20260409}, retry_signals=[sig])]
  )

  proc._announce_worker_connected()

  assert [p["symbol"] for p in proc.db.inserted] == ["AAA"]


def test_worker_connected_reply_payload_logged_before_parsing(monkeypatch):
  """A handshake reply lands on the request's private inbox, so it never passes
  through the subscriber's "Received NATS message" DEBUG line. Log the raw bytes
  here instead — and *before* parsing, so a payload that doesn't parse at all is
  still visible rather than leaving only an exception."""
  debug_lines = []
  monkeypatch.setattr(
    processor_module.log,
    "debug",
    lambda msg, *a, **k: debug_lines.append(msg % a if a else msg),
  )
  proc = _connected_proc()
  proc.publisher = FakePublisher(["not-json{"])

  proc._announce_worker_connected()

  assert any("reply payload: not-json{" in line for line in debug_lines)


def test_worker_connected_request_payload_logged_before_sending(monkeypatch):
  """The outbound handshake rides ``publisher.request``, not ``publisher.publish``,
  so it never passes through the subscriber's "Received NATS message" DEBUG line
  either. Log the raw JSON here so ``market``/``gateway``/``strategies`` are
  visible without inferring them from the account_id-only INFO summary."""
  debug_lines = []
  monkeypatch.setattr(
    processor_module.log,
    "debug",
    lambda msg, *a, **k: debug_lines.append(msg % a if a else msg),
  )
  proc = _connected_proc()
  proc.publisher = FakePublisher([_ack_payload()])

  proc._announce_worker_connected()

  assert any(
    "request payload: " in line and '"action":"WORKER_CONNECTED"' in line
    for line in debug_lines
  )


def test_ack_crypto_leverage_init_section_reaches_the_market_hook():
  """The leverage-init pass is now an ACK section rather than a standalone SYSTEM
  action; the base parses it and hands the typed section to the market hook."""
  proc = _connected_proc()
  seen = []
  proc._apply_market_init = lambda ack: seen.append(ack.crypto_leverage_init)
  proc.publisher = FakePublisher(
    [_ack_payload(crypto_leverage_init={"symbols": ["BTC"], "default_leverage": 10})]
  )

  proc._announce_worker_connected()

  assert len(seen) == 1
  assert seen[0].symbols == ["BTC"]
  assert seen[0].default_leverage == 10


def test_ack_without_leverage_section_hands_the_hook_nothing():
  """Omitting the section skips the leverage pass — it is the only trigger, so a
  worker that wasn't asked must not re-initialise anything on its own."""
  proc = _connected_proc()
  seen = []
  proc._apply_market_init = lambda ack: seen.append(ack.crypto_leverage_init)
  proc.publisher = FakePublisher([_ack_payload(strategy_magic_map={})])

  proc._announce_worker_connected()

  assert seen == [None]


def test_ack_invalid_leverage_section_does_not_cost_the_other_sections():
  """Sections are salvaged independently — a malformed leverage block is dropped
  on its own while the magic map in the same payload still applies."""
  proc = _magic_map_proc()
  seen = []
  proc._apply_market_init = lambda ack: seen.append(ack.crypto_leverage_init)
  proc.publisher = FakePublisher(
    [
      _ack_payload(
        strategy_magic_map={"MT5_GOLD": 20260409},
        crypto_leverage_init={"default_leverage": "not-an-int"},
      )
    ]
  )

  proc._announce_worker_connected()

  assert seen == [None]
  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}


def test_ack_invalid_signal_does_not_cost_the_magic_map():
  """Sections are salvaged independently: a malformed signal in the replay must
  not stop the magic map from landing. Without the map a FOREX worker rejects
  *every* signal as an unknown strategy, so letting one bad replay entry take it
  down would turn a cosmetic broker bug into a full trading outage."""
  proc = _magic_map_proc()
  import json as _json

  raw = _json.dumps(
    {
      "action": SystemActionEnum.WORKER_CONNECTED_ACK.value,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "account_id": "FAKE_MKT-FAKE_GW-acct-1",
      "strategy_magic_map": {"MT5_GOLD": 20260409},
      "retry_signals": [
        {"strategy": "MT5_GOLD", "symbol": "AAA"}
      ],  # no action/timestamp
    }
  )
  proc.publisher = FakePublisher([raw])

  proc._announce_worker_connected()

  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}
  assert proc.executor_magic_maps == [{"MT5_GOLD": 20260409}]
  # The unparseable signal itself is still dropped — never executed.
  assert proc.db.inserted == []


def test_ack_drops_only_the_bad_signal_and_replays_the_rest():
  """One malformed entry must not cancel the gap-fill the rest of the batch
  provides — each signal is validated on its own."""
  proc = _retry_proc()
  good = _fresh_signal(SignalActionEnum.LONG, signal_id="good", symbol="AAA")
  import json as _json

  raw = _json.dumps(
    {
      "action": SystemActionEnum.WORKER_CONNECTED_ACK.value,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "account_id": "FAKE_MKT-FAKE_GW-acct-1",
      "retry_signals": [
        {"strategy": "S", "symbol": "BAD"},  # no action/timestamp
        good.model_dump(mode="json"),
      ],
    }
  )
  proc.publisher = FakePublisher([raw])

  proc._announce_worker_connected()

  assert [row["symbol"] for row in proc.db.inserted] == ["AAA"]
  assert proc.notifications == ["filled:LONG:1"]


def test_ack_magic_map_keeps_the_parseable_entries():
  """A corrupt magic for one strategy costs that strategy only. Its own signals
  are then rejected loudly by the unknown-strategy guard, which is a far better
  failure than every strategy losing its magic at once."""
  proc = _magic_map_proc(nats_subjects="MT5_GOLD,MT5_FX,ADMIN,SYSTEM")

  _push_magic_map(proc, {"MT5_GOLD": 20260409, "MT5_FX": "not-an-int"})

  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}
  assert proc.executor_magic_maps == [{"MT5_GOLD": 20260409}]


def test_ack_wholly_unparseable_magic_map_is_not_read_as_a_clear():
  """Salvaging nothing from a non-empty push leaves the current map alone rather
  than storing ``{}`` — an empty map is the broker's explicit "clear my magics"
  instruction, and a parse failure must never be mistaken for one."""
  proc = _magic_map_proc()
  proc.settings["strategy_magic_map"] = {"MT5_GOLD": 20260409}

  _push_magic_map(proc, {"MT5_GOLD": "not-an-int"})

  assert proc.settings["strategy_magic_map"] == {"MT5_GOLD": 20260409}
  assert proc.executor_magic_maps == []


def test_ack_envelope_error_still_drops_everything():
  """Section salvage does not extend to the envelope: an ACK with no account_id
  addresses nobody, so there is nothing to apply it to."""
  proc = _magic_map_proc()
  import json as _json

  raw = _json.dumps(
    {
      "action": SystemActionEnum.WORKER_CONNECTED_ACK.value,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "strategy_magic_map": {"MT5_GOLD": 20260409},
    }
  )
  proc.publisher = FakePublisher([raw])

  proc._announce_worker_connected()

  assert "strategy_magic_map" not in proc.settings
  assert proc.executor_magic_maps == []


@pytest.mark.parametrize("action", ["RETRY_SIGNALS", "STRATEGY_MAGIC_MAP"])
def test_removed_system_actions_are_no_longer_recognised(action):
  """Neither survives as a standalone SYSTEM action — both are connect-time only
  and the broker delivers them as sections of the ACK. A stale broker still
  publishing them fails envelope validation and is ignored, never executed.
  (``CRYPTO_LEVERAGE_INIT`` deliberately remains: it is the one piece of config
  that also changes at runtime, so it needs the always-live SYSTEM broadcast.)"""
  assert not hasattr(SystemActionEnum, action)

  proc = _magic_map_proc()
  import json as _json

  raw = _json.dumps(
    {
      "action": action,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "account_id": "FAKE_MKT-FAKE_GW-acct-1",
      "strategy_magic_map": {"MT5_GOLD": 20260409},
    }
  )
  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

  assert "strategy_magic_map" not in proc.settings
  assert proc.executor_magic_maps == []


def test_leverage_init_reaches_the_same_pass_from_both_delivery_paths():
  """The ACK section and the runtime SYSTEM broadcast carry the same payload and
  must converge on one leverage-init pass — the connect-time and runtime paths
  differ only in how the message got here."""
  from worker.gateways.crypto.signal_processor import CryptoSignalProcessor
  from worker.schemas.inbox_schema import WorkerConnectedAckSchema
  from worker.schemas.system_schema import SystemCryptoLeverageInitSchema

  runs = []
  proc = CryptoSignalProcessor.__new__(CryptoSignalProcessor)
  proc._run_leverage_init = lambda symbols=None, max_leverage_cap=None: runs.append(
    (symbols, max_leverage_cap)
  )

  ack = WorkerConnectedAckSchema(
    account_id="CRYPTO-BINANCE-1",
    crypto_leverage_init={"symbols": ["BTC"], "default_leverage": 10},
  )
  proc._apply_market_init(ack)

  proc._handle_system_action(
    SystemActionEnum.CRYPTO_LEVERAGE_INIT,
    SystemCryptoLeverageInitSchema(
      account_id="CRYPTO-BINANCE-1", symbols=["BTC"], default_leverage=10
    ).model_dump(mode="json"),
  )

  assert runs == [(["BTC"], 10), (["BTC"], 10)]
