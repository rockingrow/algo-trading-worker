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
  def order_filled(signal, result, pos_ticket, footer, risk_info=None, settings_dict=None):
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

  # Record dispatched SYSTEM actions (base parses the envelope + ensures
  # connection, then calls this hook).
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
  proc._process_message(NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json())

  assert len(proc.db.inserted) == 1
  row = proc.db.inserted[0]
  assert row["strategy_code"] == 777  # came from the broker hook (_magic_for)
  assert row["market_type"] == "FAKE_MKT"
  assert row["action"] == "long"
  assert proc.notifications == ["filled:LONG:555"]
  assert proc.magic_calls == ["strat-1"]


def test_exit_signal_updates_status():
  result = {"success": True, "ticket": 9, "source_ticket": 5, "price": 31000.0, "volume": 0.02}
  proc = FakeProcessor(result)
  proc._process_message(NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.SL).model_dump_json())

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
  result = {"success": True, "ticket": 9, "source_ticket": 5, "price": 0.0, "volume": 0.02}
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
  result = {"success": True, "ticket": 9, "source_ticket": 5, "price": 0.0, "volume": 0.02}
  proc = FakeProcessor(result)
  proc._process_message(
    NatsSubjectEnum.SIGNAL,
    make_signal(SignalActionEnum.FLAT, price=None).model_dump_json(),
  )
  assert proc.db.logged[0]["price"] == 0.0


def test_failed_result_sends_failure_notification():
  proc = FakeProcessor({"success": False, "retcode": -1, "comment": "boom"})
  proc._process_message(NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json())
  assert proc.db.inserted == []
  assert proc.notifications == ["failed:LONG"]


# ── MAX_OPEN_ORDERS exposure guard ─────────────────────────────────────────── #


def _open_row(strategy, symbol):
  return {
    "strategy": strategy, "symbol": symbol, "status": "OPENED",
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
    make_signal(SignalActionEnum.LONG, symbol="XAUUSD", strategy="strat-1").model_dump_json(),
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
    make_signal(SignalActionEnum.LONG, symbol="XAUUSD", strategy="strat-2").model_dump_json(),
  )

  assert seen == []
  assert len(proc.db.rejected) == 1
  assert "strat-1" in proc.db.rejected[0]["comment"]


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


def test_system_subject_parsed_and_dispatched_to_action_hook():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1"}
  raw = (
    '{"action":"CRYPTO_LEVERAGE_INIT","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1","symbols":["BTC","ETH"],"default_leverage":10}'
  )
  proc._process_message(NatsSubjectEnum.SYSTEM, raw)
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
  raw = (
    '{"action":"CRYPTO_LEVERAGE_INIT","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-acct-1","symbols":["BTC"]}'
  )
  proc._process_message(NatsSubjectEnum.SYSTEM, raw)
  assert len(proc.system_calls) == 1


def test_system_wrong_account_id_silently_skips():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-acct-1"}
  raw = (
    '{"action":"CRYPTO_LEVERAGE_INIT","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-other","symbols":["BTC"]}'
  )
  proc._process_message(NatsSubjectEnum.SYSTEM, raw)
  assert proc.system_calls == []


def test_system_malformed_json_dropped_without_dispatch():
  proc = FakeProcessor({"success": True})
  proc._process_message(NatsSubjectEnum.SYSTEM, "not json {{")
  assert proc.system_calls == []


def test_system_invalid_envelope_dropped_without_dispatch():
  proc = FakeProcessor({"success": True})
  # Unknown action fails SystemActionEnum validation → dropped before dispatch.
  proc._process_message(NatsSubjectEnum.SYSTEM, '{"action":"NOPE","timestamp":"2026-06-30T00:00:00+00:00"}')
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
  proc.publisher = FakePublisher([
    '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}'
  ])
  proc._announce_worker_connected()

  assert len(proc.publisher.requests) == 1
  subject, payload, timeout = proc.publisher.requests[0]
  assert subject == NatsSubjectEnum.SYSTEM
  import json as _json

  assert _json.loads(payload)["action"] == SystemActionEnum.WORKER_CONNECTED.value
  assert timeout == 5
  assert proc.system_calls == []


def test_announce_worker_connected_routes_crypto_leverage_init_reply():
  proc = _connected_proc()
  proc.publisher = FakePublisher([
    '{"action":"CRYPTO_LEVERAGE_INIT","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1","symbols":["BTC"],"default_leverage":10}'
  ])
  proc._announce_worker_connected()

  assert len(proc.system_calls) == 1
  action, data = proc.system_calls[0]
  assert action == SystemActionEnum.CRYPTO_LEVERAGE_INIT
  assert data["symbols"] == ["BTC"]
  assert data["default_leverage"] == 10


def test_announce_worker_connected_error_reply_logged_without_retry():
  proc = _connected_proc()
  proc.publisher = FakePublisher([
    '{"action":"WORKER_CONNECTED_ERROR","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1","reason":"missing settings"}'
  ])
  proc._announce_worker_connected()

  # The broker responded (not a timeout) — a config problem on its side isn't
  # fixed by retrying, so the handshake attempt ends here, logged for an operator.
  assert len(proc.publisher.requests) == 1
  assert proc.system_calls == []


def test_announce_worker_connected_retries_with_backoff_on_timeout(monkeypatch):
  sleeps = []
  monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
  proc = _connected_proc()
  proc.publisher = FakePublisher([
    TimeoutError("no reply"),
    TimeoutError("no reply"),
    '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}',
  ])
  proc._announce_worker_connected()

  assert len(proc.publisher.requests) == 3
  assert sleeps == [0, 5, 10]  # leading 0 = jitter (pinned), then backoff schedule


def test_announce_worker_connected_retries_indefinitely_until_success(monkeypatch):
  sleeps = []
  monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
  proc = _connected_proc()
  proc.publisher = FakePublisher([
    TimeoutError("no reply"),
    TimeoutError("no reply"),
    TimeoutError("no reply"),
    TimeoutError("no reply"),
    '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}',
  ])
  proc._announce_worker_connected()

  assert len(proc.publisher.requests) == 5
  # Leading 0 = jitter (pinned); backoff caps at its last configured value
  # rather than growing unbounded.
  assert sleeps == [0, 5, 10, 20, 20]


def test_announce_worker_connected_escalates_to_error_after_threshold(monkeypatch):
  monkeypatch.setattr(time, "sleep", lambda s: None)
  levels = []
  monkeypatch.setattr(processor_module.log, "warning", lambda *a, **k: levels.append("WARNING"))
  monkeypatch.setattr(processor_module.log, "error", lambda *a, **k: levels.append("ERROR"))
  proc = _connected_proc()
  proc.publisher = FakePublisher([
    TimeoutError("no reply"),
    TimeoutError("no reply"),
    TimeoutError("no reply"),
    '{"action":"WORKER_CONNECTED_ACK","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}',
  ])
  proc._announce_worker_connected()

  # First two timeouts stay WARNING; from _HANDSHAKE_ALERT_THRESHOLD (3) onward
  # they escalate to ERROR so TelegramLogHandler forwards an operator alert.
  assert levels == ["WARNING", "WARNING", "ERROR"]


def test_system_not_connected_short_circuits():
  proc = FakeProcessor({"success": True}, connected=False)
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1"}
  raw = (
    '{"action":"CRYPTO_LEVERAGE_INIT","timestamp":"2026-06-30T00:00:00+00:00",'
    '"account_id":"FAKE_MKT-FAKE_GW-acct-1"}'
  )
  proc._process_message(NatsSubjectEnum.SYSTEM, raw)
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
  proc._process_message(NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json())
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
  assert parse_strategy_subjects("MT5_GOLD,ADMIN,MT5_FX, ,MT5_GOLD,SYSTEM,CRYPTO_ETH") == [
    "MT5_GOLD", "MT5_FX", "CRYPTO_ETH",
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
  # RETRY_SIGNALS reply for this worker.
  assert payload["strategies"] == ["MT5_GOLD", "MT5_FX"]


def test_worker_connected_payload_strategies_defaults_to_empty_when_unset():
  proc = FakeProcessor({"success": True})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1", "gw_key": "FAKE_GW"}
  proc._gateway_setting_key = "gw_key"

  import json as _json

  payload = _json.loads(proc._worker_connected_payload())
  assert payload["strategies"] == []


# ── RETRY_SIGNALS SYSTEM action ────────────────────────────────────────────── #


def _retry_signals_payload(signals, account_id="FAKE_MKT-FAKE_GW-acct-1"):
  """Serialise a RETRY_SIGNALS envelope with the given signal payloads."""
  import json as _json

  return _json.dumps({
    "action": SystemActionEnum.RETRY_SIGNALS.value,
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "account_id": account_id,
    "signals": [s.model_dump(mode="json") for s in signals],
  })


def _fresh_signal(action=SignalActionEnum.LONG, **overrides):
  """A signal whose ``timestamp`` is well within MAX_RETRY_TIMEOUT so it is
  eligible for replay unless the test overrides ``timestamp`` explicitly."""
  overrides.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
  return make_signal(action, **overrides)


def _retry_proc():
  proc = FakeProcessor({"success": True, "ticket": 1, "price": 2000.0, "volume": 0.1})
  proc.settings = {"account_id": "FAKE_MKT-FAKE_GW-acct-1"}
  # Route RETRY_SIGNALS through the real base handler instead of the FakeProcessor
  # test-only capture, so the dedup + timeout + _process_signal path is exercised.
  proc._handle_system_action = lambda action, data: BaseSignalProcessor._handle_system_action(
    proc, action, data
  )
  return proc


def test_retry_signals_executes_fresh_signal_via_process_signal():
  proc = _retry_proc()
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="sig-fresh", symbol="AAA")
  raw = _retry_signals_payload([sig])

  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

  # The eligible signal went through the normal pipeline: it was persisted as
  # an OPENED position and a fill notification was sent to the channel.
  assert len(proc.db.inserted) == 1
  assert proc.db.inserted[0]["symbol"] == "AAA"
  assert proc.notifications == ["filled:LONG:1"]


def test_retry_signals_skips_signal_already_processed():
  proc = _retry_proc()
  proc.db.known_signal_ids.add("dup-1")
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="dup-1", symbol="AAA")
  raw = _retry_signals_payload([sig])

  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

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
  raw = _retry_signals_payload([sig])

  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

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
    timestamp=(datetime.now(timezone.utc) - timedelta(seconds=MAX_RETRY_TIMEOUT + 5)).isoformat(),
  )
  raw = _retry_signals_payload([fresh, duplicate, stale])

  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

  # Only the fresh (non-dup, in-window) signal reaches the broker path.
  assert [row["symbol"] for row in proc.db.inserted] == ["AAA"]
  assert proc.notifications == ["filled:LONG:1"]


def test_retry_signals_empty_batch_is_a_no_op():
  proc = _retry_proc()
  raw = _retry_signals_payload([])

  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

  assert proc.db.inserted == []
  assert proc.notifications == []


def test_retry_signals_marks_executed_signal_id_in_db():
  """The processed signal_id is stored in position_logs + positions so a
  subsequent RETRY_SIGNALS carrying the same id dedups against it."""
  proc = _retry_proc()
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="sig-persist", symbol="AAA")
  raw = _retry_signals_payload([sig])

  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

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
  stale_ts = (datetime.now(timezone.utc) - timedelta(seconds=MAX_RETRY_TIMEOUT + 30)).isoformat()
  stale = [
    _fresh_signal(SignalActionEnum.LONG, signal_id="stale-1", symbol="STA", timestamp=stale_ts),
    _fresh_signal(SignalActionEnum.LONG, signal_id="stale-2", symbol="STB", timestamp=stale_ts),
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

  raw = _retry_signals_payload(signals)
  proc._process_message(NatsSubjectEnum.SYSTEM, raw)

  # The 5 fresh, eligible entries all executed and were persisted + notified.
  assert sorted(row["symbol"] for row in proc.db.inserted) == [f"SYM{i}" for i in range(5)]
  assert len(proc.notifications) == 5
  # Duplicates, stale entries, and the failing signal never reach persistence.
  assert not any(
    row["symbol"] in ("DUPA", "DUPB", "STA", "STB", "BOOM") for row in proc.db.inserted
  )


def test_retry_signals_dispatched_via_worker_connected_reply():
  """RETRY_SIGNALS is also a valid WORKER_CONNECTED reply — the broker can
  replay in-flight signals right after the handshake instead of on a separate
  SYSTEM push. The reply router must dispatch through the same handler."""
  proc = _connected_proc()
  # Wire the real base action-dispatch so RETRY_SIGNALS hits _handle_retry_signals.
  proc._handle_system_action = lambda action, data: BaseSignalProcessor._handle_system_action(
    proc, action, data
  )
  sig = _fresh_signal(SignalActionEnum.LONG, signal_id="wc-retry", symbol="ZZZ")
  import json as _json

  reply = _json.dumps({
    "action": SystemActionEnum.RETRY_SIGNALS.value,
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "account_id": "FAKE_MKT-FAKE_GW-acct-1",
    "signals": [sig.model_dump(mode="json")],
  })
  proc.publisher = FakePublisher([reply])

  proc._announce_worker_connected()

  assert len(proc.db.inserted) == 1
  assert proc.db.inserted[0]["symbol"] == "ZZZ"
