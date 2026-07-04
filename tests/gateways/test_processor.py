"""Tests for the Template-Method BaseSignalProcessor.

Verifies the base enforces the broker hooks (abstract methods) and that the
shared `_process_message` algorithm runs identically regardless of market.
"""

import time
from types import SimpleNamespace

import pytest
from helpers import make_signal

from worker.gateways import processor as processor_module
from worker.gateways.config import ExecutionConfig
from worker.gateways.processor import BaseSignalProcessor
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.signal_schema import SignalActionEnum
from worker.schemas.system_schema import SystemActionEnum


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

  def log_position(self, **kw):
    self.logged.append(kw)

  def insert_position(self, **kw):
    self.inserted.append(kw)

  def update_position_status(self, **kw):
    self.updated.append(kw)


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
  def _handle_admin_message(self, raw):
    self.admin_calls.append(raw)

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


def test_admin_subject_routed_to_hook():
  proc = FakeProcessor({"success": True})
  proc._process_message(NatsSubjectEnum.ADMIN, '{"action":"FLAT"}')
  assert proc.admin_calls == ['{"action":"FLAT"}']
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
