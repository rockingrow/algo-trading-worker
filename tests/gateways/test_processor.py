"""Tests for the Template-Method BaseSignalProcessor.

Verifies the base enforces the broker hooks (abstract methods) and that the
shared `_process_message` algorithm runs identically regardless of market.
"""

from types import SimpleNamespace

import pytest
from helpers import make_signal

from worker.gateways.processor import BaseSignalProcessor
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.signal_schema import SignalActionEnum


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
  def order_filled(signal, result, pos_ticket, footer):
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
    self._market_type = "FAKE_MKT"
    self._connected = connected
    self.admin_calls = []
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
