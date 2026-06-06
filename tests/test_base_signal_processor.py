"""Tests for the Template-Method BaseSignalProcessor.

Verifies the base enforces the broker hooks (abstract methods) and that the
shared `_process_message` algorithm runs identically regardless of market.
"""

from types import SimpleNamespace

import pytest
from conftest import make_signal

from worker.core.base_signal_processor import BaseSignalProcessor
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
    self.ctx = SimpleNamespace(
      db_service=self.db,
      channel_notifier=SimpleNamespace(
        send_message=lambda m: self.notifications.append(m)
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

    # Implement everything EXCEPT _handle_admin_message.
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
  assert row["magic"] == 777  # came from the broker hook
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
  assert proc.db.updated[0]["source_ticket"] == 5
  assert proc.notifications == ["filled:SL:5"]


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


def test_not_connected_short_circuits():
  proc = FakeProcessor({"success": True}, connected=False)
  proc._process_message(NatsSubjectEnum.SIGNAL, make_signal(SignalActionEnum.LONG).model_dump_json())
  assert proc.db.logged == []
  assert proc.notifications == []


def test_malformed_json_ignored():
  proc = FakeProcessor({"success": True})
  proc._process_message(NatsSubjectEnum.SIGNAL, "not json {{")
  assert proc.db.logged == []
