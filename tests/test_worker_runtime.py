"""Tests for the shared worker runtime + the unified orchestrator factory."""

from worker.market import GatewayProcessOrchestrator, create_market_orchestrator
from worker.settings import MarketTypeEnum
from worker.worker_runtime import run_worker


class FakeProcessor:
  """Records the lifecycle calls run_worker makes."""

  def __init__(self, ctx, settings, *, connect_ok=True):
    self.calls = []
    self._connect_ok = connect_ok

  def connect(self):
    self.calls.append("connect")
    return self._connect_ok

  def send_startup_notification(self):
    self.calls.append("startup")

  def start_market_jobs(self, stop_event):
    self.calls.append("jobs")

  def run(self, stop_event):
    self.calls.append("run")

  def shutdown(self):
    self.calls.append("shutdown")

  def send_shutdown_notification(self):
    self.calls.append("shutdown_notify")


def _patch_ctx(monkeypatch):
  # WorkerContext does DB + notifier setup we don't need here.
  import worker.worker_runtime as rt

  class _Ctx:
    def start_notification_job(self, stop_event):
      pass

  monkeypatch.setattr(rt, "WorkerContext", lambda settings: _Ctx())


def test_run_worker_drives_full_lifecycle(monkeypatch):
  _patch_ctx(monkeypatch)
  captured = {}

  def factory(ctx, settings):
    captured["proc"] = FakeProcessor(ctx, settings)
    return captured["proc"]

  run_worker(factory, {}, stop_event=object(), label="TEST")
  assert captured["proc"].calls == [
    "connect", "startup", "jobs", "run", "shutdown", "shutdown_notify"
  ]


def test_run_worker_aborts_when_connect_fails(monkeypatch):
  _patch_ctx(monkeypatch)
  proc = FakeProcessor(None, None, connect_ok=False)
  run_worker(lambda ctx, s: proc, {}, stop_event=object(), label="TEST")
  # No jobs/run/shutdown once connect returns False.
  assert proc.calls == ["connect"]


def test_factory_builds_forex_orchestrator():
  orch = create_market_orchestrator({"market_type": MarketTypeEnum.FOREX})
  assert isinstance(orch, GatewayProcessOrchestrator)
  assert orch._label == "FOREX"


def test_factory_builds_crypto_orchestrator():
  orch = create_market_orchestrator({"market_type": MarketTypeEnum.CRYPTO})
  assert isinstance(orch, GatewayProcessOrchestrator)
  assert orch._label == "CRYPTO"


def test_factory_rejects_unknown_market():
  import pytest

  with pytest.raises(ValueError):
    create_market_orchestrator({"market_type": "OPTIONS"})
