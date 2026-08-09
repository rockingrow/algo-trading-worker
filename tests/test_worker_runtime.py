"""Tests for the shared worker runtime + the unified orchestrator factory."""

from worker.market import (
  GatewayProcessOrchestrator,
  ThreadGatewayOrchestrator,
  create_market_orchestrator,
)
from worker.settings import MarketTypeEnum
from worker.worker_runtime import run_worker


class FakeProcessor:
  """Records the lifecycle calls run_worker makes."""

  def __init__(self, ctx, settings, *, connect_ok=True, jobs_error=False):
    self.calls = []
    self._connect_ok = connect_ok
    self._jobs_error = jobs_error

  def connect(self):
    self.calls.append("connect")
    return self._connect_ok

  def send_startup_notification(self):
    self.calls.append("startup")

  def start_market_jobs(self, stop_event):
    self.calls.append("jobs")
    if self._jobs_error:
      raise RuntimeError("job startup boom")

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
    "connect",
    "startup",
    "jobs",
    "run",
    "shutdown",
    "shutdown_notify",
  ]


def test_run_worker_aborts_when_connect_fails(monkeypatch):
  _patch_ctx(monkeypatch)
  proc = FakeProcessor(None, None, connect_ok=False)
  run_worker(lambda ctx, s: proc, {}, stop_event=object(), label="TEST")
  # connect failed → no startup/jobs/run; shutdown still runs to release the
  # broker session/bridge, but no shutdown banner (startup never fired).
  assert proc.calls == ["connect", "shutdown"]


def test_run_worker_cleans_up_when_jobs_fail(monkeypatch):
  _patch_ctx(monkeypatch)
  proc = FakeProcessor(None, None, jobs_error=True)
  run_worker(lambda ctx, s: proc, {}, stop_event=object(), label="TEST")
  # start_market_jobs raised → run never happens, but shutdown + banner still fire.
  assert proc.calls == ["connect", "startup", "jobs", "shutdown", "shutdown_notify"]


def test_run_worker_survives_factory_error(monkeypatch):
  _patch_ctx(monkeypatch)

  def boom_factory(ctx, settings):
    raise RuntimeError("factory boom")

  # A construction failure must be logged and swallowed, never propagated.
  run_worker(boom_factory, {}, stop_event=object(), label="TEST")


def test_factory_builds_forex_process_orchestrator():
  # FOREX → child process (GIL isolation for MetaTrader5).
  orch = create_market_orchestrator({"market_type": MarketTypeEnum.FOREX})
  assert isinstance(orch, GatewayProcessOrchestrator)
  assert orch._label == "FOREX"


def test_factory_builds_crypto_thread_orchestrator():
  # CRYPTO → in-process thread (no multiprocessing).
  orch = create_market_orchestrator({"market_type": MarketTypeEnum.CRYPTO})
  assert isinstance(orch, ThreadGatewayOrchestrator)
  assert not isinstance(orch, GatewayProcessOrchestrator)
  assert orch._label == "CRYPTO"


def test_factory_rejects_unknown_market():
  import pytest

  with pytest.raises(ValueError):
    create_market_orchestrator({"market_type": "OPTIONS"})


# ── Worker entry points (now in worker.market) ──────────────────────────────── #


def test_crypto_worker_main_uses_crypto_processor(monkeypatch):
  import worker.market as m

  captured = {}

  def fake_run(factory, settings, stop, *, label):
    captured["factory"] = factory
    captured["label"] = label

  monkeypatch.setattr(m, "run_worker", fake_run)
  m.crypto_worker_main({}, object())

  from worker.gateways.crypto.signal_processor import CryptoSignalProcessor

  assert captured["factory"] is CryptoSignalProcessor
  assert captured["label"] == "Crypto"


def test_forex_worker_main_uses_forex_processor(monkeypatch):
  import worker.market as m

  captured = {}

  def fake_run(factory, settings, stop, *, label):
    captured["factory"] = factory
    captured["label"] = label

  monkeypatch.setattr(m, "run_worker", fake_run)
  m.forex_worker_main({}, object())

  from worker.gateways.forex.signal_processor import ForexSignalProcessor

  assert captured["factory"] is ForexSignalProcessor
  assert captured["label"] == "FOREX"
