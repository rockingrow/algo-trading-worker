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


# ── Standalone crypto entry point ──────────────────────────────────────────── #


def test_crypto_worker_main_uses_crypto_processor(monkeypatch):
  import worker.crypto_worker as cw

  captured = {}

  def fake_run(factory, settings, stop, *, label):
    captured["factory"] = factory
    captured["label"] = label

  monkeypatch.setattr(cw, "run_worker", fake_run)
  cw.crypto_worker_main({}, object())

  from worker.gateways.crypto.signal_processor import CryptoSignalProcessor

  assert captured["factory"] is CryptoSignalProcessor
  assert captured["label"] == "Crypto"


def test_heartbeat_writes_fresh_timestamp(tmp_path, monkeypatch):
  import threading
  import time

  from worker.crypto_worker import _start_heartbeat

  hb = tmp_path / "heartbeat"
  monkeypatch.setenv("HEARTBEAT_FILE", str(hb))
  stop = threading.Event()
  _start_heartbeat(stop)
  try:
    for _ in range(100):  # wait up to ~2s for the first beat
      if hb.exists():
        break
      time.sleep(0.02)
    assert hb.exists()
    assert time.time() - float(hb.read_text()) < 5  # a real, fresh timestamp
  finally:
    stop.set()


def test_heartbeat_noop_without_env(monkeypatch):
  import threading

  from worker.crypto_worker import _start_heartbeat

  monkeypatch.delenv("HEARTBEAT_FILE", raising=False)
  _start_heartbeat(threading.Event())  # must not raise / not start a thread
