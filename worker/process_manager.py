"""
worker/process_manager.py
─────────────────────────
Generic child-process supervisor.

Runs a blocking worker function in a **separate OS process** so a GIL-holding
extension (the MetaTrader5 C extension for FOREX) or a long-lived
websocket/REST loop (for CRYPTO) never freezes the FastAPI/uvicorn event loop.

Market-agnostic: it only manages the subprocess lifetime. All broker/exchange
specifics live in the worker function executed inside the child process.
:class:`~worker.market.GatewayProcessOrchestrator` (FOREX) uses this; CRYPTO
runs via :class:`~worker.market.ThreadGatewayOrchestrator` (thread, not process).
"""

from __future__ import annotations

import multiprocessing
from typing import Optional


class WorkerProcessManager:
  """Manages a child process that runs a worker function."""

  def __init__(
    self, settings_dict: dict, worker_fn, process_name: str = "worker"
  ) -> None:
    self._settings_dict = settings_dict
    self._worker_fn = worker_fn
    self._process_name = process_name
    self._process: Optional[multiprocessing.Process] = None
    self._stop_event = multiprocessing.Event()
    self._stopping = False

  def _spawn(self) -> multiprocessing.Process:
    return multiprocessing.Process(
      target=self._worker_fn,
      args=(self._settings_dict, self._stop_event),
      name=self._process_name,
      daemon=True,
    )

  def start(self) -> None:
    """Spawn the child process."""
    self._stop_event.clear()
    self._process = self._spawn()
    self._process.start()

  def stop(self) -> None:
    """Signal the child to shut down gracefully, then force-kill if needed."""
    self._stopping = True
    if self._process and self._process.is_alive():
      self._stop_event.set()
      self._process.join(timeout=15)  # wait for shutdown notification to send
      if self._process.is_alive():
        self._process.terminate()
        self._process.join(timeout=5)
        if self._process.is_alive():
          self._process.kill()

  def restart(self) -> None:
    """Restart the child process after an unexpected crash."""
    self._stopping = False
    self._stop_event.clear()
    self._process = self._spawn()
    self._process.start()

  @property
  def is_alive(self) -> bool:
    return self._process is not None and self._process.is_alive()

  @property
  def stopping(self) -> bool:
    return self._stopping
