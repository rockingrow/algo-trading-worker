"""
worker/mt5/manager.py
─────────────────────
Runs the blocking MT5 + NATS worker inside a **separate OS process** so the
GIL-holding MetaTrader5 C extension never freezes the FastAPI/uvicorn event loop.

Architecture
────────────
  FastAPI process  ──start/stop──▶  MT5Worker process
                                    ├─ MT5 reconnect loop
                                    └─ NATS listen loop

The FastAPI process only manages the subprocess lifetime; all MT5/NATS blocking
code lives in the child process and is therefore 100% GIL-isolated.
"""

from __future__ import annotations

import multiprocessing
from typing import Optional


class MT5Manager:
  """
  Manages a child process that runs a worker function.
  The parent (FastAPI) process delegates all blocking work to the child so its
  event loop is completely free from GIL interference.
  """

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
      self._process.join(timeout=15)  # wait for Telegram notification to send
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
