"""
worker/mt5_worker.py
────────────────────
Entry point for the MT5 child process.

The full child lifecycle lives in :func:`worker.worker_runtime.run_worker`; this
module only binds the MT5 processor. ``Mt5SignalProcessor`` is imported *inside*
the function so the heavy MetaTrader5 stack is never loaded by the parent
FastAPI process.
"""

from __future__ import annotations

from worker.worker_runtime import run_worker


def mt5_worker_main(settings_dict: dict, stop_event) -> None:
  """Entry point for the MT5 child process."""
  from worker.gateways.mt5.signal_processor import Mt5SignalProcessor

  run_worker(Mt5SignalProcessor, settings_dict, stop_event, label="MT5")
