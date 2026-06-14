"""
worker/forex_worker.py
──────────────────────
Entry point for the FOREX child process.

The full child lifecycle lives in :func:`worker.worker_runtime.run_worker`; this
module only binds the FOREX platform processor. ``Mt5SignalProcessor`` is
imported *inside* the function so the heavy MetaTrader5 stack is never loaded by
the parent FastAPI process. MT5 is the first platform serving the FOREX market;
future platforms live beside it under ``worker.gateways.forex.<platform>``.
"""

from __future__ import annotations

from worker.worker_runtime import run_worker


def forex_worker_main(settings_dict: dict, stop_event) -> None:
  """Entry point for the FOREX child process."""
  from worker.gateways.forex.mt5.signal_processor import Mt5SignalProcessor

  run_worker(Mt5SignalProcessor, settings_dict, stop_event, label="FOREX")
