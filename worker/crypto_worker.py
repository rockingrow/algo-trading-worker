"""
worker/crypto_worker.py
───────────────────────
Entry point for the CRYPTO child process.

The full child lifecycle lives in :func:`worker.worker_runtime.run_worker`; this
module only binds the crypto processor. ``CryptoSignalProcessor`` is imported
*inside* the function so the parent FastAPI process never loads exchange or
websocket code, and nothing here imports ``worker.gateways.mt5.*`` / MetaTrader5
— the CRYPTO path carries no Forex dependencies.
"""

from __future__ import annotations

from worker.worker_runtime import run_worker


def crypto_worker_main(settings_dict: dict, stop_event) -> None:
  """Entry point for the crypto child process."""
  from worker.gateways.crypto.signal_processor import CryptoSignalProcessor

  run_worker(CryptoSignalProcessor, settings_dict, stop_event, label="Crypto")
