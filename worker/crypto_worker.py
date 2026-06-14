"""
worker/crypto_worker.py
───────────────────────
Entry point for the CRYPTO worker.

The full worker lifecycle lives in :func:`worker.worker_runtime.run_worker`; this
module only binds the crypto processor. ``CryptoSignalProcessor`` is imported
*inside* the function so the crypto stack is loaded only on the CRYPTO path.

The crypto gateway is pure Python (signed REST + a websocket user-data stream,
both in daemon threads), so — unlike MT5 — it needs **no** GIL-isolating child
process: the FastAPI/uvicorn app (``make start``) runs it in a background thread
via the thread orchestrator. Nothing here imports ``worker.gateways.forex.mt5.*`` /
MetaTrader5 — the CRYPTO path carries no Forex dependencies.
"""

from __future__ import annotations

from worker.worker_runtime import run_worker


def crypto_worker_main(settings_dict: dict, stop_event) -> None:
  """Run the crypto worker lifecycle until *stop_event* is set."""
  from worker.gateways.crypto.signal_processor import CryptoSignalProcessor

  run_worker(CryptoSignalProcessor, settings_dict, stop_event, label="Crypto")
