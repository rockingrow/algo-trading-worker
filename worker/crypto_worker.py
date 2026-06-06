"""
worker/crypto_worker.py
───────────────────────
Entry point for the CRYPTO child process — the counterpart of
:mod:`worker.mt5_worker`.

Composition mirrors the FOREX worker:

  * :class:`WorkerContext` — market-agnostic infrastructure (DB, direct + outbox
    notifiers, NotificationJob dispatcher).
  * :class:`CryptoSignalProcessor` — exchange gateway, NATS, executor, signal
    handler, and the crypto background jobs (position CDC + user data stream).

Heavy/exchange-specific imports stay lazy (inside :func:`crypto_worker_main`) so
the parent FastAPI process never loads exchange or websocket code. Crucially,
nothing here imports ``worker.gateways.mt5.*`` or MetaTrader5 — the CRYPTO path carries
no Forex dependencies.
"""

from __future__ import annotations

import multiprocessing

from worker.context import WorkerContext
from worker.logger import get_logger

log = get_logger("worker.crypto_worker")


def crypto_worker_main(settings_dict: dict, stop_event) -> None:
  """Entry point for the crypto child process."""
  from worker.gateways.crypto.signal_processor import CryptoSignalProcessor

  log.info("[Crypto Process] Started (PID=%d)", multiprocessing.current_process().pid)

  ctx = WorkerContext(settings_dict)
  ctx.start_notification_job(stop_event)

  processor = CryptoSignalProcessor(ctx, settings_dict)
  if not processor.connect():
    return

  processor.send_startup_notification()
  log.info("[Crypto Process] Worker loop started.")

  processor.start_market_jobs(stop_event)

  try:
    processor.run(stop_event)
  except KeyboardInterrupt:
    log.info("[Crypto Process] Received shutdown signal.")
  except Exception as e:
    log.exception("[Crypto Process] Unexpected error: %s", e)
  finally:
    processor.shutdown()
    processor.send_shutdown_notification()
    log.info("[Crypto Process] Exiting.")
