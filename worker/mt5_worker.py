"""
worker/mt5_worker.py
────────────────────
Entry point for the MT5 child process.

Composition is split across two collaborators:

  * :class:`WorkerContext` — market-agnostic infrastructure (DB, direct +
    outbox notifiers, NotificationJob dispatcher).
  * :class:`Mt5SignalProcessor` — MT5/FOREX-specific bridge, NATS,
    executor, signal handler, and MT5 background jobs.

This module is loaded eagerly by the parent FastAPI process (via
:class:`ForexMarketOrchestrator`), so we keep heavy MetaTrader5 imports
lazy — :class:`Mt5SignalProcessor` is imported inside
:func:`mt5_worker_main` and never touched by the parent process.
"""

from __future__ import annotations

import multiprocessing

from worker.context import WorkerContext
from worker.logger import get_logger

log = get_logger("worker.mt5_worker")


def mt5_worker_main(settings_dict: dict, stop_event) -> None:
  """Entry point for the MT5 child process."""
  from worker.mt5.signal_processor import Mt5SignalProcessor

  log.info("[MT5 Process] Started (PID=%d)", multiprocessing.current_process().pid)

  ctx = WorkerContext(settings_dict)
  ctx.start_notification_job(stop_event)

  processor = Mt5SignalProcessor(ctx, settings_dict)
  if not processor.connect():
    return

  processor.send_startup_notification()
  log.info("[MT5 Process] Worker loop started.")

  processor.start_market_jobs(stop_event)

  try:
    processor.run(stop_event)
  except KeyboardInterrupt:
    log.info("[MT5 Process] Received shutdown signal.")
  except Exception as e:
    log.exception("[MT5 Process] Unexpected error: %s", e)
  finally:
    processor.shutdown()
    processor.send_shutdown_notification()
    log.info("[MT5 Process] Exiting.")
