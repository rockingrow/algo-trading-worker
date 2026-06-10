"""
worker/worker_runtime.py
────────────────────────
Shared child-process bootstrap for every market.

``mt5_worker_main`` and ``crypto_worker_main`` used to be byte-for-byte identical
except for which ``<Market>SignalProcessor`` they built. That common lifecycle —
build :class:`WorkerContext`, start the notification job, connect, notify, start
jobs, run the loop, shut down — lives here once. Each market entry point only
supplies its processor class (lazily, so the parent process never imports a
broker SDK) and a log label.
"""

from __future__ import annotations

import multiprocessing
from typing import Callable

from worker.context import WorkerContext
from worker.logger import get_logger

log = get_logger("worker.runtime")


def run_worker(
  processor_factory: Callable[[WorkerContext, dict], object],
  settings_dict: dict,
  stop_event,
  *,
  label: str,
) -> None:
  """Drive one market's signal processor through its full lifecycle.

  *processor_factory* takes ``(ctx, settings_dict)`` and returns an object with
  ``connect()``, ``send_startup_notification()``, ``start_market_jobs(stop_event)``,
  ``run(stop_event)``, ``shutdown()`` and ``send_shutdown_notification()`` — i.e.
  a :class:`~worker.gateways.processor.BaseSignalProcessor`.
  """
  log.info("[%s Process] Started (PID=%d)", label, multiprocessing.current_process().pid)

  ctx = WorkerContext(settings_dict)
  ctx.start_notification_job(stop_event)

  # Everything after the notification job is started must run through the finally
  # block so a failure during construction, connect, or job startup never leaves a
  # half-built processor (open NATS connections / broker session, started jobs) behind.
  processor = None
  started = False
  try:
    processor = processor_factory(ctx, settings_dict)
    if not processor.connect():
      log.error("[%s Process] Could not connect to broker. Aborting startup.", label)
      return

    processor.send_startup_notification()
    started = True
    log.info("[%s Process] Worker loop started.", label)

    processor.start_market_jobs(stop_event)
    processor.run(stop_event)
  except KeyboardInterrupt:
    log.info("[%s Process] Received shutdown signal.", label)
  except Exception as e:
    log.exception("[%s Process] Unexpected error: %s", label, e)
  finally:
    if processor is not None:
      try:
        processor.shutdown()
      except Exception:
        log.exception("[%s Process] Error during processor shutdown.", label)
      # Only pair a shutdown banner with a startup banner that actually fired.
      if started:
        try:
          processor.send_shutdown_notification()
        except Exception:
          log.exception("[%s Process] Error sending shutdown notification.", label)
    log.info("[%s Process] Exiting.", label)
