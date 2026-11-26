"""
worker/worker_runtime.py
────────────────────────
Shared child-process bootstrap for every market.

``forex_worker_main`` and ``crypto_worker_main`` (both defined in
:mod:`worker.market`) used to be byte-for-byte identical except for which
``<Market>SignalProcessor`` they built. That common lifecycle — build
:class:`WorkerContext`, start the notification job, connect, notify, start
jobs, run the loop, shut down — lives here once. Each market entry point only
supplies its processor class (lazily, so the parent process never imports a
broker SDK) and a log label.
"""

from __future__ import annotations

import multiprocessing
from typing import Callable, Optional

from worker.context import WorkerContext
from worker.logger import get_logger
from worker.settings import MarketTypeEnum, settings

log = get_logger("worker.runtime")

# How old an unresolved attempt must be before startup treats it as abandoned.
# Comfortably longer than any single broker call, so a row still in flight when
# another market's worker starts is never reported.
_UNRESOLVED_ATTEMPT_AGE = 120  # seconds


def _report_unresolved_attempts(
  ctx: WorkerContext, label: str, market_type: Optional[str]
) -> None:
  """Alert on signal attempts whose outcome was never recorded.

  An ``ATTEMPTING`` row means the worker called the broker and died, or could
  not write the answer. The position may be live or may not exist, and because
  nothing can be concluded the signal is deliberately *not* replayed — so
  without this alert the trade would sit unreconciled and silent. Run at startup
  because that is exactly when the previous process's unfinished work is
  visible.
  """
  try:
    rows = ctx.db_service.stale_signal_attempts(_UNRESOLVED_ATTEMPT_AGE, market_type)
  except Exception:
    log.exception("[%s Process] Could not scan for unresolved signal attempts.", label)
    return
  if not rows:
    return
  summary = ", ".join(
    f"{row.get('symbol')} {row.get('action')} (signal_id={row.get('signal_id')})"
    for row in rows[:10]
  )
  more = f" and {len(rows) - 10} more" if len(rows) > 10 else ""
  log.critical(
    "[%s Process] %d signal attempt(s) have no recorded outcome: %s%s. A position "
    "may be live for each. They will not be replayed — check the broker.",
    label,
    len(rows),
    summary,
    more,
  )
  try:
    ctx.notifier.send_message(
      f"⚠️ <b>[{label}] {len(rows)} signal attempt(s) with no recorded outcome</b>\n\n"
      f"{summary}{more}\n\n"
      "The worker called the broker but never recorded the answer (a crash "
      "mid-order). A position may be live and untracked for each. These signals "
      "are not replayed automatically — check the broker."
    )
  except Exception:
    log.exception("[%s Process] Could not notify unresolved signal attempts.", label)


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
  log.info(
    "[%s Process] Started (PID=%d)", label, multiprocessing.current_process().pid
  )

  ctx = WorkerContext(settings_dict)
  ctx.start_notification_job(stop_event)

  # After the outbox dispatcher is running, so the alert can actually be sent.
  market_type = settings_dict.get("market_type")
  _report_unresolved_attempts(
    ctx,
    label,
    market_type.value if isinstance(market_type, MarketTypeEnum) else market_type,
  )

  # Forward this process's ERROR logs to Telegram. A market processor may run in
  # a child process with no event loop, so the handler drains its queue on a
  # background thread; it is flushed in the finally block below so the crash
  # path's log.exception is delivered before the process exits.
  if settings.telegram.enabled and settings.telegram.log_errors_enabled:
    from worker.services.notification_service import telegram_log_handler

    telegram_log_handler.start()

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
    if settings.telegram.enabled and settings.telegram.log_errors_enabled:
      from worker.services.notification_service import telegram_log_handler

      telegram_log_handler.stop()
    log.info("[%s Process] Exiting.", label)
