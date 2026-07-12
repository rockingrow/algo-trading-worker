"""
worker/jobs/manual_order_sync_job.py
────────────────────────────────────
Background polling job that adopts FOREX positions opened *by hand* on the MT5
terminal (desktop / mobile / web) into the worker's tracking, so they complete
the normal position cycle (TP1 / TP2 / SL / R_SL / FLAT) just like a
worker-opened trade.

A hand-opened order carries a magic this worker never assigned (typically 0),
so it is invisible to the magic-scoped signal pipeline: no DB row is ever
inserted for it, and every exit signal bails at the "no tracked position" guard.
This job closes that gap. Each scan it asks the executor for positions whose
magic is *not* owned (:meth:`ForexExecutor.get_manual_open_positions`), and for
any that has no DB row yet it inserts one as ``OPENED`` under the reserved
``manual_order_strategy``, with ``author = 'manual'`` on the audit log. From then
on the position is tracked exactly like any other: ``PositionCDC`` publishes it,
and exit signals addressed to ``manual_order_strategy`` resolve it through the
executor's reserved-strategy path and close it.

The reserved strategy is a single logical bucket, so it inherits the
one-active-per-``(strategy, symbol)`` DB invariant: at most one hand-opened
position per symbol can be adopted at a time. A second live manual position on
the same symbol is left untracked (and logged) rather than colliding with the
unique index.
"""

from __future__ import annotations

import threading
from typing import Any

from worker.gateways.forex.base import SIDE_LONG
from worker.icons import MANUAL
from worker.logger import get_logger
from worker.schemas.job_schema import LogAuthorEnum
from worker.services.notification_service import _box

log = get_logger("worker.jobs.manual_order_sync_job")

_POLL_INTERVAL = 5  # seconds


class ManualOrderSyncJob:
  """Daemon thread that adopts hand-opened MT5 positions into DB tracking.

  *executor* must expose ``get_manual_open_positions()`` and ``base_symbol()``
  (``ForexExecutor``); *db_service* provides the position store; *manual_strategy*
  is the reserved strategy name the adopted rows are filed under.
  """

  def __init__(
    self,
    executor: Any,
    db_service: Any,
    notifier: Any,
    manual_strategy: str,
    poll_interval: int = _POLL_INTERVAL,
  ) -> None:
    self._executor = executor
    self._db = db_service
    self._notifier = notifier
    self._manual_strategy = manual_strategy
    self._poll_interval = poll_interval
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  # ── lifecycle ─────────────────────────────────────────────────────────── #

  def start(self, stop_event=None) -> None:
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run,
      name="manual-order-sync",
      daemon=True,
    )
    self._thread.start()
    log.info(
      "[ManualOrderSyncJob] Started (strategy=%s, poll_interval=%ds)",
      self._manual_strategy, self._poll_interval,
    )

  def stop(self) -> None:
    self._stop_event.set()

  # ── main loop ─────────────────────────────────────────────────────────── #

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        self._scan()
      except Exception as exc:
        log.exception("[ManualOrderSyncJob] Unexpected error during scan: %s", exc)
      self._stop_event.wait(self._poll_interval)

  def _scan(self) -> None:
    for pos in self._executor.get_manual_open_positions():
      try:
        self._adopt(pos)
      except Exception as exc:
        # One bad position must not stop the rest of the scan.
        log.exception(
          "[ManualOrderSyncJob] Failed to adopt ticket=%s: %s",
          getattr(pos, "ticket", "?"), exc,
        )

  # ── per-position handler ──────────────────────────────────────────────── #

  def _adopt(self, pos) -> None:
    ticket = str(pos.ticket)

    # Already tracked (this or a prior scan adopted it) → nothing to do.
    if self._db.get_position(ref_source_id=ticket) is not None:
      return

    # Store under the base symbol upstream exit signals use, not the broker's
    # suffixed tradeable name — otherwise the exit's DB lookup (by base symbol)
    # would never match this row.
    symbol = self._executor.base_symbol(pos.symbol)

    # The reserved strategy is one active slot per symbol. If it is already
    # occupied (another manual position on this symbol, or a stale row), don't
    # collide with the unique index — leave this one untracked and flag it.
    existing = self._db.get_open_positions_by_strategy(self._manual_strategy, symbol)
    if existing:
      log.warning(
        "[ManualOrderSyncJob] %s already tracks a %s position (ref_source_id=%s) — "
        "leaving manual ticket=%s untracked (one-active-per-strategy/symbol).",
        symbol, self._manual_strategy, existing[0].get("ref_source_id"), ticket,
      )
      return

    action = "long" if pos.side == SIDE_LONG else "short"
    comment = "Manual order adopted from terminal"

    log.info(
      "[ManualOrderSyncJob] Adopting manual position | ticket=%s symbol=%s "
      "side=%s vol=%s magic=%s → strategy=%s",
      ticket, symbol, action, pos.volume, pos.magic, self._manual_strategy,
    )

    # Audit log first (author='manual'), then the tracked positions row — mirrors
    # the ordering the signal processor uses for worker-opened trades.
    self._db.log_position(
      strategy=self._manual_strategy,
      ref_id=ticket,
      ref_source_id=ticket,
      symbol=symbol,
      action=action,
      volume=pos.volume,
      price=pos.price_open,
      sl=pos.sl or None,
      tp1=pos.tp or None,
      gateway_return_code=0,
      comment=comment,
      author=LogAuthorEnum.MANUAL.value,
    )
    self._db.insert_position(
      ref_id=ticket,
      strategy=self._manual_strategy,
      symbol=symbol,
      action=action,
      volume=pos.volume,
      opened_price=pos.price_open,
      gateway_return_code=0,
      comment=comment,
      strategy_code=pos.magic,
    )

    self._notify(ticket, symbol, action, pos)

  def _notify(self, ticket: str, symbol: str, action: str, pos) -> None:
    self._notifier.send_message(
      _box(
        f"{MANUAL} <b>Manual Order Adopted</b>\n\n"
        f"Symbol: <b>{symbol}</b>\n"
        f"Side: <b>{action.upper()}</b>\n"
        f"Volume: <b>{pos.volume}</b>\n"
        f"Entry Price: <b>{pos.price_open}</b>\n"
        f"Position: <b>{ticket}</b>\n"
        f"Strategy: <b>{self._manual_strategy}</b>\n"
        f"----------------------------------\n"
        f"Now tracked — exit signals (TP1/TP2/SL/R_SL/FLAT) addressed to "
        f"<b>{self._manual_strategy}</b> will complete its cycle."
      )
    )
