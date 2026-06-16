"""
worker/gateways/crypto/reconcile_job.py
───────────────────────────────────────
Periodic safety-net reconciler for CRYPTO positions — the durability backstop
behind the exchange user-data stream.

The websocket user-data stream (:class:`BinanceUserDataStream`) is the *primary*
source of exchange-side close events, but it can miss a fill: a reconnect gap,
a handler exception, or worker downtime while an SL / TP / liquidation triggers.
A missed event leaves the DB row stuck ``OPENED`` / ``TP1`` forever. This job
polls the live exchange positions and, when a DB-open position no longer exists
on the exchange, hands the row to *handler* to be marked closed — so correctness
no longer depends on the push stream being perfect.

Safety properties
─────────────────
* **Two-scan confirmation.** A row must be DB-open *and* exchange-flat on two
  consecutive scans before it is reconciled. This absorbs the brief lag between
  an entry fill and the position appearing in ``positionRisk``, so a freshly
  opened position is never mis-reconciled.
* **No empty-fetch mass close.** If the live-position fetch raises, the scan is
  skipped entirely (the suspect set is left untouched); an API blip is never
  read as "every position is flat".
* **Idempotent.** The handler only ever acts on ``OPENED`` / ``TP1`` rows; once a
  row is closed it drops out of the next scan, and a late stream event for the
  same row is ignored (the row is no longer open).
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Set

from worker.logger import get_logger

log = get_logger("worker.gateways.crypto.reconcile_job")

_POLL_INTERVAL = 45  # seconds — gentle on rate limits (one positionRisk call)


class CryptoReconcileJob:
  """Daemon thread that reconciles DB-open positions against live exchange state.

  *handler* is invoked once per confirmed missed close with the DB row dict; it
  owns the actual DB status update and notification (it lives on the signal
  processor, which holds the gateway / notifier / presenter).
  """

  def __init__(
    self,
    db_service: Any,
    executor: Any,
    handler: Callable[[Dict[str, Any]], None],
    poll_interval: int = _POLL_INTERVAL,
  ) -> None:
    self._db = db_service
    self._executor = executor
    self._handler = handler
    self._poll_interval = poll_interval
    # ref_source_ids that were DB-open AND exchange-flat on the previous scan.
    self._suspected: Set[Any] = set()
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  # ── lifecycle ───────────────────────────────────────────────────────────── #

  def start(self, stop_event=None) -> None:
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run, name="crypto-reconcile", daemon=True
    )
    self._thread.start()
    log.info("[CryptoReconcile] Started (poll_interval=%ds).", self._poll_interval)

  def stop(self) -> None:
    self._stop_event.set()

  # ── main loop ───────────────────────────────────────────────────────────── #

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        self._scan()
      except Exception as exc:
        # Includes a failed live-position fetch: skip this scan, leave the
        # suspect set untouched, and retry next interval.
        log.exception("[CryptoReconcile] scan error: %s", exc)
      self._stop_event.wait(self._poll_interval)

  def _scan(self) -> None:
    # Resolved exchange symbols that currently hold an open position. A failure
    # here raises out of _scan (caught by _run) rather than yielding an empty set,
    # so a transient API error can never look like "everything is flat".
    live = {p.symbol for p in self._executor.get_all_open_positions()}

    db_rows = self._db.get_open_positions_for_flat()
    stale: Dict[Any, Dict[str, Any]] = {
      row["ref_source_id"]: row
      for row in db_rows
      if self._executor.get_symbol(row["symbol"]) not in live
    }

    # Confirm only rows stale on this scan AND the previous one.
    confirmed = [row for rid, row in stale.items() if rid in self._suspected]
    self._suspected = set(stale)

    for row in confirmed:
      log.warning(
        "[CryptoReconcile] DB row open but exchange flat | symbol=%s "
        "ref_source_id=%s — reconciling as closed (missed fill event).",
        row.get("symbol"), row.get("ref_source_id"),
      )
      try:
        self._handler(row)
      except Exception as exc:
        # Leave the row open; it stays stale and is retried on the next scan.
        log.exception(
          "[CryptoReconcile] handler failed for ref_source_id=%s: %s",
          row.get("ref_source_id"), exc,
        )
