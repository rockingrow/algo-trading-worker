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

The same live-vs-DB scan also catches the opposite drift: a position **open on
the exchange that the DB does not track** — a manual open placed directly via the
exchange UI / app / a third party, which never flowed through a worker signal.
When *manual_handler* is supplied, such a position is handed to it to be imported
into the DB so the worker manages it (close detection, downstream publishing) like
any other. Leaving *manual_handler* unset keeps the job close-only.

Safety properties
─────────────────
* **Two-scan confirmation.** A row must be DB-open *and* exchange-flat on two
  consecutive scans before it is reconciled; symmetrically, a position must be
  exchange-open *and* DB-untracked on two consecutive scans before it is imported.
  This absorbs the brief lag between an entry fill and the position appearing in
  (or disappearing from) ``positionRisk``, so a freshly opened position is never
  mis-reconciled and a worker-opened position mid-persist is never re-imported.
* **No empty-fetch mass close.** If the live-position fetch raises, the scan is
  skipped entirely (both suspect sets are left untouched); an API blip is never
  read as "every position is flat".
* **Idempotent.** The close handler only ever acts on ``OPENED`` / ``TP1`` rows;
  once a row is closed it drops out of the next scan, and a late stream event for
  the same row is ignored. An imported manual position becomes DB-tracked, so it
  drops out of the untracked set on the next scan and is never imported twice.
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

  *manual_handler*, when supplied, is invoked once per confirmed untracked live
  position with the :class:`ExchangePosition`; it owns importing the position into
  the DB and notifying. Left ``None``, the job never inspects untracked positions
  and stays close-only (its original behaviour).
  """

  def __init__(
    self,
    db_service: Any,
    executor: Any,
    handler: Callable[[Dict[str, Any]], None],
    poll_interval: int = _POLL_INTERVAL,
    manual_handler: Callable[[Any], None] | None = None,
  ) -> None:
    self._db = db_service
    self._executor = executor
    self._handler = handler
    self._manual_handler = manual_handler
    self._poll_interval = poll_interval
    # ref_source_ids that were DB-open AND exchange-flat on the previous scan.
    self._suspected: Set[Any] = set()
    # Resolved exchange symbols that were exchange-open AND DB-untracked on the
    # previous scan (candidates for manual import; unused when manual_handler is None).
    self._suspected_manual: Set[Any] = set()
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
    # Live exchange positions, fetched once (one positionRisk call). A failure
    # here raises out of _scan (caught by _run) rather than yielding an empty
    # list, so a transient API error can never look like "everything is flat".
    live_positions = self._executor.get_all_open_positions()
    live = {p.symbol for p in live_positions}  # resolved exchange symbols

    db_rows = self._db.get_open_positions_for_flat()

    self._reconcile_missed_closes(db_rows, live)
    self._import_manual_positions(db_rows, live_positions)

  def _reconcile_missed_closes(self, db_rows, live: Set[Any]) -> None:
    """Mark DB-open rows that no longer exist on the exchange as closed."""
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

  def _import_manual_positions(self, db_rows, live_positions) -> None:
    """Import positions open on the exchange that the DB does not track.

    A live position whose resolved symbol matches no DB-open row was opened
    outside the worker (manually on the exchange); once confirmed on two
    consecutive scans it is handed to *manual_handler* to be persisted.
    """
    if self._manual_handler is None:
      return

    tracked = {self._executor.get_symbol(row["symbol"]) for row in db_rows}
    # Keyed by resolved exchange symbol — one net position per symbol, matching
    # how the DB tracks and how closes are reconciled.
    untracked: Dict[Any, Any] = {
      pos.symbol: pos for pos in live_positions if pos.symbol not in tracked
    }

    # Import only positions untracked on this scan AND the previous one, so a
    # worker-opened position still mid-persist is never mistaken for a manual one.
    confirmed = [pos for sym, pos in untracked.items() if sym in self._suspected_manual]
    self._suspected_manual = set(untracked)

    for pos in confirmed:
      log.warning(
        "[CryptoReconcile] Exchange position untracked in DB | symbol=%s "
        "side=%s qty=%s — importing (opened manually on exchange).",
        pos.symbol, getattr(pos, "side", None), getattr(pos, "volume", None),
      )
      try:
        self._manual_handler(pos)
      except Exception as exc:
        # Leave it unimported; it stays untracked and is retried on the next scan.
        log.exception(
          "[CryptoReconcile] manual_handler failed for symbol=%s: %s",
          getattr(pos, "symbol", None), exc,
        )
