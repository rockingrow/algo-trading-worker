"""
worker/gateways/reconcile_job.py
────────────────────────────────
Market-agnostic broker↔DB position reconciler (Template Method pattern).

Every market has *some* push/poll channel that reports a broker-side close
(CRYPTO: the exchange user-data websocket; FOREX: ``MT5EventJob`` scanning deal
history), and every one of them can miss a close:

* a reconnect gap, a handler exception, or worker downtime while an SL / TP /
  liquidation triggers;
* FOREX specifically: ``MT5EventJob`` deliberately ignores ``DEAL_REASON_EXPERT``
  (our own ``order_send``) to avoid double-firing, so a TP1 partial close whose
  volume happened to equal the *whole* position closes the position on the broker
  while the DB row stays ``TP1`` forever.

A missed close leaves the row stuck ``OPENED`` / ``TP1``, which then blocks later
signals (``MAX_OPEN_ORDERS``, the netting guard) even though nothing is live.

This job is the durability backstop shared by both markets: it polls the live
broker positions and, when a DB-open row matches none of them, hands the row to
*handler* to be marked closed — so correctness no longer depends on any push
channel being perfect.

The same live-vs-DB scan also catches the opposite drift: a position **open on the
broker that the DB does not track** — opened directly in the exchange UI / MT5
terminal, never through a worker signal. When *manual_handler* is supplied, such a
position is handed to it to be imported into the DB so the worker manages it like
any other. Leaving *manual_handler* unset keeps the job close-only.

Only *how a live position correlates with a DB row* differs per market (a CEX nets
one position per symbol; MT5 tracks individual tickets under per-strategy magics),
so that single variation point is the :meth:`~BasePositionReconcileJob._live_keys`
/ :meth:`~BasePositionReconcileJob._db_keys` hook pair — mirroring
``BaseSignalProcessor._flat_match_key`` / ``_flat_db_match_keys``, which solve the
same correlation problem for ADMIN FLAT.

Safety properties (identical for every market)
──────────────────────────────────────────────
* **Two-scan confirmation.** A row must be DB-open *and* broker-flat on two
  consecutive scans before it is reconciled; symmetrically, a position must be
  broker-open *and* DB-untracked on two consecutive scans before it is imported.
  This absorbs the brief lag between an entry fill and the position appearing in
  (or disappearing from) the broker's position list, so a freshly opened position
  is never mis-reconciled and a worker-opened position mid-persist is never
  re-imported.
* **No empty-fetch mass close.** If the live-position fetch raises (or the hook
  reports the data unavailable), the scan is skipped entirely and both suspect
  sets are left untouched; an API blip or an offline terminal is never read as
  "every position is flat".
* **Never reconcile what cannot be verified.** If a row's match keys cannot be
  computed (symbol resolution failed, magic unknown), the row is treated as live
  and left alone rather than closed on incomplete information.
* **Idempotent.** The close handler only ever acts on ``OPENED`` / ``TP1`` rows;
  once a row is closed it drops out of the next scan, and a late close event for
  the same row is ignored. An imported manual position becomes DB-tracked, so it
  drops out of the untracked set on the next scan and is never imported twice.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Set

from worker.logger import get_logger

log = get_logger("worker.gateways.reconcile_job")

_POLL_INTERVAL = 45  # seconds — gentle on rate limits (one position fetch)


class BasePositionReconcileJob(ABC):
  """Daemon thread that reconciles DB-open positions against live broker state.

  *handler* is invoked once per confirmed missed close with the DB row dict; it
  owns the actual DB status update and notification (it lives on the signal
  processor, which holds the gateway / notifier / presenter).

  *manual_handler*, when supplied, is invoked once per confirmed untracked live
  position with the broker's position object; it owns importing the position into
  the DB and notifying. Left ``None``, the job never inspects untracked positions
  and stays close-only.
  """

  #: Short label used in log lines, e.g. ``"CryptoReconcile"``.
  name: str = "Reconcile"
  #: Name given to the daemon thread.
  thread_name: str = "position-reconcile"

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
    # ref_source_ids that were DB-open AND broker-flat on the previous scan.
    self._suspected: Set[Any] = set()
    # Live-position ids that were broker-open AND DB-untracked on the previous
    # scan (candidates for manual import; unused when manual_handler is None).
    self._suspected_manual: Set[Any] = set()
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  # ── lifecycle ───────────────────────────────────────────────────────────── #

  def start(self, stop_event=None) -> None:
    """Start the background polling thread.

    *stop_event* can be a ``threading.Event`` or ``multiprocessing.Event`` — both
    expose the same ``is_set()`` / ``wait()`` interface. When provided, the job
    shares the process-level shutdown signal so it stops with the rest of the
    worker.
    """
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run, name=self.thread_name, daemon=True
    )
    self._thread.start()
    log.info("[%s] Started (poll_interval=%ds).", self.name, self._poll_interval)

  def stop(self) -> None:
    self._stop_event.set()

  # ── main loop ───────────────────────────────────────────────────────────── #

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        self._scan()
      except Exception as exc:
        # Includes a failed live-position fetch: skip this scan, leave the
        # suspect sets untouched, and retry next interval.
        log.exception("[%s] scan error: %s", self.name, exc)
      self._stop_event.wait(self._next_interval())

  def _scan(self) -> None:
    if not self._should_scan():
      return

    # Live broker positions, fetched once. A failure here raises out of _scan
    # (caught by _run) or returns None, rather than yielding an empty list, so a
    # transient error can never look like "everything is flat".
    live_positions = self._fetch_live_positions()
    if live_positions is None:
      log.warning(
        "[%s] Live positions unavailable — skipping scan (suspects kept).", self.name
      )
      return

    db_rows = self._db.get_open_positions_for_flat()

    self._reconcile_missed_closes(db_rows, live_positions)
    self._import_untracked_positions(db_rows, live_positions)

  # ── direction 1: DB open, broker flat → close the stuck row ─────────────── #

  def _reconcile_missed_closes(self, db_rows, live_positions) -> None:
    """Mark DB-open rows that match no live broker position as closed."""
    live_keys = self._collect_live_keys(live_positions)

    stale: Dict[Any, Dict[str, Any]] = {
      row["ref_source_id"]: row
      for row in db_rows
      if self._is_row_stale(row, live_keys)
    }

    # Confirm only rows stale on this scan AND the previous one.
    confirmed = [row for rid, row in stale.items() if rid in self._suspected]
    self._suspected = set(stale)

    for row in confirmed:
      log.warning(
        "[%s] DB row open but broker flat | symbol=%s strategy=%s "
        "ref_source_id=%s — reconciling as closed (missed close event).",
        self.name, row.get("symbol"), row.get("strategy"), row.get("ref_source_id"),
      )
      try:
        self._handler(row)
      except Exception as exc:
        # Leave the row open; it stays stale and is retried on the next scan.
        log.exception(
          "[%s] handler failed for ref_source_id=%s: %s",
          self.name, row.get("ref_source_id"), exc,
        )

  def _is_row_stale(self, row: Dict[str, Any], live_keys: Set[Any]) -> bool:
    """True when *row* correlates with no live broker position.

    A row whose keys cannot be resolved (symbol lookup failed, magic unknown) is
    reported **not** stale: closing a position on unverifiable information is far
    worse than leaving a stuck row for another scan.
    """
    try:
      keys = self._db_keys(row)
    except Exception as exc:
      log.warning(
        "[%s] Cannot resolve match keys for ref_source_id=%s (%s) — treating as live.",
        self.name, row.get("ref_source_id"), exc,
      )
      return False
    if not keys:
      log.warning(
        "[%s] No match keys for ref_source_id=%s — treating as live.",
        self.name, row.get("ref_source_id"),
      )
      return False
    return keys.isdisjoint(live_keys)

  # ── direction 2: broker open, DB untracked → import the position ────────── #

  def _import_untracked_positions(self, db_rows, live_positions) -> None:
    """Import positions open on the broker that the DB does not track.

    A live position matching no DB-open row was opened outside the worker
    (manually on the exchange / in the terminal); once confirmed on two
    consecutive scans it is handed to *manual_handler* to be persisted.
    """
    if self._manual_handler is None:
      return

    tracked: Set[Any] = set()
    for row in db_rows:
      try:
        tracked |= self._db_keys(row)
      except Exception as exc:
        # An incomplete tracked set would make a worker-owned position look
        # manual and get imported twice — skip the import pass entirely.
        log.warning(
          "[%s] Cannot resolve match keys for ref_source_id=%s (%s) — "
          "skipping manual-import pass this scan.",
          self.name, row.get("ref_source_id"), exc,
        )
        return

    untracked: Dict[Any, Any] = {
      self._live_id(pos): pos
      for pos in live_positions
      if self._live_keys(pos).isdisjoint(tracked)
    }

    # Import only positions untracked on this scan AND the previous one, so a
    # worker-opened position still mid-persist is never mistaken for a manual one.
    confirmed = [pos for pid, pos in untracked.items() if pid in self._suspected_manual]
    self._suspected_manual = set(untracked)

    for pos in confirmed:
      log.warning(
        "[%s] Broker position untracked in DB | symbol=%s side=%s vol=%s — "
        "importing (opened outside the worker).",
        self.name, getattr(pos, "symbol", None), getattr(pos, "side", None),
        getattr(pos, "volume", None),
      )
      try:
        self._manual_handler(pos)
      except Exception as exc:
        # Leave it unimported; it stays untracked and is retried on the next scan.
        log.exception(
          "[%s] manual_handler failed for symbol=%s: %s",
          self.name, getattr(pos, "symbol", None), exc,
        )

  # ── helpers ─────────────────────────────────────────────────────────────── #

  def _collect_live_keys(self, live_positions) -> Set[Any]:
    """Union of every key the live positions can be matched by."""
    keys: Set[Any] = set()
    for pos in live_positions:
      keys |= self._live_keys(pos)
    return keys

  def _next_interval(self) -> int:
    """Seconds to wait before the next scan. Overridden by markets that idle at a
    slower cadence while their venue is closed."""
    return self._poll_interval

  def _should_scan(self) -> bool:
    """Whether a scan should run at all (default: always).

    Overridden by markets whose broker connection is legitimately down at times
    (e.g. the FOREX weekend window), where an empty position list would otherwise
    be misread as a flat account.
    """
    return True

  # ── Abstract hooks (each concrete market must define these) ─────────────── #

  @abstractmethod
  def _fetch_live_positions(self) -> Optional[List[Any]]:
    """Return the worker's live broker positions, or ``None`` when live state is
    unavailable (which skips the scan). Raising is equally acceptable — ``_run``
    catches it and the suspect sets are left untouched either way."""

  @abstractmethod
  def _live_keys(self, pos: Any) -> Set[Any]:
    """The set of keys a *live broker position* can be correlated by (a CEX →
    ``{resolved symbol}``; MT5 → ticket plus its ``(symbol, magic)`` slot)."""

  @abstractmethod
  def _db_keys(self, row: Dict[str, Any]) -> Set[Any]:
    """The set of keys a *DB row* can be correlated by — intersected against the
    keys produced by :meth:`_live_keys`. An empty set means "cannot verify" and
    the row is left alone."""

  @abstractmethod
  def _live_id(self, pos: Any) -> Any:
    """Stable identity of a live position, used to key the manual-import suspect
    set across scans (a CEX → resolved symbol; MT5 → ticket)."""
