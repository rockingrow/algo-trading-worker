"""
worker/gateways/forex/reconcile_job.py
──────────────────────────────────────
Periodic safety-net reconciler for FOREX positions — the durability backstop
behind :class:`~worker.jobs.mt5_event_job.MT5EventJob`.

``MT5EventJob`` is the *primary* source of platform-side close events, but by
design it only reports closes the **terminal** initiated (SL / TP / stop-out /
manual): a close carrying ``DEAL_REASON_EXPERT`` is our own ``order_send`` and is
skipped so the pipeline never double-fires. That leaves a real gap:

* a TP1 **partial close whose volume equals the whole position** (common when the
  entry was already at the broker's minimum lot, e.g. 0.01) closes the position
  outright, yet the DB row stays ``TP1`` — the position is gone on the broker and
  no terminal event will ever arrive for it;
* a close that landed while the worker was down, or whose ticket vanished during
  a reconnect gap, is likewise never seen.

A row stuck ``OPENED`` / ``TP1`` then blocks later signals (``MAX_OPEN_ORDERS``,
the netting guard) even though nothing is live on the broker.

This job polls the live platform positions and marks any DB-open row that
correlates with none of them as closed. The poll loop, two-scan confirmation, and
safety properties live in the market-agnostic
:class:`~worker.gateways.reconcile_job.BasePositionReconcileJob`; this subclass
supplies the FOREX specifics:

Correlation (``_live_keys`` / ``_db_keys``)
───────────────────────────────────────────
MT5 tracks individual **tickets**, and several strategies may hold the same symbol
under different magics, so a row is matched primarily by ticket (``ref_id`` /
``ref_source_id`` — the same pair ``ForexSignalProcessor`` uses for ADMIN FLAT).
A ticket match alone is not enough, though: some brokers **re-ticket** a position
after a partial close, and the new ticket appears in neither DB column. So each
row also carries its ``(resolved symbol, magic)`` *slot* key: while any live
position occupies that strategy's slot on that symbol, the row is considered live
and is never reconciled. The slot key is deliberately conservative — it can only
ever *prevent* a reconcile, never cause one, which is the right trade-off when
the alternative is marking a live position closed and losing management of it.
When the row's magic cannot be resolved (strategy missing from
``STRATEGY_MAGIC_MAP``), the key degrades to symbol-only — more conservative
still.

Connection safety
─────────────────
``MT5Gateway.get_positions`` maps ``positions_get() → None`` (terminal offline) to
an empty list, which is indistinguishable from a genuinely flat account and would
otherwise reconcile the entire book. Scans are therefore skipped while the market
is closed for the weekend (the health thread deliberately closes the connection
then) and whenever the platform is not connected — including a re-check before an
empty position list is trusted.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set

from worker.gateways.reconcile_job import BasePositionReconcileJob
from worker.logger import get_logger
from worker.settings import MT5_HEALTH_INTERVAL_WEEKEND, is_market_closed

log = get_logger("worker.gateways.forex.reconcile_job")

# Seconds between scans. ``positions_get()`` is a local terminal call with no
# rate limit, but this is only a backstop behind MT5EventJob's 5 s polling, so a
# gentle cadence is plenty: a stuck row clears within two intervals.
_POLL_INTERVAL = 60


class ForexReconcileJob(BasePositionReconcileJob):
  """Reconciles DB-open positions against live platform state, by ticket/slot."""

  name = "ForexReconcile"
  thread_name = "forex-reconcile"

  def __init__(
    self,
    db_service: Any,
    executor: Any,
    handler: Callable[[Dict[str, Any]], None],
    gateway: Any = None,
    poll_interval: int = _POLL_INTERVAL,
    manual_handler: Callable[[Any], None] | None = None,
    magic_for: Callable[[str], Optional[int]] | None = None,
    market_closed_fn: Callable[[], bool] = is_market_closed,
  ) -> None:
    """*gateway* is used only for connection checks (an offline terminal reports
    zero positions); *magic_for* resolves a strategy to its MT5 magic for the slot
    key. Both are optional so the job stays unit-testable with plain fakes."""
    super().__init__(
      db_service=db_service,
      executor=executor,
      handler=handler,
      poll_interval=poll_interval,
      manual_handler=manual_handler,
    )
    self._gateway = gateway
    self._magic_for = magic_for
    self._market_closed = market_closed_fn

  # ── Scan gating ─────────────────────────────────────────────────────────── #

  def _should_scan(self) -> bool:
    if self._market_closed():
      # Weekend maintenance window: the health thread has closed the connection,
      # so positions_get() would report an empty (meaningless) book.
      return False
    if self._gateway is not None and not self._gateway.is_connected():
      log.debug("[%s] Platform not connected — skipping scan.", self.name)
      return False
    return True

  def _next_interval(self) -> int:
    # Idle at the slow weekend cadence instead of waking every poll interval to
    # do nothing, mirroring MT5EventJob.
    return MT5_HEALTH_INTERVAL_WEEKEND if self._market_closed() else self._poll_interval

  # ── Market hooks ────────────────────────────────────────────────────────── #

  def _fetch_live_positions(self) -> Optional[List[Any]]:
    positions = self._executor.get_all_open_positions()
    if not positions and self._gateway is not None and not self._gateway.is_connected():
      # The terminal dropped mid-scan: an empty list here means "no data", not
      # "flat account". Report unavailable so the scan is skipped.
      return None
    return positions

  def _live_keys(self, pos: Any) -> Set[Any]:
    # Ticket first; the slot keys let a re-ticketed position still match its row.
    return {
      self._ticket_key(pos.ticket),
      self._slot_key(pos.symbol, pos.magic),
      self._symbol_key(pos.symbol),
    }

  def _db_keys(self, row: Dict[str, Any]) -> Set[Any]:
    keys = {
      self._ticket_key(k)
      for k in (row.get("ref_id"), row.get("ref_source_id"))
      if k is not None
    }
    # The DB stores the signal symbol (e.g. XAUUSD); live positions carry the
    # broker's tradeable name (e.g. XAUUSDm), so resolve before comparing.
    symbol = self._executor.get_symbol(row["symbol"])
    magic = self._resolve_magic(row.get("strategy"))
    keys.add(
      self._slot_key(symbol, magic) if magic is not None else self._symbol_key(symbol)
    )
    return keys

  def _live_id(self, pos: Any) -> Any:
    return self._ticket_key(pos.ticket)

  # ── Key helpers ─────────────────────────────────────────────────────────── #
  # Keys are namespaced strings so ticket / slot / symbol keys can never collide
  # inside the single set the base class intersects.

  @staticmethod
  def _ticket_key(ticket: Any) -> str:
    # Tickets are ints on the platform but TEXT in the DB — normalise both sides.
    return f"ticket:{ticket}"

  @staticmethod
  def _slot_key(symbol: Any, magic: Any) -> str:
    return f"slot:{symbol}:{magic}"

  @staticmethod
  def _symbol_key(symbol: Any) -> str:
    return f"symbol:{symbol}"

  def _resolve_magic(self, strategy: Optional[str]) -> Optional[int]:
    """Magic for *strategy*, or ``None`` when it cannot be resolved (unknown
    strategy, no resolver wired) — the caller then falls back to a symbol-only
    slot key."""
    if not strategy or self._magic_for is None:
      return None
    try:
      return self._magic_for(strategy)
    except Exception:
      log.debug("[%s] No magic mapped for strategy=%s.", self.name, strategy)
      return None
