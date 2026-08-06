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

Everything above — the poll loop, two-scan confirmation, and both reconcile
directions — lives in the market-agnostic
:class:`~worker.gateways.reconcile_job.BasePositionReconcileJob` (see its module
docstring for the full safety properties, and
:class:`~worker.gateways.forex.reconcile_job.ForexReconcileJob` for the FOREX
counterpart). All this subclass supplies is *how a CEX position correlates with a
DB row*: by **resolved exchange symbol**, since the exchange nets one position
per symbol — exactly the key ``CryptoSignalProcessor`` uses to reconcile an ADMIN
FLAT.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from worker.gateways.reconcile_job import BasePositionReconcileJob


class CryptoReconcileJob(BasePositionReconcileJob):
  """Reconciles DB-open positions against live exchange state, by symbol."""

  name = "CryptoReconcile"
  thread_name = "crypto-reconcile"

  # ── Market hooks ────────────────────────────────────────────────────────── #

  def _fetch_live_positions(self) -> Optional[List[Any]]:
    # One positionRisk call. A failure propagates (never an empty list), so a
    # transient API error is skipped rather than read as "everything is flat".
    return self._executor.get_all_open_positions()

  def _live_keys(self, pos: Any) -> Set[Any]:
    # ExchangePosition.symbol is already the resolved exchange symbol.
    return {pos.symbol}

  def _db_keys(self, row: Dict[str, Any]) -> Set[Any]:
    # The DB stores the original signal symbol (e.g. BTCUSD); resolve it to the
    # exchange form (e.g. BTCUSDT) the live positions are reported under.
    return {self._executor.get_symbol(row["symbol"])}

  def _live_id(self, pos: Any) -> Any:
    # Keyed by resolved exchange symbol — one net position per symbol, matching
    # how the DB tracks and how closes are reconciled.
    return pos.symbol
