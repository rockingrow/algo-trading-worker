"""
worker/gateways/guard.py
────────────────────────
Entry guards shared by every ``<Market>SignalProcessor``: policy checks run
before a LONG/SHORT entry is sent to the broker. A rejected entry is never
placed — it is only logged/persisted/notified by the caller.

Split out of :mod:`worker.gateways.processor` to keep the processor focused on
message dispatch; the guards themselves need only the DB service, the entry
signal, and (for the exposure cap) the worker's settings.
"""

from __future__ import annotations

from typing import Optional

from worker.schemas.signal_schema import SignalSchema


def symbol_open_rejection(
  db_service, signal: SignalSchema, *, allow_multi_strategy: bool = False
) -> Optional[str]:
  """Return a reason string when *signal* (a LONG/SHORT entry) must be rejected
  because an order is already open on its symbol, else ``None``.

  One-open-order-per-symbol policy: any active (OPENED/TP1) position on the
  symbol blocks a fresh entry, no matter which strategy holds it. This is
  stricter than MAX_OPEN_ORDERS and intentionally also rejects a same-strategy
  re-entry or scale-in — while any order is live on the symbol, no new order is
  placed. The rejected entry is still logged and forwarded to the broker with
  status REJECTED by the caller.

  ``allow_multi_strategy`` (FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL, resolved per
  market — see BaseMarketStrategy.allows_multi_strategy_per_symbol) exempts
  *other* strategies' positions from this rule: each strategy trades under its
  own magic number there, so a concurrent position is intended, not a netting
  conflict. A position already held by *this* strategy still blocks — the
  one-open-order-per-(strategy, symbol) rule is untouched by the toggle.
  """
  open_positions = db_service.get_open_positions_for_flat(symbol=signal.symbol)
  if not open_positions:
    return None
  same_strategy_open = any(p.get("strategy") == signal.strategy for p in open_positions)
  if allow_multi_strategy and not same_strategy_open:
    return None
  holders = sorted({p.get("strategy") for p in open_positions if p.get("strategy")})
  held_by = f" (held by {', '.join(holders)})" if holders else ""
  return (
    f"{signal.symbol} already has an open order{held_by}; "
    f"entry not placed (open position on symbol)."
  )


def max_open_orders_rejection(
  db_service, settings: dict, signal: SignalSchema
) -> Optional[str]:
  """Return a reason string when *signal* (a LONG/SHORT entry) must be rejected
  because the worker is already at its MAX_OPEN_ORDERS cap, else ``None``.

  The cap counts active (OPENED/TP1) positions across every strategy/symbol. A
  re-entry or scale-in on a symbol this strategy already holds replaces the
  existing position rather than opening a new slot, so it is never counted
  against the cap. A value of 0 (or unset) disables the limit.
  """
  max_orders = settings.get("max_open_orders")
  if not max_orders or max_orders <= 0:
    return None

  open_positions = db_service.get_open_positions_for_flat()
  already_held = any(
    p.get("strategy") == signal.strategy and p.get("symbol") == signal.symbol
    for p in open_positions
  )
  if already_held:
    return None

  open_count = len(open_positions)
  if open_count < max_orders:
    return None
  return (
    f"Max open orders reached ({open_count}/{max_orders}); "
    f"entry not placed (MAX_OPEN_ORDERS)."
  )
