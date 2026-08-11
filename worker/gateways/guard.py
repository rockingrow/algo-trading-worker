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

from worker.schemas.signal_schema import SignalActionEnum, SignalSchema

# Defaults used when the setting is absent from the flat settings dict (a
# lightweight test double, or a worker started from an older .env). They mirror
# StrategySettings.max_entry_drift_r_percent / .max_entry_drift_percent.
_DEFAULT_DRIFT_R_PERCENT = 50.0
_DEFAULT_DRIFT_PERCENT = 0.5


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


def _threshold(settings: dict, key: str, default: float) -> float:
  """Read a drift threshold from the flat settings dict.

  A missing key falls back to *default*; an explicit ``0`` (or a negative value)
  means the operator disabled that check, and is returned as-is so the caller
  can skip it.
  """
  raw = settings.get(key)
  if raw is None:
    return default
  try:
    return float(raw)
  except (TypeError, ValueError):
    return default


def _drift_rejection(
  signal: SignalSchema, live_price: float, settings: dict, quote_label: str
) -> Optional[str]:
  """Reject when the market drifted too far from the price the signal was built
  on, else ``None``.

  Two units, in priority order:

  * **% of planned risk** (``MAX_ENTRY_DRIFT_R_PERCENT``) — the drift measured
    against the signal's own entry→SL distance ("R"). This is the primary rule
    because it is self-normalizing: it means the same thing on BTCUSDT at
    100,000 as on EURUSD at 1.08, and it tightens automatically on a
    tight-stop signal without any per-symbol tuning.
  * **% of price** (``MAX_ENTRY_DRIFT_PERCENT``) — the fallback for a signal
    that carries no SL (or a zero-width one), where there is no risk distance
    to measure against.

  Either threshold set to 0 disables that rule.
  """
  if not signal.price or signal.price <= 0:
    return None
  drift = abs(live_price - signal.price)

  risk_distance = abs(signal.price - signal.sl) if signal.sl else 0.0
  if risk_distance > 0:
    max_r_percent = _threshold(
      settings, "max_entry_drift_r_percent", _DEFAULT_DRIFT_R_PERCENT
    )
    if max_r_percent <= 0:
      return None
    drift_r_percent = drift / risk_distance * 100
    if drift_r_percent <= max_r_percent:
      return None
    return (
      f"{signal.symbol} {signal.action.value} rejected: price moved too far since "
      f"the signal was generated — {quote_label} {live_price} vs signal price "
      f"{signal.price} is a drift of {drift:g} ({drift_r_percent:.1f}% of the "
      f"{risk_distance:g} entry-to-SL risk), over the "
      f"{max_r_percent:g}% MAX_ENTRY_DRIFT_R_PERCENT limit. Entering here would "
      f"take a materially worse price than the signal planned for."
    )

  max_percent = _threshold(settings, "max_entry_drift_percent", _DEFAULT_DRIFT_PERCENT)
  if max_percent <= 0:
    return None
  drift_percent = drift / signal.price * 100
  if drift_percent <= max_percent:
    return None
  return (
    f"{signal.symbol} {signal.action.value} rejected: price moved too far since "
    f"the signal was generated — {quote_label} {live_price} vs signal price "
    f"{signal.price} is a drift of {drift:g} ({drift_percent:.2f}%), over the "
    f"{max_percent:g}% MAX_ENTRY_DRIFT_PERCENT limit (signal carries no SL, so "
    f"the drift is measured against price). Entering here would take a "
    f"materially worse price than the signal planned for."
  )


def stale_signal_rejection(
  signal: SignalSchema, live_price: Optional[float], settings: dict
) -> Optional[str]:
  """Return a reason string when *signal* (a LONG/SHORT entry) is too stale to
  execute at *live_price*, else ``None``.

  A market order always fills at the live quote, never at the price the signal
  was built on. When the market has already run past the signal's own targets,
  filling it opens a position that is broken from the first tick:

  * **Past TP2** — the position opens *beyond* its take-profit, so it is
    instantly in loss by at least the spread with no profit left to capture.
    (Downstream, ``StopValidator`` would silently rewrite the now-unreachable TP
    to just above the entry, hiding the problem rather than surfacing it.)
  * **Past SL** — the position opens at or beyond its stop, so it is stopped
    out effectively on entry.
  * **Drifted too far** — the market has not passed a level yet, but has moved
    enough that the trade no longer resembles the one the signal described. See
    :func:`_drift_rejection` for the units.

  *live_price* is the price the entry would actually fill at (ask for a LONG,
  bid for a SHORT on a quoted market; mark price on a CEX). ``None`` — no quote
  available — skips every check: a missing quote is a data-availability problem,
  and blocking entries on it would be a worse failure than letting the broker
  decide.
  """
  if live_price is None or live_price <= 0:
    return None

  is_long = signal.action == SignalActionEnum.LONG
  quote_label = "ask" if is_long else "bid"

  if signal.tp2:
    passed_tp = live_price >= signal.tp2 if is_long else live_price <= signal.tp2
    if passed_tp:
      return (
        f"{signal.symbol} {signal.action.value} rejected: the market has already "
        f"reached its take-profit — {quote_label} is {live_price} and tp2 is "
        f"{signal.tp2}. Opening now would put the entry "
        f"{'above' if is_long else 'below'} its own target, so the position "
        f"would start in loss with no profit left to take."
      )

  if signal.sl:
    passed_sl = live_price <= signal.sl if is_long else live_price >= signal.sl
    if passed_sl:
      return (
        f"{signal.symbol} {signal.action.value} rejected: the market has already "
        f"reached its stop-loss — {quote_label} is {live_price} and sl is "
        f"{signal.sl}. Opening now would put the entry at or past its own stop, "
        f"so the position would be stopped out immediately."
      )

  return _drift_rejection(signal, live_price, settings, quote_label)
