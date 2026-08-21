"""
worker/gateways/forex/mt5/deal_pnl.py
─────────────────────────────────────
Realized-PnL arithmetic over MetaTrader 5 *deals*.

MT5 books the money a close made or lost on the deal that closed it, split over
four fields — the raw price result (``profit``) plus the costs the account was
charged for holding and transacting it (``commission``, ``swap``, ``fee``). What
an operator wants to read in a close notification is the net of all four: that is
the amount the account balance actually moved by.

Kept in its own module because two callers need the same arithmetic and neither
may import the other: :class:`~worker.gateways.forex.mt5.gateway.MT5Gateway`
(reading back the deal its own ``order_send`` produced) and
:mod:`worker.gateways.forex.mt5.close_detector` (which already holds the closing
deal it found in history, but imports the ``MetaTrader5`` extension at module
scope and so cannot be imported off-Windows).

Deals are read structurally (``getattr``) rather than by field access: the
callers pass whatever ``history_deals_get`` returned, and a broker/terminal
combination that omits one of the cost fields must degrade to "no charge" rather
than raise inside a notification path.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

#: Deal fields that sum to the net amount booked to the account balance.
_PNL_FIELDS = ("profit", "commission", "swap", "fee")


def deal_realized_pnl(deal: Any) -> float:
  """Net amount *deal* booked to the account: price result minus its costs."""
  total = 0.0
  for name in _PNL_FIELDS:
    value = getattr(deal, name, 0.0)
    if value is None:
      continue
    try:
      total += float(value)
    except (TypeError, ValueError):
      # A field the terminal reported as something non-numeric is treated as no
      # charge; a notification must never fail over a cost field.
      continue
  return total


def deals_realized_pnl(deals: Optional[Iterable[Any]]) -> Optional[float]:
  """Net realized PnL across *deals*, or ``None`` when there are none.

  ``None`` is deliberately distinct from ``0.0``: an empty (or unavailable) deal
  history means the amount is *unknown* and must be reported as such, whereas
  ``0.0`` is a genuine break-even close.
  """
  if not deals:
    return None
  return sum(deal_realized_pnl(deal) for deal in deals)
