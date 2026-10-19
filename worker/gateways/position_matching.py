"""
worker/gateways/position_matching.py
────────────────────────────────────
Matching a broker position against a tracked position reference.

The reference travels as TEXT through SQLite (``positions.ref_source_id``) but
each gateway hands its positions back with a native ticket type — an ``int``
on MT5, an exchange order id (often a string) on a CEX. Comparing them
directly silently never matches, which would turn a ticket-scoped exit into a
"no position found" failure, so the comparison is normalised in one place and
both the market layer and the executors use it.

Only needed since one strategy can hold several positions on a symbol
(FOREX_ALLOW_MULTI_POSITIONS_PER_SYMBOL): with a single position per
(strategy, symbol) there was nothing to tell apart.
"""

from __future__ import annotations

from typing import Any, List, Optional


def same_ticket(candidate: Any, ticket: Any) -> bool:
  """True when *candidate* and *ticket* name the same broker position.

  Compared as text so an ``int`` MT5 ticket matches the TEXT ``ref_source_id``
  SQLite handed back.
  """
  if candidate is None or ticket is None:
    return False
  return str(candidate).strip() == str(ticket).strip()


def filter_by_ticket(positions: List[Any], ticket: Optional[Any]) -> List[Any]:
  """Narrow *positions* to the one carrying *ticket*.

  ``None`` — no specific position was resolved — returns the list untouched,
  which is what every caller wants when only one position can exist for the
  (strategy, symbol) being addressed.
  """
  if ticket is None:
    return positions
  return [p for p in positions if same_ticket(getattr(p, "ticket", None), ticket)]
