"""
worker/mt5/close_detector.py
────────────────────────────
Scans MT5 deal history to detect positions closed server-side (SL, TP, stop-out,
or manual) that were not triggered by the ZMQ signal pipeline.

The caller maintains a `seen_tickets` set that is mutated in-place each scan so
the job can detect disappearances across successive polls without any shared state
of its own.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Set

import MetaTrader5 as mt5

from worker.logger import get_logger

log = get_logger("worker.mt5.close_detector")


# ── Close-reason enum ─────────────────────────────────────────────────────── #


class TerminalCloseReason(str, Enum):
  """Reason codes for positions closed by the MT5 terminal without a broker signal."""

  SL = "SL"
  TP = "TP"
  STOP_OUT = "STOP_OUT"
  MANUAL = "MANUAL"


# Map MT5 deal-reason codes → our enum.
# DEAL_REASON_EXPERT (3) is intentionally absent: that is our own order_send
# closing the position via the ZMQ pipeline — we must not double-fire.
_REASON_MAP: dict[int, TerminalCloseReason] = {}


def _build_reason_map() -> None:
  """Populate _REASON_MAP lazily (mt5 constants are only valid after import)."""
  global _REASON_MAP
  _REASON_MAP = {
    mt5.DEAL_REASON_SL: TerminalCloseReason.SL,
    mt5.DEAL_REASON_TP: TerminalCloseReason.TP,
    mt5.DEAL_REASON_SO: TerminalCloseReason.STOP_OUT,
    mt5.DEAL_REASON_CLIENT: TerminalCloseReason.MANUAL,
    mt5.DEAL_REASON_MOBILE: TerminalCloseReason.MANUAL,
    mt5.DEAL_REASON_WEB: TerminalCloseReason.MANUAL,
  }


# ── Data transfer objects ─────────────────────────────────────────────────── #


@dataclass
class AccountSnapshot:
  """Point-in-time snapshot of MT5 account state captured at position-close detection."""

  login: int
  name: str
  balance: float
  equity: float
  leverage: int
  margin: float
  free_margin: float
  margin_level: float
  server: str


@dataclass
class TerminalClosedEvent:
  """Event emitted when the MT5 terminal closes a position without a broker-side signal."""

  # Position identity
  source_ticket: int  # position ticket (= source_ticket in our system)
  deal_ticket: int  # the closing deal ticket
  symbol: str
  # Close details
  close_reason: TerminalCloseReason
  close_price: float
  close_volume: float
  close_time: datetime
  # Entry details
  entry_price: float
  # Order parameters captured at entry time
  sl: Optional[float]
  tp: Optional[float]  # server-side TP (= tp1 price level in our system)
  # Account snapshot at detection time
  account: Optional[AccountSnapshot]


# ── Internal helpers ──────────────────────────────────────────────────────── #


def _account_snapshot() -> Optional[AccountSnapshot]:
  info = mt5.account_info()
  if info is None:
    return None
  return AccountSnapshot(
    login=info.login,
    name=info.name,
    balance=info.balance,
    equity=info.equity,
    leverage=info.leverage,
    margin=info.margin,
    free_margin=info.margin_free,
    margin_level=info.margin_level,
    server=info.server,
  )


def _build_event(position_ticket: int) -> Optional[TerminalClosedEvent]:
  """
  Query deal history for a position that just disappeared from positions_get().
  Returns a TerminalClosedEvent only when the close was terminal-initiated.
  Returns None when closed by our own code (DEAL_REASON_EXPERT) or when
  history is unavailable.
  """
  if not _REASON_MAP:
    _build_reason_map()

  deals = mt5.history_deals_get(position=position_ticket)
  if not deals:
    log.warning("[close_detector] No deal history for position=%d", position_ticket)
    return None

  opening_deal = next((d for d in deals if d.entry == mt5.DEAL_ENTRY_IN), None)
  closing_deal = next((d for d in deals if d.entry == mt5.DEAL_ENTRY_OUT), None)

  if closing_deal is None:
    log.warning("[close_detector] No closing deal found for position=%d", position_ticket)
    return None

  reason = _REASON_MAP.get(closing_deal.reason)
  if reason is None:
    # Closed by our own code (DEAL_REASON_EXPERT) or unrecognised reason — skip.
    log.debug(
      "[close_detector] Position %d closed with deal_reason=%d — not terminal-initiated, skipping",
      position_ticket,
      closing_deal.reason,
    )
    return None

  # Retrieve SL/TP from the original opening order (deals don't carry SL/TP).
  orders = mt5.history_orders_get(position=position_ticket)
  opening_order = next(
    (o for o in (orders or []) if o.type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)),
    None,
  )

  return TerminalClosedEvent(
    source_ticket=position_ticket,
    deal_ticket=closing_deal.ticket,
    symbol=closing_deal.symbol,
    close_reason=reason,
    close_price=closing_deal.price,
    close_volume=closing_deal.volume,
    close_time=datetime.fromtimestamp(closing_deal.time),
    entry_price=opening_deal.price if opening_deal else 0.0,
    sl=opening_order.sl if opening_order else None,
    tp=opening_order.tp if opening_order else None,
    account=_account_snapshot(),
  )


# ── Public API ────────────────────────────────────────────────────────────── #


def scan_terminal_closed_positions(
  magic_numbers: Set[int],
  seen_tickets: Set[int],
) -> List[TerminalClosedEvent]:
  """
  Diff current open positions against *seen_tickets*.

  *magic_numbers* is the set of every magic this worker owns (base plus any
  per-strategy magics), so positions opened under a strategy's dedicated magic
  are tracked too.

  For every ticket that disappeared, query deal history to determine whether
  the close was terminal-initiated (SL, TP, stop-out, manual).  Only those
  cases produce a TerminalClosedEvent.

  *seen_tickets* is mutated in-place to reflect the current position set so
  successive calls naturally detect the next round of disappearances.

  If positions_get() returns None (terminal offline) the scan is skipped and
  *seen_tickets* is left untouched to prevent false-positive events.
  """
  positions = mt5.positions_get()
  if positions is None:
    log.warning(
      "[close_detector] positions_get() returned None — terminal offline, skipping scan"
    )
    return []

  current_tickets: Set[int] = {p.ticket for p in positions if p.magic in magic_numbers}
  disappeared = seen_tickets - current_tickets
  events: List[TerminalClosedEvent] = []

  for ticket in disappeared:
    event = _build_event(ticket)
    if event is not None:
      events.append(event)
      log.info(
        "[close_detector] Terminal close detected | position=%d reason=%s price=%.5f vol=%.2f",
        ticket,
        event.close_reason.value,
        event.close_price,
        event.close_volume,
      )

  new_tickets = current_tickets - seen_tickets
  if new_tickets:
    log.debug("[close_detector] New positions now tracked: %s", new_tickets)

  seen_tickets.clear()
  seen_tickets.update(current_tickets)

  return events
