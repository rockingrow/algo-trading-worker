"""
worker/crypto/base.py
─────────────────────
CEX-agnostic exchange abstraction.

Every centralized exchange (Binance, and future additions) is integrated by
implementing :class:`BaseExchangeGateway`. Nothing above this layer
(``CryptoExecutor``, ``CryptoMarket``, the signal processor) imports a concrete
exchange — they depend only on this contract, which is the whole point of the
factory pattern: swapping or adding an exchange never touches business logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Reuse the broker-neutral side vocabulary the rest of the worker speaks.
SIDE_LONG = "LONG"
SIDE_SHORT = "SHORT"


@dataclass
class SymbolFilter:
  """Trading rules for a symbol, used to round quantities/prices to valid steps."""

  step_size: float = 0.0  # quantity increment (LOT_SIZE.stepSize)
  min_qty: float = 0.0  # minimum order quantity
  tick_size: float = 0.0  # price increment (PRICE_FILTER.tickSize)


@dataclass
class ExchangePosition:
  """A live position on the exchange.

  Exposes ``volume`` / ``price_open`` aliases so it is structurally compatible
  with the position objects the market-strategy logic reads (it was written
  against MT5 position attributes), keeping that logic broker-neutral.
  """

  symbol: str
  side: str  # SIDE_LONG / SIDE_SHORT
  quantity: float
  entry_price: float
  ticket: int  # synthetic identifier (exchange has no per-position ticket)
  tp: float = 0.0
  sl: float = 0.0
  magic: int = 0  # crypto has no magic; kept for structural parity
  unrealized_pnl: float = 0.0

  @property
  def volume(self) -> float:
    return abs(self.quantity)

  @property
  def price_open(self) -> float:
    return self.entry_price


class BaseExchangeGateway(ABC):
  """Contract every CEX adapter must implement.

  Order-placement methods return a normalized result dict shaped like the rest
  of the worker's ``TradeResult``::

      {"success": bool, "retcode": int, "ticket": int,
       "price": float, "volume": float, "comment": str}
  """

  name: str = "BASE"

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  @abstractmethod
  def connect(self) -> bool:
    """Verify connectivity/credentials. Return True if the gateway is usable."""

  def close(self) -> None:  # noqa: B027 - optional hook, not all gateways need it
    """Release any held resources (HTTP sessions, sockets)."""

  # ── Market data / rules ───────────────────────────────────────────────── #

  @abstractmethod
  def get_symbol_filter(self, symbol: str) -> SymbolFilter:
    """Return quantity/price step rules for *symbol*."""

  @abstractmethod
  def get_mark_price(self, symbol: str) -> float:
    """Return the current mark/last price for *symbol*."""

  # ── Positions ─────────────────────────────────────────────────────────── #

  @abstractmethod
  def get_positions(self, symbol: Optional[str] = None) -> List[ExchangePosition]:
    """Return all non-zero positions, optionally filtered to *symbol*."""

  # ── Orders ────────────────────────────────────────────────────────────── #

  @abstractmethod
  def place_market_order(
    self,
    symbol: str,
    side: str,
    quantity: float,
    reduce_only: bool = False,
    client_order_id: Optional[str] = None,
  ) -> Dict[str, Any]:
    """Place a market order. *side* is SIDE_LONG/SIDE_SHORT (mapped to BUY/SELL)."""

  @abstractmethod
  def set_stop_loss(
    self, symbol: str, position_side: str, stop_price: float, quantity: float
  ) -> Dict[str, Any]:
    """Place / replace a reduce-only stop order protecting an open position."""

  @abstractmethod
  def cancel_all_orders(self, symbol: str) -> None:
    """Cancel all open (e.g. resting stop) orders for *symbol*."""

  # ── Account ───────────────────────────────────────────────────────────── #

  @abstractmethod
  def get_account(self) -> Optional[Dict[str, Any]]:
    """Return an account snapshot (balance, equity/leverage where available)."""

  def get_account_footer(self) -> str:
    """Human-readable account footer appended to notifications."""
    info = self.get_account() or {}
    balance = info.get("balance")
    return (
      f"\n<b>Exchange:</b> {self.name}\n"
      f"<b>Balance:</b> {balance}\n"
      "----------------------------------"
    )

  # ── Event ingestion ───────────────────────────────────────────────────── #

  def create_event_stream(self, handler):  # pragma: no cover - default no-op
    """Return a startable event-ingestion job for exchange-side fills/exits.

    Each exchange owns the optimal mechanism (Binance → websocket user data
    stream). The returned object must expose ``start(stop_event)`` / ``stop()``.
    Returns ``None`` when the exchange has no push stream.
    """
    return None
