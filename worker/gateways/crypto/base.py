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

from worker.schemas.trade_result import TradeResult

# Reuse the broker-neutral side vocabulary the rest of the worker speaks.
SIDE_LONG = "LONG"
SIDE_SHORT = "SHORT"

# Marker prepended to the clientOrderId of every reduce-only close the worker
# places itself. The exchange event parser uses it to tell the worker's own
# closes apart from an external close (web UI / mobile / third-party / liquidation),
# which are otherwise an identical MARKET reduce-only fill. Kept here (agnostic
# layer) so both the executor and a concrete exchange's event stream share one
# source of truth without the executor importing a concrete exchange module.
WORKER_ORDER_PREFIX = "awkr_"


@dataclass
class SymbolFilter:
  """Trading rules for a symbol, used to round quantities/prices to valid steps."""

  step_size: float = 0.0  # quantity increment (LOT_SIZE.stepSize)
  min_qty: float = 0.0  # minimum order quantity
  tick_size: float = 0.0  # price increment (PRICE_FILTER.tickSize)
  min_notional: float = (
    0.0  # minimum order notional (MIN_NOTIONAL/NOTIONAL.minNotional)
  )


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

  Order-placement methods return a :class:`~worker.schemas.trade_result.TradeResult`.
  """

  name: str = "BASE"

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  @abstractmethod
  def connect(self) -> bool:
    """Verify connectivity/credentials. Return True if the gateway is usable."""

  def close(self) -> None:  # noqa: B027 - optional hook, not all gateways need it
    """Release any held resources (HTTP sessions, sockets)."""

  def set_position_mode(self, hedge_mode: bool) -> bool:
    """Reconcile the account's position mode to *hedge_mode* on the exchange.

    ``True`` → Hedge (orders carry an explicit ``positionSide``); ``False`` →
    One-way (netting, direction inferred from ``side``). Called once after
    :meth:`connect` so the exchange-side setting always matches the payload
    convention this gateway sends, instead of relying on the operator to have
    flipped it by hand (a mismatch is what produces Binance -4061).

    Returns ``True`` when the account is in the requested mode after the call
    (including when it was already there). Default no-op returning ``True`` for
    exchanges without a switchable position mode. Implementations must treat this
    as best-effort — never raise — so a failed switch degrades to the account's
    current mode rather than aborting startup.
    """
    return True

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
  ) -> TradeResult:
    """Place a market order. *side* is SIDE_LONG/SIDE_SHORT (mapped to BUY/SELL)."""

  @abstractmethod
  def set_stop_loss(
    self, symbol: str, position_side: str, stop_price: float, quantity: float
  ) -> TradeResult:
    """Place / replace a reduce-only stop order protecting an open position."""

  @abstractmethod
  def set_take_profit(
    self, symbol: str, position_side: str, tp_price: float, quantity: float
  ) -> TradeResult:
    """Place / replace a reduce-only take-profit order for an open position."""

  @abstractmethod
  def cancel_all_orders(self, symbol: str) -> None:
    """Cancel all open (e.g. resting stop) orders for *symbol*."""

  # ── Leverage ──────────────────────────────────────────────────────────── #

  def get_max_leverage(self, symbol: str) -> Optional[int]:
    """Return the maximum leverage for *symbol*, or ``None`` if the exchange does
    not expose a per-symbol cap. This is the symbol's market-wide ceiling;
    account-level restrictions (sub-account / VIP tier) may not be reflected and
    can only be discovered when :meth:`set_leverage` is rejected — implementations
    should detect and honor that lower cap there."""
    return None

  def set_leverage(
    self, symbol: str, leverage: int, min_leverage_cap: Optional[int] = None
  ) -> Optional[int]:
    """Set the working leverage for *symbol*. Returns the leverage **actually
    applied** on success — which may be below *leverage* if an account-level cap
    forces it lower — or ``None`` on failure. ``min_leverage_cap`` is an optional
    last-resort floor to retry at when an account-cap rejection cannot be parsed
    for its real ceiling; implementations that cannot detect such caps ignore it.
    Default no-op for exchanges where leverage is not API-configurable."""
    return None

  # ── Account ───────────────────────────────────────────────────────────── #

  @abstractmethod
  def get_account(self) -> Optional[Dict[str, Any]]:
    """Return an account snapshot (balance, equity/leverage where available)."""

  def get_account_footer(self, account_id: Optional[str] = None) -> str:
    """Human-readable account footer appended to notifications."""
    info = self.get_account() or {}
    balance = info.get("balance")
    account_line = f"<b>Account ID:</b> {account_id}\n" if account_id else ""
    return (
      f"\n<b>Exchange:</b> {self.name}\n"
      f"{account_line}"
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
