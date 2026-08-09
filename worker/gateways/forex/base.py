"""
worker/gateways/forex/base.py
─────────────────────────────
Platform-agnostic FOREX gateway abstraction.

Every forex trading platform (MetaTrader 5 today; a future MT6 beside it) is
integrated by implementing :class:`BasePlatformGateway`. Nothing above this layer
(:class:`~worker.gateways.forex.executor.ForexExecutor`, the signal processor)
imports a concrete platform — they depend only on this contract, which is the
whole point of the factory pattern: swapping or adding a platform never touches
business logic.

Mirrors :mod:`worker.gateways.crypto.base` (``BaseExchangeGateway`` +
``SymbolFilter`` / ``ExchangePosition``); the value types below are the forex
counterparts a CEX does not need (MT5 lots/points/contract-size vs. exchange
step/tick filters).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from worker.schemas.trade_result import TradeResult

# Broker-neutral side vocabulary the rest of the worker speaks.
SIDE_LONG = "LONG"
SIDE_SHORT = "SHORT"


@dataclass
class SymbolSpec:
  """Per-symbol trading rules, read once from the platform and consumed by the
  agnostic lot-sizing / stop-validation math. Mirrors every ``symbol_info`` field
  the FOREX sizing logic needs."""

  resolved_name: str
  point: float
  digits: int
  volume_min: float
  volume_step: float
  volume_max: float
  tick_value: float  # value of one tick for one lot (MT5 trade_tick_value)
  tick_size: float  # price increment (MT5 trade_tick_size)
  contract_size: float  # units per lot (MT5 trade_contract_size)
  stops_level: int  # minimum stop distance in points (MT5 trade_stops_level)


@dataclass
class Tick:
  """Current best bid/ask for a symbol."""

  bid: float
  ask: float


@dataclass
class PlatformPosition:
  """A live position on the platform.

  Exposes the ``volume`` / ``price_open`` attributes the broker-neutral
  market-strategy logic reads (see
  :meth:`~worker.gateways.market_strategy.ExecutorBackedMarket.handle_tp1`).
  ``side`` is SIDE_LONG / SIDE_SHORT; the concrete gateway maps it back to the
  platform's own buy/sell encoding when closing.
  """

  ticket: int
  magic: int
  side: str  # SIDE_LONG / SIDE_SHORT
  volume: float
  price_open: float
  sl: float = 0.0
  tp: float = 0.0
  symbol: str = ""  # resolved platform symbol name


class BasePlatformGateway(ABC):
  """Contract every forex-platform adapter must implement.

  Order methods return a :class:`~worker.schemas.trade_result.TradeResult`.
  Symbol arguments are the *resolved* platform symbol (callers resolve via
  :meth:`resolve_symbol` first), matching how ``MT5Executor`` worked.
  """

  name: str = "BASE"

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  @abstractmethod
  def connect(self) -> bool:
    """Establish the platform connection. Return True if usable."""

  def close(self) -> None:  # noqa: B027 - optional hook
    """Release the platform connection."""

  @abstractmethod
  def is_connected(self) -> bool:
    """Return True if the platform is currently reachable."""

  @abstractmethod
  def reconnect(self, max_attempts: int = 0, delay_seconds: float = 10.0) -> bool:
    """Reconnect with retries (``max_attempts=0`` → unlimited)."""

  def restart_terminal(self, startup_wait: float = 15.0) -> bool:  # pragma: no cover
    """Hard-restart the platform's terminal process where applicable.

    Default no-op (returns False) for platforms with no local terminal.
    """
    return False

  # ── Account ───────────────────────────────────────────────────────────── #

  @abstractmethod
  def get_account(self) -> Optional[Dict[str, Any]]:
    """Account snapshot (must include ``balance`` / ``equity`` where available)."""

  @abstractmethod
  def get_account_footer(self) -> str:
    """Human-readable account footer appended to notifications."""

  # ── Market data / rules ───────────────────────────────────────────────── #

  @abstractmethod
  def resolve_symbol(self, base_symbol: str) -> str:
    """Resolve a base symbol (e.g. ``XAUUSD``) to the tradeable platform name."""

  @abstractmethod
  def get_symbol_spec(self, symbol: str) -> Optional[SymbolSpec]:
    """Return the trading rules for *symbol* (already resolved)."""

  @abstractmethod
  def get_tick(self, symbol: str) -> Optional[Tick]:
    """Return the current bid/ask for *symbol* (already resolved)."""

  # ── Positions ─────────────────────────────────────────────────────────── #

  @abstractmethod
  def get_positions(self, symbol: Optional[str] = None) -> List[PlatformPosition]:
    """Return live positions, optionally filtered to *symbol*."""

  # ── Orders ────────────────────────────────────────────────────────────── #

  @abstractmethod
  def place_order(
    self,
    symbol: str,
    side: str,
    volume: float,
    price: float,
    sl: Optional[float],
    tp: Optional[float],
    magic: int,
    comment: str,
  ) -> TradeResult:
    """Open a market order. *side* is SIDE_LONG/SIDE_SHORT."""

  @abstractmethod
  def close_position(
    self,
    position: PlatformPosition,
    volume: Optional[float] = None,
    comment: str = "Close",
  ) -> TradeResult:
    """Close (or partially close) *position* with a counter-direction market order.

    *volume* defaults to the position's full volume; pass a smaller value for a
    partial close.
    """

  @abstractmethod
  def modify_sl(self, position: PlatformPosition, new_sl: float) -> TradeResult:
    """Move the stop-loss of *position* to *new_sl* (preserving its TP)."""

  # ── Event ingestion ───────────────────────────────────────────────────── #

  def create_close_detection_job(
    self, magic_numbers, db_service, notifier
  ):  # pragma: no cover - default no-op
    """Return a startable job that detects platform-side closes (terminal SL/TP/
    stop-out/manual), or ``None`` when the platform pushes such events instead.

    The returned object must expose ``start(stop_event=...)``.
    """
    return None
