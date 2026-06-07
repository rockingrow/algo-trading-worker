"""
worker/core/market_strategy.py
──────────────────────────────
Market abstraction layer.

Architecture
────────────
  BaseMarketStrategy     Abstract interface every market must implement.
  ExecutorBackedMarket   Shared concrete logic for any market that drives an
                         executor satisfying ``TradeExecutorProtocol``.
  ForexMarket            FOREX implementation backed by MT5Executor.
  CryptoMarket           CRYPTO implementation backed by CryptoExecutor.
  MarketStrategyFactory  Factory that reads ``settings.market_type`` and
                         returns the correct concrete strategy.

SignalHandler consumes *only* the BaseMarketStrategy interface, keeping it fully
decoupled from any broker's internals. The entry / TP1 / full-close logic is the
same regardless of broker — it only ever calls the executor protocol — so it
lives once in :class:`ExecutorBackedMarket`; ``ForexMarket`` and ``CryptoMarket``
just bind a concrete executor.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from worker.core.config import ExecutionConfig
from worker.interfaces.executor_protocol import TradeExecutorProtocol
from worker.logger import get_logger
from worker.schemas.metatrader_schema import TradeResult
from worker.schemas.signal_schema import SignalSchema
from worker.settings import MarketTypeEnum

logger = get_logger("worker.core.market_strategy")


# ──────────────────────────────────────────────────────────────────────────── #
#  Abstract Base                                                                #
# ──────────────────────────────────────────────────────────────────────────── #


class BaseMarketStrategy(ABC):
  """
  Contract that all market implementations must satisfy.

  Every method maps directly to one of SignalHandler's action groups:

    entry             →  Group 1 (LONG / SHORT — direction encoded in signal.action)
    handle_tp1        →  Group 2 (partial close + breakeven SL)
    handle_full_close →  Group 3 (TP2 / SL / R_SL / FLAT full exits)

  Position helpers (get_open_positions, close_all_positions) are used
  by SignalHandler for pre-flight checks and stale cleanup.
  """

  # ── Entry ────────────────────────────────────────────────────────────── #

  @abstractmethod
  def entry(self, signal: SignalSchema) -> TradeResult:
    """Open a new LONG (BUY) or SHORT (SELL) position — direction in signal.action."""

  # ── Group 2: TP1 ─────────────────────────────────────────────────────── #

  @abstractmethod
  def handle_tp1(self, signal: SignalSchema) -> TradeResult:
    """Partial close + move SL to breakeven."""

  # ── Group 3: Full exits ───────────────────────────────────────────────── #

  @abstractmethod
  def handle_full_close(self, signal: SignalSchema) -> TradeResult:
    """Close all remaining volume. ``signal.action`` carries the exit reason."""

  # ── Position helpers ──────────────────────────────────────────────────── #

  @abstractmethod
  def get_open_positions(
    self, symbol: str, strategy: Optional[str] = None
  ) -> List[Any]:
    """Return open positions for *symbol*.

    When *strategy* is given, only positions belonging to that strategy are
    returned, so strategies sharing a symbol stay isolated.
    """

  @abstractmethod
  def close_all_positions(
    self, symbol: str, reason: str = "CLOSE", strategy: Optional[str] = None
  ) -> TradeResult:
    """Force-close positions for *symbol*.

    When *strategy* is given, only that strategy's positions are closed.
    """


# ──────────────────────────────────────────────────────────────────────────── #
#  ExecutorBackedMarket — shared logic over any TradeExecutorProtocol           #
# ──────────────────────────────────────────────────────────────────────────── #


class ExecutorBackedMarket(BaseMarketStrategy):
  """
  Concrete strategy logic shared by every executor-driven market.

  Delegates all broker communication to the injected *executor* so this class
  stays free of any broker's raw API and is easy to unit-test. The executor is
  accepted as :class:`TradeExecutorProtocol` (duck-typed) so tests can inject
  any compatible fake.
  """

  def __init__(
    self, executor: TradeExecutorProtocol, config: ExecutionConfig
  ) -> None:
    self._executor = executor
    self._config = config

  # ── Entry ────────────────────────────────────────────────────────────── #

  def entry(self, signal: SignalSchema) -> TradeResult:
    """Open a BUY or SELL market order — direction encoded in signal.action."""
    return self._executor.open_position(signal)

  # ── TP1 ──────────────────────────────────────────────────────────────── #

  def handle_tp1(self, signal: SignalSchema) -> TradeResult:
    """
    Partial close at TP1 then move SL to position entry (breakeven).

    Steps delegated to executor:
      1. Derive close volume (% of position or signal quantity).
      2. partial_close_position
      3. update_position_sl to breakeven (pos.price_open)
    """
    symbol = signal.symbol
    positions = self._executor.get_open_positions(symbol, strategy=signal.strategy)

    if not positions:
      return TradeResult.fail("No open position — likely SL already triggered.")

    pos = positions[0]

    if self._config.volume_decision_enabled:
      calculated_volume = pos.volume * (self._config.position_tp1_percent / 100)
      close_volume = self._executor.normalize_volume(symbol, calculated_volume)
      logger.info(
        f"[handle_tp1] VOLUME_DECISION mode | "
        f"position_volume={pos.volume} position_tp1_percent={self._config.position_tp1_percent}% "
        f"calculated={calculated_volume} → close_volume={close_volume}"
      )
    else:
      if signal.quantity is None:
        return TradeResult.fail("Missing quantity in TP1 signal")
      close_volume = self._executor.convert_quantity_to_lots(symbol, signal.quantity)
      logger.info(
        f"[handle_tp1] Payload quantity mode | qty={signal.quantity} → close_volume={close_volume}"
      )

    close_result = self._executor.partial_close_position(
      symbol=symbol,
      close_volume=close_volume,
      position_ticket=pos.ticket,
      strategy=signal.strategy,
    )

    if not close_result.get("success"):
      return close_result

    sl_result = self._executor.update_position_sl(
      symbol=symbol,
      new_sl=pos.price_open,
      position_ticket=pos.ticket,
      strategy=signal.strategy,
    )

    close_result["sl_update"] = sl_result
    return close_result

  # ── Group 3 ──────────────────────────────────────────────────────────── #

  def handle_full_close(self, signal: SignalSchema) -> TradeResult:
    """Full close using actual broker volume; reason derived from signal.action."""
    return self._executor.close_all_positions(
      signal.symbol, reason=signal.action.value, strategy=signal.strategy
    )

  # ── Helpers ──────────────────────────────────────────────────────────── #

  def get_open_positions(
    self, symbol: str, strategy: Optional[str] = None
  ) -> List[Any]:
    return self._executor.get_open_positions(symbol, strategy=strategy)

  def close_all_positions(
    self, symbol: str, reason: str = "CLOSE", strategy: Optional[str] = None
  ) -> TradeResult:
    return self._executor.close_all_positions(symbol, reason=reason, strategy=strategy)


# ──────────────────────────────────────────────────────────────────────────── #
#  Concrete markets                                                             #
# ──────────────────────────────────────────────────────────────────────────── #


class ForexMarket(ExecutorBackedMarket):
  """FOREX / CFD market via MetaTrader 5 (backed by ``MT5Executor``)."""


class CryptoMarket(ExecutorBackedMarket):
  """CRYPTO market via a centralized exchange (backed by ``CryptoExecutor``)."""


# ──────────────────────────────────────────────────────────────────────────── #
#  Factory                                                                      #
# ──────────────────────────────────────────────────────────────────────────── #


class MarketStrategyFactory:
  """
  Maps a :class:`MarketTypeEnum` to its :class:`BaseMarketStrategy` implementation.

  ``market_type`` is passed in explicitly (Dependency Injection) rather than read
  from the global ``settings`` singleton, so the factory has no hidden dependency
  and is trivially testable.

  Usage (in the signal processor)::

      strategy = MarketStrategyFactory.create(market_type, executor, config)
      handler  = SignalHandler(strategy, db_service)
  """

  _MARKET_CLASSES = {
    MarketTypeEnum.FOREX: ForexMarket,
    MarketTypeEnum.CRYPTO: CryptoMarket,
  }

  @staticmethod
  def create(
    market_type, executor=None, config: Optional[ExecutionConfig] = None
  ) -> BaseMarketStrategy:
    """
    Instantiate and return the correct market strategy.

    Parameters
    ----------
    market_type:
        A :class:`MarketTypeEnum` (or its string value) selecting the market.
    executor:
        The broker executor. Must satisfy :class:`TradeExecutorProtocol`.
        Required for both FOREX (``MT5Executor``) and CRYPTO (``CryptoExecutor``).
    config:
        Execution/risk configuration. Required for every market.
    """
    market_type = (
      market_type
      if isinstance(market_type, MarketTypeEnum)
      else MarketTypeEnum(market_type)
    )
    logger.info(f"[MarketStrategyFactory] market_type={market_type.value}")

    market_cls = MarketStrategyFactory._MARKET_CLASSES.get(market_type)
    if market_cls is None:
      raise ValueError(
        f"Unsupported or not-yet-implemented market_type: {market_type!r}"
      )

    if executor is None:
      raise ValueError(
        f"MarketStrategyFactory: executor must be provided for {market_type.value} market."
      )
    if config is None:
      raise ValueError(
        f"MarketStrategyFactory: config must be provided for {market_type.value} market."
      )

    strategy = market_cls(executor=executor, config=config)
    logger.info("[MarketStrategyFactory] %s strategy loaded.", market_cls.__name__)
    return strategy
