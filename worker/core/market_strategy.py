"""
worker/core/market_strategy.py
──────────────────────────────
Market abstraction layer.

Architecture
────────────
  BaseMarketStrategy     Abstract interface every market must implement.
  ForexMarket            Production implementation backed by MT5Executor.
  MarketStrategyFactory  Factory that reads ``settings.market_type`` and
                         returns the correct concrete strategy.

SignalHandler consumes *only* the BaseMarketStrategy interface, keeping
it fully decoupled from MT5 internals.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from worker.core.config import ExecutionConfig
from worker.interfaces.mt5_executor_protocol import MT5ExecutorProtocol
from worker.logger import get_logger
from worker.schemas.metatrader_schema import TradeResult
from worker.schemas.signal_schema import SignalSchema
from worker.settings import MarketTypeEnum, settings

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
  def get_open_positions(self, symbol: str) -> List[Any]:
    """Return all open positions for *symbol* (filtered to this strategy's scope)."""

  @abstractmethod
  def close_all_positions(self, symbol: str, reason: str = "CLOSE") -> TradeResult:
    """Force-close ALL positions for *symbol*."""


# ──────────────────────────────────────────────────────────────────────────── #
#  ForexMarket — backed by MT5Executor                                         #
# ──────────────────────────────────────────────────────────────────────────── #


class ForexMarket(BaseMarketStrategy):
  """
  Concrete implementation for Forex/CFD markets via MetaTrader 5.

  Delegates all broker communication to the injected *executor* so that
  ForexMarket itself remains free of raw ``mt5.*`` calls and is
  easier to unit-test. The executor is accepted as
  :class:`~worker.mt5.protocol.MT5ExecutorProtocol` (duck-typed) so
  tests can inject any compatible mock without importing MetaTrader5.
  """

  def __init__(self, executor: MT5ExecutorProtocol, config: ExecutionConfig) -> None:
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
    positions = self._executor.get_open_positions(symbol)

    if not positions:
      return {
        "success": False,
        "retcode": -1,
        "comment": "No open position — likely SL already triggered.",
      }

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
        return {
          "success": False,
          "retcode": -1,
          "comment": "Missing quantity in TP1 signal",
        }
      close_volume = self._executor.convert_quantity_to_lots(symbol, signal.quantity)
      logger.info(
        f"[handle_tp1] Payload quantity mode | qty={signal.quantity} → close_volume={close_volume}"
      )

    close_result = self._executor.partial_close_position(
      symbol=symbol,
      close_volume=close_volume,
      position_ticket=pos.ticket,
    )

    if not close_result.get("success"):
      return close_result

    sl_result = self._executor.update_position_sl(
      symbol=symbol,
      new_sl=pos.price_open,
      position_ticket=pos.ticket,
    )

    close_result["sl_update"] = sl_result
    return close_result

  # ── Group 3 ──────────────────────────────────────────────────────────── #

  def handle_full_close(self, signal: SignalSchema) -> TradeResult:
    """Full close using actual MT5 volume; reason derived from signal.action."""
    return self._executor.close_all_positions(signal.symbol, reason=signal.action.value)

  # ── Helpers ──────────────────────────────────────────────────────────── #

  def get_open_positions(self, symbol: str) -> List[Any]:
    return self._executor.get_open_positions(symbol)

  def close_all_positions(self, symbol: str, reason: str = "CLOSE") -> TradeResult:
    return self._executor.close_all_positions(symbol, reason=reason)


# ──────────────────────────────────────────────────────────────────────────── #
#  Factory                                                                      #
# ──────────────────────────────────────────────────────────────────────────── #


class MarketStrategyFactory:
  """
  Reads ``settings.market_type`` (a :class:`MarketTypeEnum`) and returns the
  matching :class:`BaseMarketStrategy` implementation.

  Usage (in the signal processor)::

      strategy = MarketStrategyFactory.create(executor=executor, config=config)
      handler  = SignalHandler(strategy, db_service)
  """

  @staticmethod
  def create(
    executor=None, config: Optional[ExecutionConfig] = None
  ) -> BaseMarketStrategy:
    """
    Instantiate and return the correct market strategy.

    Parameters
    ----------
    executor:
        Required when ``market_type`` is ``FOREX``.
        Must satisfy :class:`~worker.mt5.protocol.MT5ExecutorProtocol`.
    config:
        Execution/risk configuration. Required for ``FOREX``.
    """
    market_type = settings.market_type
    logger.info(f"[MarketStrategyFactory] Detected market_type={market_type.value}")

    if market_type == MarketTypeEnum.FOREX:
      if executor is None:
        raise ValueError(
          "MarketStrategyFactory: executor must be provided for FOREX market."
        )
      if config is None:
        raise ValueError(
          "MarketStrategyFactory: config must be provided for FOREX market."
        )
      strategy = ForexMarket(executor=executor, config=config)
      logger.info("[MarketStrategyFactory] ForexMarket strategy loaded.")
      return strategy

    raise ValueError(
      f"Unsupported or not-yet-implemented market_type: {market_type!r}"
    )
