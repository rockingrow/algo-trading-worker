"""
worker/core/market_strategy.py
──────────────────────────────
Market abstraction layer.

Architecture
────────────
  BaseMarketStrategy   Abstract interface every market must implement.
  ForexMarket          Production implementation backed by MT5Executor.
  CryptoMarket         Placeholder — TBD.
  MarketStrategyFactory  Factory that reads ``settings.market_type`` and
                         returns the correct concrete strategy.

SignalHandler consumes *only* the BaseMarketStrategy interface, keeping
it fully decoupled from MT5 internals.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from worker.logger import get_logger
from worker.schemas.broker_schema import SignalSchema
from worker.settings import MarketTypeEnum, settings

logger = get_logger("worker.core.market_strategy")


# ──────────────────────────────────────────────────────────────────────────── #
#  Abstract Base                                                                #
# ──────────────────────────────────────────────────────────────────────────── #


class BaseMarketStrategy(ABC):
  """
  Contract that all market implementations must satisfy.

  Every method maps directly to one of SignalHandler's action groups:

    entry_long / entry_short  →  Group 1 (LONG / SHORT)
    handle_tp1                →  Group 2 (partial close + breakeven SL)
    handle_tp2                →  Group 3 – TP2 full exit
    handle_sl                 →  Group 3 – hard SL triggered
    handle_r_sl               →  Group 3 – reverse SL (trailing stop exit)

  Position helpers (get_open_positions, close_all_positions) are used
  by SignalHandler for pre-flight checks and stale cleanup.
  """

  # ── Entry ────────────────────────────────────────────────────────────── #

  @abstractmethod
  def entry_long(self, signal: SignalSchema) -> Dict[str, Any]:
    """Open a new LONG (BUY) position."""

  @abstractmethod
  def entry_short(self, signal: SignalSchema) -> Dict[str, Any]:
    """Open a new SHORT (SELL) position."""

  # ── Group 2: TP1 ─────────────────────────────────────────────────────── #

  @abstractmethod
  def handle_tp1(self, signal: SignalSchema) -> Dict[str, Any]:
    """Partial close + move SL to breakeven."""

  # ── Group 3: Full exits ───────────────────────────────────────────────── #

  @abstractmethod
  def handle_tp2(self, signal: SignalSchema) -> Dict[str, Any]:
    """Close all remaining volume at TP2."""

  @abstractmethod
  def handle_sl(self, signal: SignalSchema) -> Dict[str, Any]:
    """Close all remaining volume because hard SL was hit."""

  @abstractmethod
  def handle_r_sl(self, signal: SignalSchema) -> Dict[str, Any]:
    """Close all remaining volume because trailing/reverse SL was hit."""

  # ── Position helpers ──────────────────────────────────────────────────── #

  @abstractmethod
  def get_open_positions(self, symbol: str) -> List[Any]:
    """Return all open positions for *symbol* (filtered to this strategy's scope)."""

  @abstractmethod
  def close_all_positions(self, symbol: str, reason: str = "CLOSE") -> Dict[str, Any]:
    """Force-close ALL positions for *symbol*."""

  @abstractmethod
  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float:
    """Convert a webhook signal quantity (units) to broker lot size."""


# ──────────────────────────────────────────────────────────────────────────── #
#  ForexMarket — backed by MT5Executor                                         #
# ──────────────────────────────────────────────────────────────────────────── #


class ForexMarket(BaseMarketStrategy):
  """
  Concrete implementation for Forex/CFD markets via MetaTrader 5.

  Delegates all broker communication to the injected :class:`MT5Executor`
  so that ForexMarket itself remains free of raw ``mt5.*`` calls and is
  easier to unit-test.
  """

  def __init__(self, executor) -> None:
    # Import here to avoid circular deps and keep CryptoMarket importable
    # without MetaTrader5 installed.
    from worker.mt5.executor import MT5Executor

    if not isinstance(executor, MT5Executor):
      raise TypeError(f"ForexMarket expects MT5Executor, got {type(executor)}")
    self._executor = executor

  # ── Entry ────────────────────────────────────────────────────────────── #

  def entry_long(self, signal: SignalSchema) -> Dict[str, Any]:
    """Open a BUY market order via MT5."""
    return self._executor.open_position(signal)

  def entry_short(self, signal: SignalSchema) -> Dict[str, Any]:
    """Open a SELL market order via MT5."""
    return self._executor.open_position(signal)

  # ── TP1 ──────────────────────────────────────────────────────────────── #

  def handle_tp1(self, signal: SignalSchema) -> Dict[str, Any]:
    """
    Partial close at TP1 price then move SL to position entry (breakeven).

    Steps delegated to MT5Executor:
      1. convert qty → lots
      2. partial_close_position
      3. update_position_sl to breakeven
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

    if signal.quantity is None:
      return {
        "success": False,
        "retcode": -1,
        "comment": "Missing quantity in TP1 signal",
      }

    close_volume = self._executor.convert_quantity_to_lots(symbol, signal.quantity)

    close_result = self._executor.partial_close_position(
      symbol=symbol,
      close_volume=close_volume,
      position_ticket=pos.ticket,
    )

    if not close_result.get("success"):
      return close_result

    # Move SL to breakeven (entry price of the original position)
    sl_result = self._executor.update_position_sl(
      symbol=symbol,
      new_sl=pos.price_open,
      position_ticket=pos.ticket,
    )

    close_result["sl_update"] = sl_result
    return close_result

  # ── Group 3 ──────────────────────────────────────────────────────────── #

  def handle_tp2(self, signal: SignalSchema) -> Dict[str, Any]:
    """Full close at TP2 using actual MT5 volume."""
    return self._executor.close_all_positions(signal.symbol, reason="TP2")

  def handle_sl(self, signal: SignalSchema) -> Dict[str, Any]:
    """Full close because hard SL was triggered."""
    return self._executor.close_all_positions(signal.symbol, reason="SL")

  def handle_r_sl(self, signal: SignalSchema) -> Dict[str, Any]:
    """Full close because reverse/trailing SL was triggered."""
    return self._executor.close_all_positions(signal.symbol, reason="R_SL")

  # ── Helpers ──────────────────────────────────────────────────────────── #

  def get_open_positions(self, symbol: str) -> List[Any]:
    return self._executor.get_open_positions(symbol)

  def close_all_positions(self, symbol: str, reason: str = "CLOSE") -> Dict[str, Any]:
    return self._executor.close_all_positions(symbol, reason=reason)

  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float:
    return self._executor.convert_quantity_to_lots(symbol, quantity)


# ──────────────────────────────────────────────────────────────────────────── #
#  CryptoMarket — TBD placeholder                                              #
# ──────────────────────────────────────────────────────────────────────────── #


class CryptoMarket(BaseMarketStrategy):
  """
  Placeholder for a future crypto exchange integration (e.g. Binance, Bybit).

  All methods raise ``NotImplementedError`` until a real implementation is
  wired in. The class is intentionally importable without any exchange SDK
  installed so that the factory and settings validation still work.
  """

  def __init__(self) -> None:
    logger.warning(
      "[CryptoMarket] This market implementation is a placeholder and not yet functional. "
      "All trade calls will raise NotImplementedError."
    )

  def entry_long(self, signal: SignalSchema) -> Dict[str, Any]:
    raise NotImplementedError("CryptoMarket.entry_long is not yet implemented.")

  def entry_short(self, signal: SignalSchema) -> Dict[str, Any]:
    raise NotImplementedError("CryptoMarket.entry_short is not yet implemented.")

  def handle_tp1(self, signal: SignalSchema) -> Dict[str, Any]:
    raise NotImplementedError("CryptoMarket.handle_tp1 is not yet implemented.")

  def handle_tp2(self, signal: SignalSchema) -> Dict[str, Any]:
    raise NotImplementedError("CryptoMarket.handle_tp2 is not yet implemented.")

  def handle_sl(self, signal: SignalSchema) -> Dict[str, Any]:
    raise NotImplementedError("CryptoMarket.handle_sl is not yet implemented.")

  def handle_r_sl(self, signal: SignalSchema) -> Dict[str, Any]:
    raise NotImplementedError("CryptoMarket.handle_r_sl is not yet implemented.")

  def get_open_positions(self, symbol: str) -> List[Any]:
    raise NotImplementedError("CryptoMarket.get_open_positions is not yet implemented.")

  def close_all_positions(self, symbol: str, reason: str = "CLOSE") -> Dict[str, Any]:
    raise NotImplementedError(
      "CryptoMarket.close_all_positions is not yet implemented."
    )

  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float:
    raise NotImplementedError(
      "CryptoMarket.convert_quantity_to_lots is not yet implemented."
    )


# ──────────────────────────────────────────────────────────────────────────── #
#  Factory                                                                      #
# ──────────────────────────────────────────────────────────────────────────── #


class MarketStrategyFactory:
  """
  Reads ``settings.market_type`` (a :class:`MarketTypeEnum`) and returns the
  matching :class:`BaseMarketStrategy` implementation.

  Usage (in app.py lifespan)::

      strategy = MarketStrategyFactory.create(executor=executor)
      handler  = SignalHandler(strategy)
  """

  @staticmethod
  def create(executor=None) -> BaseMarketStrategy:
    """
    Instantiate and return the correct market strategy.

    Parameters
    ----------
    executor:
        Only required when ``market_type`` is ``FOREX``.
        Should be an :class:`~worker.mt5.executor.MT5Executor` instance.
    """
    market_type = settings.market_type
    logger.info(f"[MarketStrategyFactory] Detected market_type={market_type.value}")

    if market_type == MarketTypeEnum.FOREX:
      if executor is None:
        raise ValueError(
          "MarketStrategyFactory: executor must be provided for FOREX market."
        )
      strategy = ForexMarket(executor=executor)
      logger.info("[MarketStrategyFactory] ForexMarket strategy loaded.")
      return strategy

    if market_type == MarketTypeEnum.CRYPTO:
      strategy = CryptoMarket()
      logger.info("[MarketStrategyFactory] CryptoMarket strategy loaded (placeholder).")
      return strategy

    raise ValueError(f"Unsupported market_type: {market_type!r}")
