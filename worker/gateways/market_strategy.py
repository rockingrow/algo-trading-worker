"""
worker/gateways/market_strategy.py
────────────────────────────────────
Market abstraction layer.

Architecture
────────────
  BaseMarketStrategy     Abstract interface every market must implement.
  ExecutorBackedMarket   Shared concrete logic for any market that drives an
                         executor satisfying ``TradeExecutorProtocol``.
  ForexMarket            FOREX implementation backed by ForexExecutor.
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

from worker.gateways.config import ExecutionConfig
from worker.interfaces.executor_protocol import TradeExecutorProtocol
from worker.logger import get_logger
from worker.schemas.signal_schema import SignalSchema
from worker.schemas.trade_result import TradeResult
from worker.settings import MarketTypeEnum

logger = get_logger("worker.gateways.market_strategy")


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

  def entry_price(self, signal: SignalSchema) -> Optional[float]:
    """The price :meth:`entry` would fill *signal* at right now, or ``None``.

    Read by the staleness guard (``guard.stale_signal_rejection``) before an
    entry is sent, so it can compare the signal's own levels against the price
    the order would really get. Concrete (not abstract) with a ``None`` default
    — the guard treats that as "no quote available" and skips, so a market
    implementation or test double that predates the capability keeps working.
    """
    return None

  # ── Group 2: TP1 ─────────────────────────────────────────────────────── #

  @abstractmethod
  def handle_tp1(self, signal: SignalSchema) -> TradeResult:
    """Partial close, then optionally move SL to breakeven (see config)."""

  # ── Group 3: Full exits ───────────────────────────────────────────────── #

  @abstractmethod
  def handle_full_close(self, signal: SignalSchema) -> TradeResult:
    """Close all remaining volume. ``signal.action`` carries the exit reason."""

  # ── Capabilities ──────────────────────────────────────────────────────── #

  @property
  def allows_multi_strategy_per_symbol(self) -> bool:
    """Whether two different strategies may hold the same symbol at once.

    Concrete (not abstract) with a conservative ``False`` default so a market
    implementation only opts in when its broker can really keep the positions
    isolated. ``SignalHandler`` reads it to decide whether a second strategy
    entering an already-held symbol is a netting conflict or a legitimate
    parallel position.
    """
    return False

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

  def __init__(self, executor: TradeExecutorProtocol, config: ExecutionConfig) -> None:
    self._executor = executor
    self._config = config

  # ── Entry ────────────────────────────────────────────────────────────── #

  def entry(self, signal: SignalSchema) -> TradeResult:
    """Open a BUY or SELL market order — direction encoded in signal.action."""
    return self._executor.open_position(signal)

  def entry_price(self, signal: SignalSchema) -> Optional[float]:
    """Delegate the live entry quote to the executor (see the base docstring)."""
    getter = getattr(self._executor, "get_entry_price", None)
    if getter is None:
      return None
    return getter(signal)

  # ── TP1 ──────────────────────────────────────────────────────────────── #

  def _resolve_tp1_params(self, signal: SignalSchema) -> tuple[float, bool]:
    """Return (tp1_percent, move_sl_to_be).

    tp1_percent resolution:
      use_custom=True  → always use config.position_tp1_percent
      use_custom=False → signal.tp1_percent if present, else config.position_tp1_percent
    move_sl_to_be: config if set, else signal if set, else False.
    """
    if self._config.use_custom_position_tp1_percent or signal.tp1_percent is None:
      tp1_percent = self._config.position_tp1_percent
    else:
      tp1_percent = signal.tp1_percent
    move_sl_to_be = (
      self._config.tp1_move_sl_to_breakeven
      if self._config.tp1_move_sl_to_breakeven is not None
      else signal.move_sl_to_be
      if signal.move_sl_to_be is not None
      else False
    )
    return tp1_percent, move_sl_to_be

  def _resolve_tp1_volume(
    self, signal: SignalSchema, pos, tp1_percent: float
  ) -> TradeResult | float:
    """Derive close volume; returns TradeResult.fail on error, float on success."""
    symbol = signal.symbol
    if self._config.volume_decision_enabled:
      if tp1_percent is None:
        # Neither config.position_tp1_percent nor signal.tp1_percent supplied a
        # percentage, so there is nothing to size the partial close from. Fail
        # cleanly instead of crashing on ``None / 100``.
        return TradeResult.fail(
          "No TP1 percent available — set POSITION_TP1_PERCENT or include "
          "tp1_percent in the signal."
        )
      calculated = pos.volume * (tp1_percent / 100)
      close_volume = self._executor.normalize_volume(symbol, calculated)
      logger.info(
        "[handle_tp1] VOLUME_DECISION mode | position_volume=%s tp1_percent=%s%% "
        "calculated=%s → close_volume=%s",
        pos.volume,
        tp1_percent,
        calculated,
        close_volume,
      )
      return close_volume
    if signal.quantity is None:
      return TradeResult.fail("Missing quantity in TP1 signal")
    close_volume = self._executor.convert_quantity_to_lots(symbol, signal.quantity)
    logger.info(
      "[handle_tp1] Payload quantity mode | qty=%s → close_volume=%s",
      signal.quantity,
      close_volume,
    )
    return close_volume

  def handle_tp1(self, signal: SignalSchema) -> TradeResult:
    """Partial close at TP1, then optionally move SL to entry (breakeven).

    See _resolve_tp1_params for tp1_percent/move_sl_to_be resolution rules.
    """
    symbol = signal.symbol
    positions = self._executor.get_open_positions(symbol, strategy=signal.strategy)

    if not positions:
      return TradeResult.fail("No open position — likely SL already triggered.")

    pos = positions[0]
    tp1_percent, move_sl_to_be = self._resolve_tp1_params(signal)

    volume_result = self._resolve_tp1_volume(signal, pos, tp1_percent)
    if isinstance(volume_result, TradeResult):
      # _resolve_tp1_volume returns a TradeResult (a dataclass, *not* a dict) on
      # failure. ``isinstance(..., dict)`` would never match it, silently passing
      # the failure object through as the close volume.
      return volume_result
    close_volume: float = volume_result

    close_result = self._executor.partial_close_position(
      symbol=symbol,
      close_volume=close_volume,
      position_ticket=pos.ticket,
      strategy=signal.strategy,
    )

    if not close_result.get("success"):
      return close_result

    if not move_sl_to_be:
      # TP1 is partial-close-only: the original entry SL stays in place and keeps
      # protecting the runner, so there is no breakeven move to attempt.
      logger.info(
        "[handle_tp1] Breakeven move disabled — keeping original entry SL for %s.",
        symbol,
      )
      return close_result

    sl_result = self._executor.update_position_sl(
      symbol=symbol,
      new_sl=pos.price_open,
      position_ticket=pos.ticket,
      strategy=signal.strategy,
    )
    close_result["sl_update"] = sl_result

    if not sl_result.get("success"):
      # Safety invariant: a live position must never run without a protective
      # stop. The breakeven SL could not be placed, so the remaining volume is
      # now unprotected — close it immediately rather than let it run naked.
      logger.critical(
        "[handle_tp1] Breakeven SL FAILED (%s) for %s — remaining position is "
        "unprotected; closing it immediately.",
        sl_result.get("comment"),
        symbol,
      )
      failsafe = self._executor.close_all_positions(
        symbol, reason="SL_FAILSAFE", strategy=signal.strategy
      )
      close_result["sl_failsafe_close"] = failsafe
      if failsafe.get("success"):
        close_result["comment"] = (
          f"TP1 partial filled but breakeven SL failed "
          f"({sl_result.get('comment')}) — unprotected position emergency-closed"
        )
      else:
        logger.critical(
          "[handle_tp1] FAILSAFE CLOSE FAILED for %s (%s) — position is OPEN "
          "WITHOUT a stop. Manual intervention required.",
          symbol,
          failsafe.get("comment"),
        )
        close_result["position_unprotected"] = True
        close_result["comment"] = (
          f"UNPROTECTED POSITION: breakeven SL failed ({sl_result.get('comment')}) "
          f"AND emergency close failed ({failsafe.get('comment')}) — close manually"
        )

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
  """FOREX / CFD market via a trading platform (backed by ``ForexExecutor``)."""

  @property
  def allows_multi_strategy_per_symbol(self) -> bool:
    """FOREX is the only market that can hold parallel positions on one symbol.

    Every operation ``SignalHandler`` performs is scoped by the strategy's own
    magic number — ``get_open_positions``/``close_all_positions`` filter on it,
    closes and SL edits target a specific ticket — so strategies sharing a
    symbol never touch each other. Driven by
    ``FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL`` (needs a hedging account; see
    ``ForexSignalProcessor._warn_if_multi_strategy_needs_hedging``).
    """
    return self._config.allow_multi_strategy_per_symbol


class CryptoMarket(ExecutorBackedMarket):
  """CRYPTO market via a centralized exchange (backed by ``CryptoExecutor``).

  Deliberately keeps the base ``allows_multi_strategy_per_symbol = False``: a
  CEX cannot isolate two strategies on one symbol. ``CryptoExecutor``
  *ignores* the ``strategy`` argument in ``get_open_positions`` (there is no
  magic-number equivalent) and cancels resting orders **symbol-wide**
  (``cancel_all_orders``), so letting a second strategy in would make the
  entry's stale-cleanup close the first strategy's position and wipe its
  SL/TP. ``CRYPTO_ALLOW_MULTI_STRATEGY_PER_SYMBOL`` therefore only relaxes
  ``CryptoExecutor._netting_conflict``; the handler-level guard still rejects
  the entry.
  """


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
        Required for both FOREX (``ForexExecutor``) and CRYPTO (``CryptoExecutor``).
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
