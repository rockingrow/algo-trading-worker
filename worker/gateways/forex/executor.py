"""
worker/gateways/forex/executor.py
─────────────────────────────────
Platform-agnostic FOREX order executor — the forex counterpart of
``CryptoExecutor``.

Implements :class:`~worker.interfaces.executor_protocol.TradeExecutorProtocol` so
:class:`~worker.gateways.market_strategy.ForexMarket` drives it exactly like the
crypto strategy drives ``CryptoExecutor``. All platform specifics are delegated to
an injected :class:`~worker.gateways.forex.base.BasePlatformGateway` (MetaTrader 5
today), so the executor is platform-agnostic and unit-testable with a fake gateway.

Magic-number strategy isolation is a forex concept (a CEX has none): each strategy
trades under its own MT5 magic via ``strategy_magic_map``, giving native
broker-level isolation without a DB lookup.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from worker.gateways.config import ExecutionConfig
from worker.gateways.forex.base import (
  SIDE_LONG,
  SIDE_SHORT,
  BasePlatformGateway,
  PlatformPosition,
  SymbolSpec,
)
from worker.gateways.forex.lot_sizing import LotSizer
from worker.gateways.forex.stop_validator import StopValidator
from worker.interfaces.db_protocol import PositionStoreProtocol
from worker.logger import get_logger
from worker.schemas.signal_schema import SignalSchema
from worker.schemas.trade_result import TradeResult, total_profit

logger = get_logger("worker.gateways.forex.executor")

_ACTION_SIDE = {"LONG": SIDE_LONG, "SHORT": SIDE_SHORT}


class ForexExecutor:
  """Sends trade orders to a forex platform gateway: open, partial-close, SL
  update, and full-close operations."""

  def __init__(
    self,
    gateway: BasePlatformGateway,
    config: ExecutionConfig,
    db: Optional[PositionStoreProtocol] = None,
    strategy_magic_map: Optional[Dict[str, int]] = None,
  ) -> None:
    self._gateway = gateway
    self._config = config
    self._db = db
    self._lot_sizer = LotSizer(config)
    self._stop_validator = StopValidator()
    self._strategy_magic_map: Dict[str, int] = strategy_magic_map or {}

  # ── Magic-number resolution ───────────────────────────────────────────── #

  def _magic_for(self, strategy: Optional[str]) -> int:
    if not strategy:
      raise ValueError("Strategy must be provided to resolve magic number.")
    if strategy not in self._strategy_magic_map:
      raise KeyError(f"Strategy '{strategy}' not found in strategy_magic_map.")
    return self._strategy_magic_map[strategy]

  def owned_magics(self) -> set:
    """Every magic number this worker owns (used for account-wide queries and
    terminal-close detection)."""
    return set(self._strategy_magic_map.values())

  def set_strategy_magic_map(self, mapping: Optional[Dict[str, int]]) -> None:
    """Replace the per-strategy magic map at runtime.

    The broker owns the mapping and pushes it in the ``WORKER_CONNECTED_ACK``
    (``strategy_magic_map``, applied before trading starts), so
    :meth:`_magic_for` and :meth:`owned_magics` resolve
    against the broker-managed mapping rather than a static .env value. A copy is
    stored so a later mutation of the caller's dict can't alter it underneath us."""
    self._strategy_magic_map = dict(mapping or {})

  # ── Symbol / volume helpers (delegated to the agnostic math) ──────────── #

  def get_symbol(self, base_symbol: str) -> str:
    return self._gateway.resolve_symbol(base_symbol)

  def _spec(self, base_symbol: str) -> Optional[SymbolSpec]:
    return self._gateway.get_symbol_spec(self._gateway.resolve_symbol(base_symbol))

  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float:
    return self._lot_sizer.convert_quantity_to_lots(self._spec(symbol), quantity)

  def normalize_volume(self, symbol: str, volume: float) -> float:
    return self._lot_sizer.normalize_volume(self._spec(symbol), volume)

  def _resolve_capital(self) -> Optional[float]:
    """Capital base for risk sizing: fixed configured capital, or live account
    equity when ``USE_ACCOUNT_EQUITY`` is set. Returns ``None`` when equity is
    required but unavailable (caller falls back to the minimum lot)."""
    if not self._config.use_account_equity:
      return self._config.capital
    account = self._gateway.get_account()
    return account.get("equity") if account else None

  # ── Position queries ──────────────────────────────────────────────────── #

  def get_open_positions(
    self, symbol: str, strategy: Optional[str] = None
  ) -> List[PlatformPosition]:
    """Open positions for the resolved symbol, scoped to this worker by magic.

    With *strategy* given, keep only that strategy's positions (native magic
    isolation); otherwise every position this worker owns for the symbol.
    """
    resolved = self.get_symbol(symbol)
    positions = self._gateway.get_positions(resolved)
    if strategy is None:
      owned = self.owned_magics()
      return [p for p in positions if p.magic in owned]
    magic = self._magic_for(strategy)
    return [p for p in positions if p.magic == magic]

  def get_all_open_positions(
    self, strategy: Optional[str] = None
  ) -> List[PlatformPosition]:
    """All open positions across symbols owned by this worker (optionally one
    strategy)."""
    positions = self._gateway.get_positions()
    if strategy is None:
      owned = self.owned_magics()
      return [p for p in positions if p.magic in owned]
    magic = self._magic_for(strategy)
    return [p for p in positions if p.magic == magic]

  def close_single_position(
    self, pos: PlatformPosition, reason: str = "FLAT"
  ) -> TradeResult:
    """Close a single position (its ``symbol`` is already resolved)."""
    result = self._gateway.close_position(pos, comment=f"Full Close {reason}")
    if result.get("success"):
      result["comment"] = f"Closed [{reason}]"
    return result

  # ── Entry: open a new LONG / SHORT position ───────────────────────────── #

  def open_position(self, signal: SignalSchema) -> TradeResult:
    """Open a new market order (LONG → BUY, SHORT → SELL)."""
    side = _ACTION_SIDE.get(signal.action.value)
    if side is None:
      logger.warning(
        f"open_position called with unsupported action: '{signal.action.value}'"
      )
      return TradeResult.fail("Action Mapping Failed")

    symbol = self.get_symbol(signal.symbol)
    spec = self._gateway.get_symbol_spec(symbol)
    tick = self._gateway.get_tick(symbol)
    if tick is None:
      return TradeResult.fail("No tick / market data unavailable")
    price = tick.ask if side == SIDE_LONG else tick.bid
    # A non-positive quote is "no market data", whatever the gateway says: pricing
    # an entry off zero would size the lot against a meaningless SL distance and
    # push the stops to the wrong side of the market (broker retcode 10016).
    if price <= 0:
      logger.error(
        f"[open_position] Unusable {symbol} quote (bid={tick.bid} ask={tick.ask}) — "
        "refusing to price an entry off zero."
      )
      return TradeResult.fail("No tick / market data unavailable")

    # Stops are validated BEFORE the lot is sized. The broker's minimum stop
    # distance can push the SL further from the entry than the signal asked for,
    # and the lot must be sized on the stop that will actually be placed:
    # risk_cash is spread over the real SL distance, so sizing on the signal's
    # (narrower) stop and then submitting the widened one silently makes the
    # position risk more than RISK_PERCENTAGE of capital.
    sl, tp = self._stop_validator.validate_stops(
      spec, side, tick, signal.sl, signal.tp2
    )

    volume = self._resolve_entry_volume(signal, spec, price, sl)

    comment = f"{signal.strategy} {(signal.signal_id or '')[-2:]}".strip()
    return self._gateway.place_order(
      symbol=symbol,
      side=side,
      volume=volume,
      price=price,
      sl=sl,
      tp=tp,
      magic=self._magic_for(signal.strategy),
      comment=comment,
    )

  def get_entry_price(self, signal: SignalSchema) -> Optional[float]:
    """The price a market entry for *signal* would fill at right now, or ``None``
    when no tick is available.

    Same side convention as :meth:`open_position` (ask for a LONG, bid for a
    SHORT), so the staleness guard judges the signal against the price the order
    would really get rather than a mid-price the broker never quotes.
    """
    side = _ACTION_SIDE.get(signal.action.value)
    if side is None:
      return None
    tick = self._gateway.get_tick(self.get_symbol(signal.symbol))
    if tick is None:
      return None
    return tick.ask if side == SIDE_LONG else tick.bid

  def _resolve_entry_volume(
    self,
    signal: SignalSchema,
    spec: Optional[SymbolSpec],
    price: float,
    sl: Optional[float],
  ) -> float:
    """Entry lot: risk-based when VOLUME_DECISION is on (min lot if no SL or
    equity unavailable), otherwise the signal's own quantity converted to lots.

    *sl* is the **effective** stop — the signal's, after ``StopValidator`` has
    widened it to satisfy the broker's minimum stop distance — not ``signal.sl``.
    Risk sizing divides risk_cash by the SL distance, so it must use the stop the
    order will really carry or the position's true risk drifts from the
    configured percentage.
    """
    if not self._config.volume_decision_enabled:
      volume = self.convert_quantity_to_lots(signal.symbol, signal.quantity)
      logger.info(
        f"[open_position] Payload quantity mode | qty={signal.quantity} → lot={volume}"
      )
      return volume

    if not sl:
      volume = spec.volume_min if spec else 0.01
      logger.warning(
        "[open_position] VOLUME_DECISION_ENABLED but no SL in signal. "
        "Falling back to minimum lot."
      )
      return volume

    if self._config.use_custom_risk_percentage:
      risk = self._config.risk_percentage
      risk_source = "config(custom)"
    else:
      # A missing OR non-positive signal risk (upstream sends 0.0 when it has no
      # opinion) means "unspecified": fall back to the configured RISK_PERCENTAGE.
      # A literal 0% would otherwise zero risk_cash and floor every entry to the
      # minimum lot regardless of CAPITAL/equity.
      use_signal_risk = signal.risk_percent is not None and signal.risk_percent > 0
      risk = signal.risk_percent if use_signal_risk else self._config.risk_percentage
      risk_source = "signal" if use_signal_risk else "config"
    # Scale-in: the broker's pre-scaled signal.quantity is ignored in risk mode,
    # so re-apply the scale-in factor here (1.0 for a normal entry). Sizing is
    # linear in risk, so scaling risk scales the resulting lot by the same factor.
    scale_factor = signal.scale_quantity_factor()
    risk *= scale_factor

    capital = self._resolve_capital()
    if capital is None:
      logger.error(
        "[open_position] USE_ACCOUNT_EQUITY set but account equity unavailable — using min lot."
      )
      return 0.01

    volume = self._lot_sizer.calculate_lot_size(spec, price, sl, risk, capital)
    capital_src = (
      "account_equity"
      if self._config.use_account_equity
      else f"capital={self._config.capital}"
    )
    # Surface the widened stop explicitly: the lot below is smaller than the
    # signal's own SL would have produced, and that difference is what keeps the
    # position inside its risk budget.
    sl_note = (
      f" (broker stop distance widened SL {signal.sl} → {sl})"
      if signal.sl and sl != signal.sl
      else ""
    )
    logger.info(
      f"[open_position] VOLUME_DECISION mode | {capital_src} "
      f"risk={risk}% (source={risk_source}, "
      f"scale_factor={scale_factor}) sl={sl}{sl_note} → lot={volume}"
    )
    return volume

  # ── TP1: partial close ────────────────────────────────────────────────── #

  def partial_close_position(
    self,
    symbol: str,
    close_volume: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
    fallback_close_price: Optional[float] = None,
  ) -> TradeResult:
    # ``fallback_close_price`` is accepted for a uniform executor contract but
    # unused here: MT5 reports the deal's own realized PnL, so a close never has
    # to be valued at the signal's price the way an avgPrice=0 crypto fill does.
    positions = self.get_open_positions(symbol, strategy=strategy)
    if not positions:
      logger.warning(f"[partial_close] No open positions found for {symbol}")
      return TradeResult.fail("No Positions Found")

    pos = self._pick(positions, position_ticket)
    # Clamp so we never close more than is actually open.
    safe_volume = min(close_volume, pos.volume)

    result = self._gateway.close_position(
      pos, volume=safe_volume, comment="Partial Close TP1"
    )
    if result.get("success"):
      result["source_ticket"] = str(pos.ticket)
    return result

  def update_position_sl(
    self,
    symbol: str,
    new_sl: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
  ) -> TradeResult:
    positions = self.get_open_positions(symbol, strategy=strategy)
    if not positions:
      logger.warning(f"[update_sl] No open positions found for {symbol}")
      return TradeResult.fail("No Positions Found")

    pos = self._pick(positions, position_ticket)
    return self._gateway.modify_sl(pos, new_sl)

  # ── TP2 / SL / R_SL: full close ───────────────────────────────────────── #

  def close_all_positions(
    self,
    symbol: str,
    reason: str = "CLOSE",
    strategy: Optional[str] = None,
    fallback_close_price: Optional[float] = None,
  ) -> TradeResult:
    """Close ALL open positions for the symbol at actual broker volume.

    ``fallback_close_price`` is part of the shared executor contract but unused
    here — MT5 reads each close's realized PnL from its own deal, so it never
    needs to value the close at the signal's price (see ``partial_close_position``).
    """
    positions = self.get_open_positions(symbol, strategy=strategy)
    if not positions:
      logger.warning(f"[close_all] No open positions found for {symbol}")
      return TradeResult.fail("No Positions Found")

    success_count = 0
    last_result: Optional[Any] = None
    closed_results: List[Any] = []
    for pos in positions:
      result = self._gateway.close_position(pos, comment=f"Full Close {reason}")
      if result.get("success"):
        success_count += 1
        last_result = result
        closed_results.append(result)
      else:
        logger.error(
          f"[close_all] Failed to close ticket {pos.ticket}: {result.get('comment')}"
        )

    if success_count > 0 and last_result is not None:
      return TradeResult.ok(
        retcode=last_result.get("retcode"),
        ticket=last_result.get("ticket"),
        source_ticket=str(positions[0].ticket),
        price=last_result.get("price"),
        volume=last_result.get("volume"),
        comment=f"Closed {success_count} position(s) [{reason}]",
        # Unlike price/volume — which describe the last fill — the PnL is summed
        # across every position closed here, because the notification reports one
        # close event and the operator's question is what the whole exit booked.
        profit=total_profit(closed_results),
      )
    return TradeResult.fail(f"Failed to close positions [{reason}]")

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _pick(
    positions: List[PlatformPosition], ticket: Optional[int]
  ) -> PlatformPosition:
    """Target a specific ticket, else the first open position."""
    if ticket:
      return next((p for p in positions if p.ticket == ticket), positions[0])
    return positions[0]
