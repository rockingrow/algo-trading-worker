from typing import Any, Dict, List, Optional

from worker.gateways.config import ExecutionConfig
from worker.gateways.mt5.lot_sizing import LotSizer
from worker.gateways.mt5.stop_validator import StopValidator
from worker.gateways.mt5.symbol_resolver import SymbolResolver
from worker.interfaces.db_protocol import PositionStoreProtocol
from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger
from worker.schemas.signal_schema import SignalSchema
from worker.schemas.trade_result import TradeResult

logger = get_logger("worker.mt5_executor")

_MT5_COMMENT_MAX = 31


def _mt5_error_code(err) -> int:
  """mt5.last_error() returns (code, description); extract just the int."""
  return err[0] if isinstance(err, tuple) else int(err)



class MT5Executor:
  """Sends trade orders to the MT5 broker: open, partial-close, SL update, and
  full-close operations.

  Order-sending is the executor's single responsibility; symbol resolution,
  lot-size math, and stop validation are delegated to injected collaborators
  (:class:`SymbolResolver`, :class:`LotSizer`, :class:`StopValidator`). The raw
  MetaTrader5 module is injected as ``mt5_api`` so the executor can be imported
  and unit-tested off-Windows with a fake gateway.
  """

  def __init__(
    self,
    slippage_deviation: int,
    config: ExecutionConfig,
    mt5_api: Optional[Mt5GatewayProtocol] = None,
    symbol_resolver: Optional[SymbolResolver] = None,
    lot_sizer: Optional[LotSizer] = None,
    stop_validator: Optional[StopValidator] = None,
    db: Optional[PositionStoreProtocol] = None,
    strategy_magic_map: Optional[Dict[str, int]] = None,
  ) -> None:
    if mt5_api is None:
      import MetaTrader5 as mt5_api  # lazy: native extension only needed at runtime

    self.deviation = slippage_deviation
    self._config = config
    self._mt5 = mt5_api
    self._resolver = symbol_resolver or SymbolResolver(mt5_api)
    self._lot_sizer = lot_sizer or LotSizer(mt5_api, self._resolver, config)
    self._stop_validator = stop_validator or StopValidator(mt5_api)
    # Persistence used to resolve which live positions belong to a strategy via
    # the authoritative `strategy` column. Optional so the executor stays unit-
    # testable without a DB (it then falls back to the comment stamp).
    self._db = db
    # Maps strategy name → its dedicated MT5 magic number. A strategy present
    # here is stamped with — and filtered by — its own magic, giving native
    # MT5-level isolation without a DB lookup.
    self._strategy_magic_map: Dict[str, int] = strategy_magic_map or {}

  # ------------------------------------------------------------------ #
  #  Magic-number resolution                                            #
  # ------------------------------------------------------------------ #

  def _magic_for(self, strategy: Optional[str]) -> int:
    """Resolve the magic number a *strategy* trades under.

    Returns the strategy's dedicated magic. Raises KeyError if not mapped.
    """
    if not strategy:
      raise ValueError("Strategy must be provided to resolve magic number.")
    if strategy not in self._strategy_magic_map:
      raise KeyError(f"Strategy '{strategy}' not found in strategy_magic_map.")
    return self._strategy_magic_map[strategy]

  def owned_magics(self) -> set:
    """All magic numbers this worker owns: every mapped magic.

    Used to recognise every position this worker may have opened across all
    strategies (e.g. account-wide queries and terminal-close detection).
    """
    return set(self._strategy_magic_map.values())

  # ------------------------------------------------------------------ #
  #  Delegated helpers (kept on the executor to satisfy the protocol)   #
  # ------------------------------------------------------------------ #

  def get_symbol(self, base_symbol: str) -> str:
    return self._resolver.get_symbol(base_symbol)

  def calculate_lot_size(
    self,
    symbol: str,
    entry_price: float,
    sl_price: float,
    risk_percent: float,
    capital: Optional[float] = None,
  ) -> float:
    return self._lot_sizer.calculate_lot_size(
      symbol, entry_price, sl_price, risk_percent, capital
    )

  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float:
    return self._lot_sizer.convert_quantity_to_lots(symbol, quantity)

  def normalize_volume(self, symbol: str, volume: float) -> float:
    return self._lot_sizer.normalize_volume(symbol, volume)

  # ------------------------------------------------------------------ #
  #  Position Query Helpers                                              #
  # ------------------------------------------------------------------ #

  def get_open_positions(
    self, symbol: str, strategy: Optional[str] = None
  ) -> List[Any]:
    """Return open positions for the resolved symbol, scoped to this worker.

    When *strategy* is given, keep only the positions that belong to that
    strategy so strategies trading the same symbol stay isolated.
    The isolation is native at the MT5 level via `strategy_magic_map`, no DB lookup needed.

    When *strategy* is None, every position this worker owns (any of
    ``owned_magics``) for the symbol is returned.
    """
    resolved = self.get_symbol(symbol)
    positions = self._mt5.positions_get(symbol=resolved)
    if positions is None:
      return []

    if strategy is None:
      owned = self.owned_magics()
      return [p for p in positions if p.magic in owned]

    magic = self._magic_for(strategy)
    return [p for p in positions if p.magic == magic]

  def get_all_open_positions(self, strategy: Optional[str] = None) -> List[Any]:
    """Return all open positions across all symbols owned by this worker.

    Filters by every magic number this worker owns (base + mapped), so
    positions opened under any strategy's dedicated magic are included.
    When *strategy* is given, only positions belonging to that strategy are returned.
    """
    positions = self._mt5.positions_get()
    if positions is None:
      return []

    if strategy is None:
      owned = self.owned_magics()
      return [p for p in positions if p.magic in owned]

    magic = self._magic_for(strategy)
    return [p for p in positions if p.magic == magic]

  def close_single_position(self, pos: Any, reason: str = "FLAT") -> TradeResult:
    """Close a single MT5 position object (pos.symbol is already resolved)."""
    close_type = (
      self._mt5.ORDER_TYPE_SELL
      if pos.type == self._mt5.ORDER_TYPE_BUY
      else self._mt5.ORDER_TYPE_BUY
    )
    tick = self._mt5.symbol_info_tick(pos.symbol)
    price = tick.bid if close_type == self._mt5.ORDER_TYPE_SELL else tick.ask

    request: Dict[str, Any] = {
      "action": self._mt5.TRADE_ACTION_DEAL,
      "symbol": pos.symbol,
      "volume": float(pos.volume),
      "type": close_type,
      "position": pos.ticket,
      "price": float(price),
      "deviation": self.deviation,
      "magic": pos.magic,  # close deal inherits the position's own magic
      "comment": f"Full Close {reason}",
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }

    logger.info(f"[close_single] Closing ticket {pos.ticket}, vol={pos.volume}, reason={reason}")
    result = self._mt5.order_send(request)

    if result and result.retcode == self._mt5.TRADE_RETCODE_DONE:
      logger.info(f"[close_single] Closed ticket {pos.ticket} successfully")
      return TradeResult.ok(
        retcode=result.retcode,
        ticket=result.order,
        price=result.price,
        volume=result.volume,
        comment=f"Closed [{reason}]",
      )

    err = result.comment if result else self._mt5.last_error()
    logger.error(f"[close_single] Failed to close ticket {pos.ticket}. Error: {err}")
    return TradeResult.fail(str(err))

  # ------------------------------------------------------------------ #
  #  Entry: Open a new LONG / SHORT position                             #
  # ------------------------------------------------------------------ #

  def open_position(self, signal: SignalSchema) -> TradeResult:
    """Open a new market order (LONG → BUY, SHORT → SELL)."""
    action_map = {
      "LONG": self._mt5.ORDER_TYPE_BUY,
      "SHORT": self._mt5.ORDER_TYPE_SELL,
    }
    action_str = signal.action.value  # already validated upstream

    if action_str not in action_map:
      logger.warning(f"open_position called with unsupported action: '{action_str}'")
      return TradeResult.fail("Action Mapping Failed")

    order_type = action_map[action_str]
    symbol = self.get_symbol(signal.symbol)
    tick = self._mt5.symbol_info_tick(symbol)
    price = tick.ask if order_type == self._mt5.ORDER_TYPE_BUY else tick.bid

    # Calculate position size
    if self._config.volume_decision_enabled:
      # Fixed capital mode: ignore payload quantity, derive lot from config
      if signal.sl:
        risk = (
          signal.risk_percent
          if signal.risk_percent is not None
          else self._config.risk_percentage
        )
        volume = self.calculate_lot_size(
          symbol,
          price,
          signal.sl,
          risk,
          capital=self._config.capital,
        )
        logger.info(
          f"[open_position] VOLUME_DECISION mode | capital={self._config.capital} "
          f"risk={risk}% (source={'signal' if signal.risk_percent is not None else 'config'}) → lot={volume}"
        )
      else:
        sym_info = self._mt5.symbol_info(symbol)
        volume = sym_info.volume_min if sym_info else 0.01
        logger.warning(
          "[open_position] VOLUME_DECISION_ENABLED but no SL in signal. "
          "Falling back to minimum lot."
        )
    else:
      # Payload quantity mode: use quantity transmitted from broker
      volume = self.convert_quantity_to_lots(symbol, signal.quantity)
      logger.info(
        f"[open_position] Payload quantity mode | qty={signal.quantity} → lot={volume}"
      )

    request: Dict[str, Any] = {
      "action": self._mt5.TRADE_ACTION_DEAL,
      "symbol": symbol,
      "volume": float(volume),
      "type": order_type,
      "price": float(price),
      "deviation": self.deviation,
      "magic": self._magic_for(signal.strategy),
      "comment": f"{signal.strategy} {(signal.signal_id or '')[-2:]}".strip()[:_MT5_COMMENT_MAX - 1],
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }

    # Validate SL/TP against live price; price may have moved since the signal
    # was generated, causing 10016 (TRADE_RETCODE_INVALID_STOPS) without this.
    sl, tp = self._stop_validator.validate_stops(
      symbol, order_type, tick, signal.sl, signal.tp2
    )

    if sl is not None:
      request["sl"] = float(sl)

    if tp is not None:
      request["tp"] = float(tp)

    logger.info(f"[open_position] Sending Order: {request}")
    result = self._mt5.order_send(request)

    if result is None:
      logger.error(f"order_send failed. error code: {self._mt5.last_error()}")
      return TradeResult.fail(
        "Send Failed", retcode=_mt5_error_code(self._mt5.last_error())
      )

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"Order rejected, retcode={result.retcode}, comment: {result.comment}"
      )
      return TradeResult.fail(result.comment, retcode=result.retcode, ticket=0)

    logger.info(
      f"[open_position] Filled! Ticket: {result.order}, Price: {result.price}, Vol: {result.volume}"
    )
    return TradeResult.ok(
      retcode=result.retcode,
      ticket=result.order,
      price=result.price,
      volume=result.volume,
    )

  # ------------------------------------------------------------------ #
  #  TP1: Partial close + move SL to breakeven                           #
  # ------------------------------------------------------------------ #

  def partial_close_position(
    self,
    symbol: str,
    close_volume: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
  ) -> TradeResult:
    """
    Partially close a position by sending a counter-direction market order
    with the specified volume. If *position_ticket* is given it targets that
    specific ticket; otherwise closes the first matching magic-number position.
    When *strategy* is given, only positions belonging to that strategy are
    considered.
    """
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol, strategy=strategy)

    if not positions:
      logger.warning(f"[partial_close] No open positions found for {resolved}")
      return TradeResult.fail("No Positions Found")

    # Target a specific ticket or fall back to the first open position
    pos = (
      next((p for p in positions if p.ticket == position_ticket), positions[0])
      if position_ticket
      else positions[0]
    )

    close_type = (
      self._mt5.ORDER_TYPE_SELL
      if pos.type == self._mt5.ORDER_TYPE_BUY
      else self._mt5.ORDER_TYPE_BUY
    )
    tick = self._mt5.symbol_info_tick(resolved)
    price = tick.bid if close_type == self._mt5.ORDER_TYPE_SELL else tick.ask

    # Clamp close_volume so we never exceed what is actually open
    safe_volume = min(close_volume, pos.volume)

    request: Dict[str, Any] = {
      "action": self._mt5.TRADE_ACTION_DEAL,
      "symbol": resolved,
      "volume": float(safe_volume),
      "type": close_type,
      "position": pos.ticket,  # CRITICAL: links close to specific ticket
      "price": float(price),
      "deviation": self.deviation,
      "magic": pos.magic,  # close deal inherits the position's own magic
      "comment": "Partial Close TP1",
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }

    logger.info(f"[partial_close] Sending partial close: {request}")
    result = self._mt5.order_send(request)

    if result is None:
      logger.error(f"partial_close order_send failed. error: {self._mt5.last_error()}")
      return TradeResult.fail(
        "Send Failed", retcode=_mt5_error_code(self._mt5.last_error())
      )

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"Partial close failed, retcode={result.retcode}, comment: {result.comment}"
      )
      return TradeResult.fail(result.comment, retcode=result.retcode)

    logger.info(
      f"[partial_close] OK. Ticket: {result.order}, Closed Vol: {result.volume}, Price: {result.price}"
    )
    return TradeResult.ok(
      retcode=result.retcode,
      ticket=result.order,
      price=result.price,
      volume=result.volume,
      source_ticket=pos.ticket,
    )

  def update_position_sl(
    self,
    symbol: str,
    new_sl: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
  ) -> TradeResult:
    """
    Update the Stop Loss of an open position to *new_sl* using
    TRADE_ACTION_SLTP. Targets a specific ticket or the first magic-number
    position found for the symbol. When *strategy* is given, only positions
    belonging to that strategy are considered.
    """
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol, strategy=strategy)

    if not positions:
      logger.warning(f"[update_sl] No open positions found for {resolved}")
      return TradeResult.fail("No Positions Found")

    pos = (
      next((p for p in positions if p.ticket == position_ticket), positions[0])
      if position_ticket
      else positions[0]
    )

    request: Dict[str, Any] = {
      "action": self._mt5.TRADE_ACTION_SLTP,
      "symbol": resolved,
      "position": pos.ticket,
      "sl": float(new_sl),
      "tp": float(pos.tp),  # preserve existing TP
    }

    logger.info(f"[update_sl] Updating SL for ticket {pos.ticket} → {new_sl}")
    result = self._mt5.order_send(request)

    if result is None:
      logger.error(f"update_sl order_send failed. error: {self._mt5.last_error()}")
      return TradeResult.fail(
        "SL Update Failed", retcode=_mt5_error_code(self._mt5.last_error())
      )

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"SL update rejected, retcode={result.retcode}, comment: {result.comment}"
      )
      return TradeResult.fail(result.comment, retcode=result.retcode)

    logger.info(f"[update_sl] SL updated successfully for ticket {pos.ticket}")
    return TradeResult.ok(
      retcode=result.retcode,
      ticket=pos.ticket,
      new_sl=new_sl,
    )

  # ------------------------------------------------------------------ #
  #  TP2 / SL / R_SL: Full close using MT5 actual volume                #
  # ------------------------------------------------------------------ #

  def close_all_positions(
    self, symbol: str, reason: str = "CLOSE", strategy: Optional[str] = None
  ) -> TradeResult:
    """
    Close ALL open positions for the symbol at actual MT5 volume.
    Webhook quantity is intentionally ignored to avoid dust-lot errors.
    When *strategy* is given, only positions belonging to that strategy are
    closed — positions opened by other strategies on the same symbol are left
    untouched.
    """
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol, strategy=strategy)

    if not positions:
      logger.warning(f"[close_all] No open positions found for {resolved}")
      return TradeResult.fail("No Positions Found")

    success_count = 0
    last_result = None

    for pos in positions:
      close_type = (
        self._mt5.ORDER_TYPE_SELL
        if pos.type == self._mt5.ORDER_TYPE_BUY
        else self._mt5.ORDER_TYPE_BUY
      )
      tick = self._mt5.symbol_info_tick(resolved)
      price = tick.bid if close_type == self._mt5.ORDER_TYPE_SELL else tick.ask

      request: Dict[str, Any] = {
        "action": self._mt5.TRADE_ACTION_DEAL,
        "symbol": resolved,
        "volume": float(pos.volume),  # Use ACTUAL MT5 volume, never webhook quantity
        "type": close_type,
        "position": pos.ticket,
        "price": float(price),
        "deviation": self.deviation,
        "magic": pos.magic,  # close deal inherits the position's own magic
        "comment": f"Full Close {reason}",
        "type_time": self._mt5.ORDER_TIME_GTC,
        "type_filling": self._mt5.ORDER_FILLING_IOC,
      }

      logger.info(
        f"[close_all] Closing ticket {pos.ticket}, vol={pos.volume}, reason={reason}"
      )
      result = self._mt5.order_send(request)

      if result and result.retcode == self._mt5.TRADE_RETCODE_DONE:
        logger.info(f"[close_all] Closed ticket {pos.ticket} successfully")
        success_count += 1
        last_result = result
      else:
        err = result.comment if result else self._mt5.last_error()
        logger.error(f"[close_all] Failed to close ticket {pos.ticket}. Error: {err}")

    if success_count > 0:
      return TradeResult.ok(
        retcode=self._mt5.TRADE_RETCODE_DONE,
        ticket=last_result.order,
        source_ticket=positions[0].ticket,  # the position's original ticket
        price=last_result.price,
        volume=last_result.volume,
        comment=f"Closed {success_count} position(s) [{reason}]",
      )

    return TradeResult.fail(f"Failed to close positions [{reason}]")
