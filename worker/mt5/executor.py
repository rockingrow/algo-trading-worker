from typing import Any, Dict, List, Optional

from worker.core.config import ExecutionConfig
from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger
from worker.mt5.lot_sizing import LotSizer
from worker.mt5.stop_validator import StopValidator
from worker.mt5.symbol_resolver import SymbolResolver
from worker.schemas.signal_schema import SignalSchema

logger = get_logger("worker.mt5_executor")


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
    magic_number: int,
    slippage_deviation: int,
    config: ExecutionConfig,
    mt5_api: Optional[Mt5GatewayProtocol] = None,
    symbol_resolver: Optional[SymbolResolver] = None,
    lot_sizer: Optional[LotSizer] = None,
    stop_validator: Optional[StopValidator] = None,
  ) -> None:
    if mt5_api is None:
      import MetaTrader5 as mt5_api  # lazy: native extension only needed at runtime

    self.magic_number = magic_number
    self.deviation = slippage_deviation
    self._config = config
    self._mt5 = mt5_api
    self._resolver = symbol_resolver or SymbolResolver(mt5_api)
    self._lot_sizer = lot_sizer or LotSizer(mt5_api, self._resolver, config)
    self._stop_validator = stop_validator or StopValidator(mt5_api)

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

  def get_open_positions(self, symbol: str) -> List[Any]:
    """Return all open positions for the resolved symbol (filtered by magic)."""
    resolved = self.get_symbol(symbol)
    positions = self._mt5.positions_get(symbol=resolved)
    if positions is None:
      return []
    return [p for p in positions if p.magic == self.magic_number]

  # ------------------------------------------------------------------ #
  #  Entry: Open a new LONG / SHORT position                             #
  # ------------------------------------------------------------------ #

  def open_position(self, signal: SignalSchema) -> Dict[str, Any]:
    """Open a new market order (LONG → BUY, SHORT → SELL)."""
    action_map = {
      "LONG": self._mt5.ORDER_TYPE_BUY,
      "SHORT": self._mt5.ORDER_TYPE_SELL,
    }
    action_str = signal.action.value  # already validated upstream

    if action_str not in action_map:
      logger.warning(f"open_position called with unsupported action: '{action_str}'")
      return {"success": False, "retcode": -1, "comment": "Action Mapping Failed"}

    order_type = action_map[action_str]
    symbol = self.get_symbol(signal.symbol)
    tick = self._mt5.symbol_info_tick(symbol)
    price = tick.ask if order_type == self._mt5.ORDER_TYPE_BUY else tick.bid

    # Calculate position size
    if self._config.volume_decision_enabled:
      # Fixed capital mode: ignore payload quantity, derive lot from config
      if signal.sl:
        volume = self.calculate_lot_size(
          symbol,
          price,
          signal.sl,
          self._config.risk_percentage,
          capital=self._config.capital,
        )
        logger.info(
          f"[open_position] VOLUME_DECISION mode | capital={self._config.capital} "
          f"risk={self._config.risk_percentage}% → lot={volume}"
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
      "magic": self.magic_number,
      "comment": f"TV {signal.action.value}",
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
      return {
        "success": False,
        "retcode": self._mt5.last_error(),
        "comment": "Send Failed",
      }

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"Order rejected, retcode={result.retcode}, comment: {result.comment}"
      )
      return {
        "success": False,
        "retcode": result.retcode,
        "comment": result.comment,
        "ticket": 0,
      }

    logger.info(
      f"[open_position] Filled! Ticket: {result.order}, Price: {result.price}, Vol: {result.volume}"
    )
    return {
      "success": True,
      "retcode": result.retcode,
      "ticket": result.order,
      "price": result.price,
      "volume": result.volume,
    }

  # ------------------------------------------------------------------ #
  #  TP1: Partial close + move SL to breakeven                           #
  # ------------------------------------------------------------------ #

  def partial_close_position(
    self, symbol: str, close_volume: float, position_ticket: Optional[int] = None
  ) -> Dict[str, Any]:
    """
    Partially close a position by sending a counter-direction market order
    with the specified volume. If *position_ticket* is given it targets that
    specific ticket; otherwise closes the first matching magic-number position.
    """
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol)

    if not positions:
      logger.warning(f"[partial_close] No open positions found for {resolved}")
      return {"success": False, "retcode": -1, "comment": "No Positions Found"}

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
      "magic": self.magic_number,
      "comment": "Partial Close TP1",
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }

    logger.info(f"[partial_close] Sending partial close: {request}")
    result = self._mt5.order_send(request)

    if result is None:
      logger.error(f"partial_close order_send failed. error: {self._mt5.last_error()}")
      return {
        "success": False,
        "retcode": self._mt5.last_error(),
        "comment": "Send Failed",
      }

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"Partial close failed, retcode={result.retcode}, comment: {result.comment}"
      )
      return {"success": False, "retcode": result.retcode, "comment": result.comment}

    logger.info(
      f"[partial_close] OK. Ticket: {result.order}, Closed Vol: {result.volume}, Price: {result.price}"
    )
    return {
      "success": True,
      "retcode": result.retcode,
      "ticket": result.order,
      "price": result.price,
      "volume": result.volume,
      "source_ticket": pos.ticket,
    }

  def update_position_sl(
    self, symbol: str, new_sl: float, position_ticket: Optional[int] = None
  ) -> Dict[str, Any]:
    """
    Update the Stop Loss of an open position to *new_sl* using
    TRADE_ACTION_SLTP. Targets a specific ticket or the first magic-number
    position found for the symbol.
    """
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol)

    if not positions:
      logger.warning(f"[update_sl] No open positions found for {resolved}")
      return {"success": False, "retcode": -1, "comment": "No Positions Found"}

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
      return {
        "success": False,
        "retcode": self._mt5.last_error(),
        "comment": "SL Update Failed",
      }

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"SL update rejected, retcode={result.retcode}, comment: {result.comment}"
      )
      return {"success": False, "retcode": result.retcode, "comment": result.comment}

    logger.info(f"[update_sl] SL updated successfully for ticket {pos.ticket}")
    return {
      "success": True,
      "retcode": result.retcode,
      "ticket": pos.ticket,
      "new_sl": new_sl,
    }

  # ------------------------------------------------------------------ #
  #  TP2 / SL / R_SL: Full close using MT5 actual volume                #
  # ------------------------------------------------------------------ #

  def close_all_positions(self, symbol: str, reason: str = "CLOSE") -> Dict[str, Any]:
    """
    Close ALL open positions for the symbol at actual MT5 volume.
    Webhook quantity is intentionally ignored to avoid dust-lot errors.
    """
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol)

    if not positions:
      logger.warning(f"[close_all] No open positions found for {resolved}")
      return {"success": False, "retcode": -1, "comment": "No Positions Found"}

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
        "magic": self.magic_number,
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
      return {
        "success": True,
        "retcode": self._mt5.TRADE_RETCODE_DONE,
        "ticket": last_result.order,
        "source_ticket": positions[0].ticket,  # Include the position's original ticket
        "price": last_result.price,
        "volume": last_result.volume,
        "comment": f"Closed {success_count} position(s) [{reason}]",
      }

    return {
      "success": False,
      "retcode": -1,
      "comment": f"Failed to close positions [{reason}]",
    }

  # ------------------------------------------------------------------ #
  #  Legacy / Convenience: keep execute_signal + close_position intact   #
  # ------------------------------------------------------------------ #

  def execute_signal(self, signal: SignalSchema) -> Dict[str, Any]:
    """
    Legacy single-entry dispatcher kept for backward compatibility.
    Prefer using SignalHandler which applies full action-specific logic.
    """
    action_str = signal.action.value

    if action_str in ("TP2", "SL", "R_SL"):
      return self.close_all_positions(signal.symbol, reason=action_str)

    if action_str == "TP1":
      close_vol = self.convert_quantity_to_lots(signal.symbol, signal.quantity)
      return self.partial_close_position(signal.symbol, close_vol)

    # LONG / SHORT
    return self.open_position(signal)

  def close_position(self, signal: SignalSchema) -> Dict[str, Any]:
    """
    Backward-compatible wrapper. Delegates to close_all_positions which
    uses actual MT5 volume — not the webhook quantity.
    """
    return self.close_all_positions(signal.symbol, reason=signal.action.value)
