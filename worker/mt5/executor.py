import math
from typing import Any, Dict, List, Optional

import MetaTrader5 as mt5

from worker.logger import get_logger
from worker.schemas.signal_schema import SignalSchema
from worker.settings import settings

logger = get_logger("worker.mt5_executor")


class MT5Executor:
  """Executes trade orders on the MT5 broker: open, partial-close, SL update, and full-close operations.

  Handles symbol resolution, lot-size calculation, stop validation, and
  translates high-level signal actions into raw mt5.order_send() calls.
  """

  def __init__(self, magic_number: int, slippage_deviation: int):
    self.magic_number = magic_number
    self.deviation = slippage_deviation
    self._symbol_cache = {}

  def get_symbol(self, base_symbol: str) -> str:
    """Dynamically find the tradeable symbol name (e.g., XAUUSD -> XAUUSDc)."""
    if base_symbol in self._symbol_cache:
      return self._symbol_cache[base_symbol]

    symbols = mt5.symbols_get(group=f"*{base_symbol}*")
    if not symbols:
      logger.warning(f"No symbols found matching {base_symbol}")
      return base_symbol

    for sym in symbols:
      # Check if name starts with base_symbol and is tradeable
      if (
        sym.name.startswith(base_symbol)
        and sym.trade_mode != mt5.SYMBOL_TRADE_MODE_DISABLED
      ):
        if not sym.visible:
          mt5.symbol_select(sym.name, True)

        logger.info(f"Resolved symbol: {base_symbol} -> {sym.name}")
        self._symbol_cache[base_symbol] = sym.name
        return sym.name

    return base_symbol

  def calculate_lot_size(
    self,
    symbol: str,
    entry_price: float,
    sl_price: float,
    risk_percent: float,
    capital: Optional[float] = None,
  ) -> float:
    """Calculate lot size based on Risk % and SL distance.

    capital: fixed base to risk against. If None, uses live account equity.
    """
    if capital is None:
      if settings.use_account_equity:
        account_info = mt5.account_info()
        if not account_info:
          logger.error(
            f"Could not retrieve account info for lot sizing. {mt5.last_error()}"
          )
          return 0.01
        capital = account_info.equity
      else:
        capital = settings.capital

    symbol_info = mt5.symbol_info(self.get_symbol(symbol))
    if not symbol_info:
      logger.error(f"Cannot find symbol {self.get_symbol(symbol)}")
      return 0.01

    risk_cash = capital * (risk_percent / 100.0)

    # Handle Distance calculation
    point = symbol_info.point
    sl_distance_points = abs(entry_price - sl_price) / point

    if sl_distance_points <= 0:
      logger.warning("SL is too close or invalid. Falling back to minimum lot size.")
      return symbol_info.volume_min

    # Get tick values: value = (tick_value / tick_size) * point
    tick_value = symbol_info.trade_tick_value
    tick_size = symbol_info.trade_tick_size

    if tick_size == 0 or tick_value == 0:
      logger.error("Tick data is zero! Cannot calculate lot size accurately.")
      return symbol_info.volume_min

    point_value = (tick_value / tick_size) * point

    # Formula: Lot = Risk_Cash / (Distance_Points * Point_Value)
    calculated_lot = risk_cash / (sl_distance_points * point_value)

    # Round according to step
    step = symbol_info.volume_step
    rounded_lot = round(calculated_lot / step) * step

    # Clamp min, max
    final_lot = max(symbol_info.volume_min, min(rounded_lot, symbol_info.volume_max))

    logger.debug(
      f"[Volume Setup] Capital: {capital}, Risk Cash: {risk_cash}, SL Points: {sl_distance_points}, "
      f"Calculated Lot: {final_lot} (Step: {step})"
    )

    return round(final_lot, 2) if symbol_info.volume_step >= 0.01 else final_lot

  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float:
    """Convert raw quantity (units/contracts) from signal to MT5 lot size."""

    symbol_info = mt5.symbol_info(self.get_symbol(symbol))
    if not symbol_info:
      logger.error(f"Cannot get symbol info for {symbol}")
      return 0.01

    contract_size = symbol_info.trade_contract_size
    if contract_size <= 0:
      logger.warning(
        f"Invalid contract size for {symbol}: {contract_size}. Using raw quantity."
      )
      return quantity

    step = symbol_info.volume_step
    calculated_lot = quantity / contract_size

    # 1. Round down to the nearest multiple of the step for strict risk management.
    # Add 1e-9 to prevent Python's floating-point precision issues
    # (e.g., 0.14999999999 / 0.01 being floored down an extra step).
    rounded_lot = math.floor((calculated_lot + 1e-9) / step) * step

    # 2. Clamp the lot size within the broker's allowed limits.
    final_lot = max(symbol_info.volume_min, min(rounded_lot, symbol_info.volume_max))

    # 3. Dynamically calculate the number of decimal places instead of hardcoding to 2.
    # This prevents floating-point artifacts like 0.020000000000000004 from causing MT5 errors.
    decimals = 0
    if step < 1.0:
      # Convert step to string, strip trailing zeros, and count the length of the fractional part.
      decimals = len(str(step).rstrip("0").split(".")[1])

    final_lot = round(final_lot, decimals)

    logger.debug(
      f"[Quantity Conversion] Units: {quantity}, Contract Size: {contract_size}, "
      f"Calculated Lot: {final_lot} (Step: {step})"
    )

    return final_lot

  def normalize_volume(self, symbol: str, volume: float) -> float:
    """Round volume according to symbol's volume_step requirements."""
    symbol_info = mt5.symbol_info(self.get_symbol(symbol))
    if not symbol_info:
      logger.warning(f"Cannot get symbol info for {symbol}. Returning volume as-is.")
      return volume

    step = symbol_info.volume_step
    # Round down to the nearest multiple of the step for strict risk management
    rounded = math.floor((volume + 1e-9) / step) * step
    # Clamp to min/max allowed by broker
    final_vol = max(symbol_info.volume_min, min(rounded, symbol_info.volume_max))

    # Dynamically calculate decimal places based on step
    decimals = 0
    if step < 1.0:
      decimals = len(str(step).rstrip("0").split(".")[1])

    final_vol = round(final_vol, decimals)

    logger.debug(
      f"[Volume Normalization] Input: {volume}, Step: {step}, Output: {final_vol}"
    )
    return final_vol

  # ------------------------------------------------------------------ #
  #  Position Query Helpers                                              #
  # ------------------------------------------------------------------ #

  def get_open_positions(self, symbol: str) -> List[Any]:
    """Return all open positions for the resolved symbol (filtered by magic)."""
    resolved = self.get_symbol(symbol)
    positions = mt5.positions_get(symbol=resolved)
    if positions is None:
      return []
    return [p for p in positions if p.magic == self.magic_number]

  # ------------------------------------------------------------------ #
  #  Stop validation helper                                              #
  # ------------------------------------------------------------------ #

  def _validate_sell_stops(self, sl, tp, tick, stop_dist, point, digits) -> tuple:
    if sl is not None:
      min_sl = tick.ask + stop_dist
      if sl <= min_sl:
        sl = round(min_sl + point, digits)
        logger.warning(
          f"[validate_stops] SHORT SL too close (ask={tick.ask} stop_dist={stop_dist}). Adjusted → {sl}"
        )
    if tp is not None:
      max_tp = tick.bid - stop_dist
      if tp >= max_tp:
        tp = round(max_tp - point, digits)
        logger.warning(
          f"[validate_stops] SHORT TP too close (bid={tick.bid} stop_dist={stop_dist}). Adjusted → {tp}"
        )
    return sl, tp

  def _validate_buy_stops(self, sl, tp, tick, stop_dist, point, digits) -> tuple:
    if sl is not None:
      max_sl = tick.bid - stop_dist
      if sl >= max_sl:
        sl = round(max_sl - point, digits)
        logger.warning(
          f"[validate_stops] LONG SL too close (bid={tick.bid} stop_dist={stop_dist}). Adjusted → {sl}"
        )
    if tp is not None:
      min_tp = tick.ask + stop_dist
      if tp <= min_tp:
        tp = round(min_tp + point, digits)
        logger.warning(
          f"[validate_stops] LONG TP too close (ask={tick.ask} stop_dist={stop_dist}). Adjusted → {tp}"
        )
    return sl, tp

  def _validate_stops(
    self,
    symbol: str,
    order_type: int,
    tick,
    sl: Optional[float],
    tp: Optional[float],
  ) -> tuple:
    """
    Ensure SL/TP satisfy the broker's minimum stop distance from the live
    tick.  Adjusts by one point if violated so the order isn't rejected
    with retcode 10016 when market price moved since the signal was sent.

    For SELL (SHORT): SL must be >= ask + stop_dist; TP must be <= bid - stop_dist
    For BUY  (LONG):  SL must be <= bid - stop_dist; TP must be >= ask + stop_dist
    """
    symbol_info = mt5.symbol_info(symbol)
    if not symbol_info:
      return sl, tp

    stop_dist = symbol_info.trade_stops_level * symbol_info.point
    point = symbol_info.point
    digits = symbol_info.digits

    if order_type == mt5.ORDER_TYPE_SELL:
      return self._validate_sell_stops(sl, tp, tick, stop_dist, point, digits)
    return self._validate_buy_stops(sl, tp, tick, stop_dist, point, digits)

  # ------------------------------------------------------------------ #
  #  Entry: Open a new LONG / SHORT position                             #
  # ------------------------------------------------------------------ #

  def open_position(self, signal: SignalSchema) -> Dict[str, Any]:
    """Open a new market order (LONG → BUY, SHORT → SELL)."""
    action_map = {"LONG": mt5.ORDER_TYPE_BUY, "SHORT": mt5.ORDER_TYPE_SELL}
    action_str = signal.action.value  # already validated upstream

    if action_str not in action_map:
      logger.warning(f"open_position called with unsupported action: '{action_str}'")
      return {"success": False, "retcode": -1, "comment": "Action Mapping Failed"}

    order_type = action_map[action_str]
    symbol = self.get_symbol(signal.symbol)
    tick = mt5.symbol_info_tick(symbol)
    price = tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid

    # Calculate position size
    if settings.volume_decision_enabled:
      # Fixed capital mode: ignore payload quantity, derive lot from settings
      if signal.sl:
        volume = self.calculate_lot_size(
          symbol,
          price,
          signal.sl,
          settings.risk_percentage,
          capital=settings.capital,
        )
        logger.info(
          f"[open_position] VOLUME_DECISION mode | capital={settings.capital} "
          f"risk={settings.risk_percentage}% → lot={volume}"
        )
      else:
        sym_info = mt5.symbol_info(symbol)
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
      "action": mt5.TRADE_ACTION_DEAL,
      "symbol": symbol,
      "volume": float(volume),
      "type": order_type,
      "price": float(price),
      "deviation": self.deviation,
      "magic": self.magic_number,
      "comment": f"TV {signal.action.value}",
      "type_time": mt5.ORDER_TIME_GTC,
      "type_filling": mt5.ORDER_FILLING_IOC,
    }

    # Validate SL/TP against live price; price may have moved since the signal
    # was generated, causing 10016 (TRADE_RETCODE_INVALID_STOPS) without this.
    sl, tp = self._validate_stops(symbol, order_type, tick, signal.sl, signal.tp2)

    if sl is not None:
      request["sl"] = float(sl)

    if tp is not None:
      request["tp"] = float(tp)

    logger.info(f"[open_position] Sending Order: {request}")
    result = mt5.order_send(request)

    if result is None:
      logger.error(f"order_send failed. error code: {mt5.last_error()}")
      return {"success": False, "retcode": mt5.last_error(), "comment": "Send Failed"}

    if result.retcode != mt5.TRADE_RETCODE_DONE:
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
      mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
    )
    tick = mt5.symbol_info_tick(resolved)
    price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask

    # Clamp close_volume so we never exceed what is actually open
    safe_volume = min(close_volume, pos.volume)

    request: Dict[str, Any] = {
      "action": mt5.TRADE_ACTION_DEAL,
      "symbol": resolved,
      "volume": float(safe_volume),
      "type": close_type,
      "position": pos.ticket,  # CRITICAL: links close to specific ticket
      "price": float(price),
      "deviation": self.deviation,
      "magic": self.magic_number,
      "comment": "Partial Close TP1",
      "type_time": mt5.ORDER_TIME_GTC,
      "type_filling": mt5.ORDER_FILLING_IOC,
    }

    logger.info(f"[partial_close] Sending partial close: {request}")
    result = mt5.order_send(request)

    if result is None:
      logger.error(f"partial_close order_send failed. error: {mt5.last_error()}")
      return {"success": False, "retcode": mt5.last_error(), "comment": "Send Failed"}

    if result.retcode != mt5.TRADE_RETCODE_DONE:
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
      "action": mt5.TRADE_ACTION_SLTP,
      "symbol": resolved,
      "position": pos.ticket,
      "sl": float(new_sl),
      "tp": float(pos.tp),  # preserve existing TP
    }

    logger.info(f"[update_sl] Updating SL for ticket {pos.ticket} → {new_sl}")
    result = mt5.order_send(request)

    if result is None:
      logger.error(f"update_sl order_send failed. error: {mt5.last_error()}")
      return {
        "success": False,
        "retcode": mt5.last_error(),
        "comment": "SL Update Failed",
      }

    if result.retcode != mt5.TRADE_RETCODE_DONE:
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
        mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
      )
      tick = mt5.symbol_info_tick(resolved)
      price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask

      request: Dict[str, Any] = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": resolved,
        "volume": float(pos.volume),  # Use ACTUAL MT5 volume, never webhook quantity
        "type": close_type,
        "position": pos.ticket,
        "price": float(price),
        "deviation": self.deviation,
        "magic": self.magic_number,
        "comment": f"Full Close {reason}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
      }

      logger.info(
        f"[close_all] Closing ticket {pos.ticket}, vol={pos.volume}, reason={reason}"
      )
      result = mt5.order_send(request)

      if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        logger.info(f"[close_all] Closed ticket {pos.ticket} successfully")
        success_count += 1
        last_result = result
      else:
        err = result.comment if result else mt5.last_error()
        logger.error(f"[close_all] Failed to close ticket {pos.ticket}. Error: {err}")

    if success_count > 0:
      return {
        "success": True,
        "retcode": mt5.TRADE_RETCODE_DONE,
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
