from typing import Any, Dict

import MetaTrader5 as mt5
from worker.logger import get_logger
from worker.schemas.broker_schema import SignalSchema

logger = get_logger("worker.mt5_executor")


class MT5Executor:
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
    self, symbol: str, entry_price: float, sl_price: float, risk_percent: float
  ) -> float:
    """Calculate lot size based on Risk % configuration and StopLoss distance."""
    account_info = mt5.account_info()
    if not account_info:
      logger.error("Could not retrieve account info for lot sizing.")
      return 0.01

    symbol_info = mt5.symbol_info(self.get_symbol(symbol))
    if not symbol_info:
      logger.error(f"Cannot find symbol {self.get_symbol(symbol)}")
      return 0.01

    equity = account_info.equity
    risk_cash = equity * (risk_percent / 100.0)

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
      f"[Volume Setup] Equity: {equity}, Risk Cash: {risk_cash}, SL Points: {sl_distance_points}, "
      f"Calculated Lot: {final_lot} (Step: {step})"
    )

    return round(final_lot, 2) if symbol_info.volume_step >= 0.01 else final_lot

  def execute_signal(self, signal: SignalSchema) -> Dict[str, Any]:
    """Execute trade on MetaTrader5."""
    action_str = signal.position.action.upper()

    if action_str in ["SL", "TP1", "TP2", "R_SL", "CLOSE"]:
      return self.close_position(signal)

    action_map = {"LONG": mt5.ORDER_TYPE_BUY, "SHORT": mt5.ORDER_TYPE_SELL}

    if action_str not in action_map:
      logger.warning(f"Action '{action_str}' currently not mapped for execution.")
      return {"success": False, "retcode": -1, "comment": "Action Mapping Failed"}

    order_type = action_map[action_str]
    symbol = self.get_symbol(signal.symbol)
    price = (
      mt5.symbol_info_tick(symbol).ask
      if order_type == mt5.ORDER_TYPE_BUY
      else mt5.symbol_info_tick(symbol).bid
    )

    # Calculate Volume instead of taking it from signal (per Roadmap requirements)
    risk_pct = signal.inputs.risk_percent if signal.inputs else 1.0

    if signal.position.sl:
      volume = self.calculate_lot_size(symbol, price, signal.position.sl, risk_pct)
    else:
      # If no SL, fall back to signal quantity
      volume = signal.position.quantity

    request = {
      "action": mt5.TRADE_ACTION_DEAL,
      "symbol": symbol,
      "volume": float(volume),
      "type": order_type,
      "price": float(price),
      "deviation": self.deviation,
      "magic": self.magic_number,
      "comment": f"TV Signal {signal.timeframe}",
      "type_time": mt5.ORDER_TIME_GTC,
      "type_filling": mt5.ORDER_FILLING_IOC,  # or ORDER_FILLING_FOK
    }

    # Assign SL/TP if provided
    if hasattr(signal.position, "sl") and signal.position.sl is not None:
      request["sl"] = float(signal.position.sl)

    if hasattr(signal.position, "tp1") and signal.position.tp1 is not None:
      request["tp"] = float(signal.position.tp1)

    logger.info(f"Sending Order: {request}")

    result = mt5.order_send(request)

    if result is None:
      logger.error(f"order_send failed immediately. error code: {mt5.last_error()}")
      return {
        "success": False,
        "retcode": mt5.last_error(),
        "comment": "Send Failed Server-Side",
      }

    if result.retcode != mt5.TRADE_RETCODE_DONE:  # 10009
      logger.error(f"Order failed, retcode={result.retcode}, comment: {result.comment}")
      return {
        "success": False,
        "retcode": result.retcode,
        "comment": result.comment,
        "ticket": 0,
      }

    logger.info(
      f"Execution Successful! Ticket: {result.order}, Price: {result.price}, Vol: {result.volume}"
    )
    return {
      "success": True,
      "retcode": result.retcode,
      "ticket": result.order,
      "price": result.price,
      "volume": result.volume,
    }

  def close_position(self, signal: SignalSchema) -> Dict[str, Any]:
    """Close an existing position based on signal symbol and magic number."""
    symbol = self.get_symbol(signal.symbol)
    positions = mt5.positions_get(symbol=symbol)

    if positions is None or len(positions) == 0:
      logger.warning(f"No open positions found to close for {symbol}")
      return {"success": False, "retcode": -1, "comment": "No Positions Found"}

    success_count = 0
    last_result = None

    for pos in positions:
      if pos.magic == self.magic_number:
        # Opposite order type
        close_type = (
          mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        )
        price = (
          mt5.symbol_info_tick(symbol).bid
          if close_type == mt5.ORDER_TYPE_SELL
          else mt5.symbol_info_tick(symbol).ask
        )

        request = {
          "action": mt5.TRADE_ACTION_DEAL,
          "symbol": symbol,
          "volume": pos.volume,
          "type": close_type,
          "position": pos.ticket,
          "price": price,
          "deviation": self.deviation,
          "magic": self.magic_number,
          "comment": f"Close {signal.position.action.upper()}",
          "type_time": mt5.ORDER_TIME_GTC,
          "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
          logger.info(f"Position Closed: Ticket {pos.ticket}")
          success_count += 1
          last_result = result
        else:
          err_msg = result.comment if result else mt5.last_error()
          logger.error(f"Failed to close position {pos.ticket}. Error: {err_msg}")

    if success_count > 0:
      return {
        "success": True,
        "retcode": mt5.TRADE_RETCODE_DONE,
        "ticket": last_result.order,
        "price": last_result.price,
        "volume": last_result.volume,
        "comment": f"Closed {success_count} positions",
      }

    return {
      "success": False,
      "retcode": -1,
      "comment": "Failed to close matched positions",
    }
