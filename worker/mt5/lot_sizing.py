"""
worker/mt5/lot_sizing.py
────────────────────────
All lot-size / volume arithmetic, extracted from ``MT5Executor``.

This is pure risk-management math (risk %, contract-size conversion, broker
step/min/max clamping) plus symbol-info lookups. Isolating it gives the
calculation logic a single reason to change and makes it unit-testable with a
fake gateway — no order-sending side effects involved.

Capital / risk inputs come from an injected :class:`ExecutionConfig` rather than
the global ``settings`` singleton (Dependency Inversion).
"""

from __future__ import annotations

import math
from typing import Optional

from worker.core.config import ExecutionConfig
from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger
from worker.mt5.symbol_resolver import SymbolResolver

logger = get_logger("worker.mt5.lot_sizing")


def _decimals_for_step(step: float) -> int:
  """Number of decimal places implied by a volume step (e.g. 0.01 -> 2).

  Prevents floating-point artifacts like 0.020000000000000004 from causing
  MT5 errors.
  """
  if step >= 1.0:
    return 0
  return len(str(step).rstrip("0").split(".")[1])


class LotSizer:
  """Computes broker-valid lot sizes from risk parameters or raw quantities."""

  def __init__(
    self,
    mt5_api: Mt5GatewayProtocol,
    symbol_resolver: SymbolResolver,
    config: ExecutionConfig,
  ) -> None:
    self._mt5 = mt5_api
    self._resolver = symbol_resolver
    self._config = config

  def calculate_lot_size(
    self,
    symbol: str,
    entry_price: float,
    sl_price: float,
    risk_percent: float,
    capital: Optional[float] = None,
  ) -> float:
    """Calculate lot size based on Risk % and SL distance.

    capital: fixed base to risk against. If None, uses live account equity or
    the configured capital depending on ``config.use_account_equity``.
    """
    if capital is None:
      if self._config.use_account_equity:
        account_info = self._mt5.account_info()
        if not account_info:
          logger.error(
            f"Could not retrieve account info for lot sizing. {self._mt5.last_error()}"
          )
          return 0.01
        capital = account_info.equity
      else:
        capital = self._config.capital

    symbol_info = self._mt5.symbol_info(self._resolver.get_symbol(symbol))
    if not symbol_info:
      logger.error(f"Cannot find symbol {self._resolver.get_symbol(symbol)}")
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
    symbol_info = self._mt5.symbol_info(self._resolver.get_symbol(symbol))
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

    # Round down to the nearest multiple of the step for strict risk management.
    # Add 1e-9 to prevent Python's floating-point precision issues
    # (e.g., 0.14999999999 / 0.01 being floored down an extra step).
    rounded_lot = math.floor((calculated_lot + 1e-9) / step) * step

    # Clamp the lot size within the broker's allowed limits.
    final_lot = max(symbol_info.volume_min, min(rounded_lot, symbol_info.volume_max))

    final_lot = round(final_lot, _decimals_for_step(step))

    logger.debug(
      f"[Quantity Conversion] Units: {quantity}, Contract Size: {contract_size}, "
      f"Calculated Lot: {final_lot} (Step: {step})"
    )

    return final_lot

  def normalize_volume(self, symbol: str, volume: float) -> float:
    """Round volume according to symbol's volume_step requirements."""
    symbol_info = self._mt5.symbol_info(self._resolver.get_symbol(symbol))
    if not symbol_info:
      logger.warning(f"Cannot get symbol info for {symbol}. Returning volume as-is.")
      return volume

    step = symbol_info.volume_step
    # Round down to the nearest multiple of the step for strict risk management
    rounded = math.floor((volume + 1e-9) / step) * step
    # Clamp to min/max allowed by broker
    final_vol = max(symbol_info.volume_min, min(rounded, symbol_info.volume_max))

    final_vol = round(final_vol, _decimals_for_step(step))

    logger.debug(
      f"[Volume Normalization] Input: {volume}, Step: {step}, Output: {final_vol}"
    )
    return final_vol
