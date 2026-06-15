"""
worker/gateways/forex/lot_sizing.py
───────────────────────────────────
Platform-agnostic lot-size / volume arithmetic for the FOREX market.

Pure risk-management math (risk %, contract-size conversion, broker
step/min/max clamping). It reads nothing from MetaTrader5 directly: the caller
(:class:`~worker.gateways.forex.executor.ForexExecutor`) supplies a
:class:`~worker.gateways.forex.base.SymbolSpec` fetched from the platform
gateway and a resolved *capital*, so this logic is unit-testable without any
platform and is shared by every forex platform.

Capital / risk inputs come from an injected :class:`ExecutionConfig` rather than
the global ``settings`` singleton (Dependency Inversion).
"""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Optional

from worker.gateways.config import ExecutionConfig
from worker.gateways.forex.base import SymbolSpec
from worker.logger import get_logger

logger = get_logger("worker.gateways.forex.lot_sizing")


def _decimals_for_step(step: float) -> int:
  """Number of decimal places implied by a volume step (e.g. 0.01 -> 2).

  Uses Decimal to handle sci-notation repr (e.g. 1e-5) that would IndexError
  a naive str.split('.') approach.
  """
  exp = Decimal(str(step)).normalize().as_tuple().exponent
  return max(0, -exp)


class LotSizer:
  """Computes broker-valid lot sizes from risk parameters or raw quantities."""

  def __init__(self, config: ExecutionConfig) -> None:
    self._config = config

  def calculate_lot_size(
    self,
    spec: Optional[SymbolSpec],
    entry_price: float,
    sl_price: float,
    risk_percent: float,
    capital: float,
  ) -> float:
    """Calculate lot size from Risk % and SL distance against *capital*."""
    if not spec:
      logger.error("Cannot size lot: missing symbol spec.")
      return 0.01

    risk_cash = capital * (risk_percent / 100.0)

    point = spec.point
    if point <= 0:
      logger.error("Symbol spec has invalid point value. Falling back to minimum lot size.")
      return spec.volume_min

    sl_distance_points = abs(entry_price - sl_price) / point

    if sl_distance_points <= 0:
      logger.warning("SL is too close or invalid. Falling back to minimum lot size.")
      return spec.volume_min

    tick_value = spec.tick_value
    tick_size = spec.tick_size

    if tick_size == 0 or tick_value == 0:
      logger.error("Tick data is zero! Cannot calculate lot size accurately.")
      return spec.volume_min

    point_value = (tick_value / tick_size) * point

    # Formula: Lot = Risk_Cash / (Distance_Points * Point_Value)
    calculated_lot = risk_cash / (sl_distance_points * point_value)

    step = spec.volume_step
    rounded_lot = round(calculated_lot / step) * step

    final_lot = max(spec.volume_min, min(rounded_lot, spec.volume_max))

    logger.debug(
      f"[Volume Setup] Capital: {capital}, Risk Cash: {risk_cash}, SL Points: {sl_distance_points}, "
      f"Calculated Lot: {final_lot} (Step: {step})"
    )

    return round(final_lot, 2) if spec.volume_step >= 0.01 else final_lot

  def convert_quantity_to_lots(
    self, spec: Optional[SymbolSpec], quantity: float
  ) -> float:
    """Convert raw quantity (units/contracts) from a signal to a broker lot size."""
    if not spec:
      logger.error("Cannot convert quantity: missing symbol spec.")
      return 0.01

    contract_size = spec.contract_size
    if contract_size <= 0:
      logger.warning(
        f"Invalid contract size: {contract_size}. Using raw quantity."
      )
      return quantity

    step = spec.volume_step
    calculated_lot = quantity / contract_size

    # Round down to the nearest step multiple for strict risk management.
    # Add 1e-9 to prevent Python's floating-point precision issues
    # (e.g., 0.14999999999 / 0.01 being floored down an extra step).
    rounded_lot = math.floor((calculated_lot + 1e-9) / step) * step

    final_lot = max(spec.volume_min, min(rounded_lot, spec.volume_max))
    final_lot = round(final_lot, _decimals_for_step(step))

    logger.debug(
      f"[Quantity Conversion] Units: {quantity}, Contract Size: {contract_size}, "
      f"Calculated Lot: {final_lot} (Step: {step})"
    )

    return final_lot

  def normalize_volume(self, spec: Optional[SymbolSpec], volume: float) -> float:
    """Round volume according to the symbol's volume_step requirements."""
    if not spec:
      logger.warning("Cannot normalize volume: missing symbol spec. Returning as-is.")
      return volume

    step = spec.volume_step
    rounded = math.floor((volume + 1e-9) / step) * step
    final_vol = max(spec.volume_min, min(rounded, spec.volume_max))
    final_vol = round(final_vol, _decimals_for_step(step))

    logger.debug(
      f"[Volume Normalization] Input: {volume}, Step: {step}, Output: {final_vol}"
    )
    return final_vol
