"""
worker/gateways/forex/mt5/stop_validator.py
───────────────────────────────────────────
Validates SL/TP levels against the broker's minimum stop distance, extracted
from ``MT5Executor``.

Market price may have moved since a signal was generated; without this guard the
order would be rejected with retcode 10016 (TRADE_RETCODE_INVALID_STOPS). Kept
as its own class so the stop-distance rules have a single reason to change.
"""

from __future__ import annotations

from typing import Optional, Tuple

from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger

logger = get_logger("worker.gateways.forex.mt5.stop_validator")


class StopValidator:
  """Adjusts SL/TP by one point when they violate the broker stop distance."""

  def __init__(self, mt5_api: Mt5GatewayProtocol) -> None:
    self._mt5 = mt5_api

  def _validate_sell_stops(self, sl, tp, tick, stop_dist, point, digits) -> Tuple:
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

  def _validate_buy_stops(self, sl, tp, tick, stop_dist, point, digits) -> Tuple:
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

  def validate_stops(
    self,
    symbol: str,
    order_type: int,
    tick,
    sl: Optional[float],
    tp: Optional[float],
  ) -> Tuple:
    """
    Ensure SL/TP satisfy the broker's minimum stop distance from the live tick.

    For SELL (SHORT): SL must be >= ask + stop_dist; TP must be <= bid - stop_dist
    For BUY  (LONG):  SL must be <= bid - stop_dist; TP must be >= ask + stop_dist
    """
    symbol_info = self._mt5.symbol_info(symbol)
    if not symbol_info:
      return sl, tp

    stop_dist = symbol_info.trade_stops_level * symbol_info.point
    point = symbol_info.point
    digits = symbol_info.digits

    if order_type == self._mt5.ORDER_TYPE_SELL:
      return self._validate_sell_stops(sl, tp, tick, stop_dist, point, digits)
    return self._validate_buy_stops(sl, tp, tick, stop_dist, point, digits)
