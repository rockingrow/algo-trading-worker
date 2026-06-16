"""
worker/gateways/forex/stop_validator.py
───────────────────────────────────────
Platform-agnostic SL/TP stop-distance validation for the FOREX market.

Market price may have moved since a signal was generated; without this guard the
order would be rejected for violating the broker's minimum stop distance (MT5
retcode 10016, TRADE_RETCODE_INVALID_STOPS). It reads nothing from MetaTrader5:
the caller supplies a :class:`~worker.gateways.forex.base.SymbolSpec` and
:class:`~worker.gateways.forex.base.Tick`, so the rules are testable without any
platform and shared across forex platforms.
"""

from __future__ import annotations

from typing import Optional, Tuple

from worker.gateways.forex.base import SIDE_SHORT, SymbolSpec, Tick
from worker.logger import get_logger

logger = get_logger("worker.gateways.forex.stop_validator")


class StopValidator:
  """Adjusts SL/TP by one point when they violate the broker stop distance."""

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
    spec: Optional[SymbolSpec],
    side: str,
    tick: Tick,
    sl: Optional[float],
    tp: Optional[float],
  ) -> Tuple:
    """Ensure SL/TP satisfy the broker's minimum stop distance from the live tick.

    For SHORT: SL must be >= ask + stop_dist; TP must be <= bid - stop_dist.
    For LONG:  SL must be <= bid - stop_dist; TP must be >= ask + stop_dist.
    """
    if not spec:
      return sl, tp

    stop_dist = spec.stops_level * spec.point
    point = spec.point
    digits = spec.digits

    if side == SIDE_SHORT:
      return self._validate_sell_stops(sl, tp, tick, stop_dist, point, digits)
    return self._validate_buy_stops(sl, tp, tick, stop_dist, point, digits)
