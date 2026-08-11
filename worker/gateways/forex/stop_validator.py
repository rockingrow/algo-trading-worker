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

  @staticmethod
  def _resolve_stop(
    candidate: float, original: float, label: str, anchor: str
  ) -> float:
    """Take the adjusted stop, unless it isn't a positive price.

    Every adjustment is anchored on the live quote, so a zero or negative
    candidate means the anchor itself was garbage (an unusable tick, or a
    ``stops_level`` wider than the price). Sending such a level guarantees a
    rejection *and* hides the real cause, so fall back to the signal's own level
    and let the broker judge it on its own terms.
    """
    if candidate <= 0:
      logger.error(
        f"[validate_stops] {label} adjustment ({anchor}) computed {candidate}, "
        f"which is not a valid price — keeping the signal's {original}."
      )
      return original
    logger.warning(
      f"[validate_stops] {label} too close ({anchor}). Adjusted → {candidate}"
    )
    return candidate

  def _validate_sell_stops(self, sl, tp, tick, stop_dist, point, digits) -> Tuple:
    if sl is not None:
      min_sl = tick.ask + stop_dist
      if sl <= min_sl:
        sl = self._resolve_stop(
          round(min_sl + point, digits),
          sl,
          "SHORT SL",
          f"ask={tick.ask} stop_dist={stop_dist}",
        )
    if tp is not None:
      max_tp = tick.bid - stop_dist
      if tp >= max_tp:
        tp = self._resolve_stop(
          round(max_tp - point, digits),
          tp,
          "SHORT TP",
          f"bid={tick.bid} stop_dist={stop_dist}",
        )
    return sl, tp

  def _validate_buy_stops(self, sl, tp, tick, stop_dist, point, digits) -> Tuple:
    if sl is not None:
      max_sl = tick.bid - stop_dist
      if sl >= max_sl:
        sl = self._resolve_stop(
          round(max_sl - point, digits),
          sl,
          "LONG SL",
          f"bid={tick.bid} stop_dist={stop_dist}",
        )
    if tp is not None:
      min_tp = tick.ask + stop_dist
      if tp <= min_tp:
        tp = self._resolve_stop(
          round(min_tp + point, digits),
          tp,
          "LONG TP",
          f"ask={tick.ask} stop_dist={stop_dist}",
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

    An unusable quote (bid/ask of zero, as the terminal reports for a symbol it
    has no price for) leaves both levels untouched: adjusting against a zero
    anchor would push every stop to the wrong side of the market.
    """
    if not spec:
      return sl, tp

    if tick is None or tick.bid <= 0 or tick.ask <= 0:
      logger.error(
        "[validate_stops] Unusable tick "
        f"({'None' if tick is None else f'bid={tick.bid} ask={tick.ask}'}) — "
        "leaving SL/TP untouched."
      )
      return sl, tp

    stop_dist = spec.stops_level * spec.point
    point = spec.point
    digits = spec.digits

    if side == SIDE_SHORT:
      return self._validate_sell_stops(sl, tp, tick, stop_dist, point, digits)
    return self._validate_buy_stops(sl, tp, tick, stop_dist, point, digits)
