"""
worker/crypto/executor.py
─────────────────────────
Crypto order executor — the CRYPTO counterpart of ``MT5Executor``.

Implements :class:`~worker.interfaces.executor_protocol.TradeExecutorProtocol`
so :class:`~worker.core.market_strategy.CryptoMarket` drives it exactly like the
Forex strategy drives the MT5 executor. All exchange specifics are delegated to
an injected :class:`~worker.gateways.crypto.base.BaseExchangeGateway`, so the executor is
exchange-agnostic and unit-testable with a fake gateway.

Notes
─────
* LONG → BUY, SHORT → SELL; closes are reduce-only counter-orders.
* "SL to breakeven" after TP1 is a reduce-only ``STOP_MARKET`` at entry price.
* In one-way mode an exchange holds a single net position per symbol, so the
  ``strategy`` argument is accepted for interface parity but cannot isolate two
  strategies trading the same symbol at the exchange level (logical isolation
  only, via the ``strategy`` column in the DB).
"""

from __future__ import annotations

import math
from typing import Any, List, Optional

from worker.core.config import ExecutionConfig
from worker.gateways.crypto.base import BaseExchangeGateway, ExchangePosition
from worker.interfaces.db_protocol import PositionStoreProtocol
from worker.logger import get_logger
from worker.schemas.signal_schema import SignalSchema
from worker.schemas.trade_result import TradeResult

logger = get_logger("worker.gateways.crypto.executor")


class CryptoExecutor:
  """Sends trade orders to a crypto exchange gateway."""

  def __init__(
    self,
    gateway: BaseExchangeGateway,
    config: ExecutionConfig,
    db: Optional[PositionStoreProtocol] = None,
    quote_asset: str = "USDT",
  ) -> None:
    self._gateway = gateway
    self._config = config
    self._db = db
    self._quote = quote_asset.upper()

  # ── Symbol / quantity helpers ─────────────────────────────────────────── #

  def get_symbol(self, base_symbol: str) -> str:
    """Resolve a signal symbol to the exchange symbol (e.g. BTCUSD → BTCUSDT)."""
    s = base_symbol.upper().replace("/", "").replace(":", "")
    if s.endswith(self._quote):
      return s
    if s.endswith("USD") and self._quote == "USDT":
      return s + "T"
    return s + self._quote

  def normalize_volume(self, symbol: str, volume: float) -> float:
    """Round *volume* down to the symbol's step size."""
    f = self._gateway.get_symbol_filter(self.get_symbol(symbol))
    step = f.step_size or 0.0
    if step <= 0:
      return round(volume, 8)
    steps = math.floor(volume / step)
    decimals = max(0, -int(round(math.log10(step)))) if step < 1 else 0
    return round(steps * step, decimals)

  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float:
    """Crypto quantities are already in base units; just normalize to step size."""
    return self.normalize_volume(symbol, quantity)

  def calculate_quantity(
    self,
    symbol: str,
    entry_price: float,
    sl_price: float,
    risk_percent: float,
    capital: float,
  ) -> float:
    """Risk-based sizing: qty = (capital * risk%) / per-unit loss to SL."""
    per_unit_risk = abs(entry_price - sl_price)
    if per_unit_risk <= 0:
      logger.warning("[calculate_quantity] Non-positive SL distance; using min qty.")
      f = self._gateway.get_symbol_filter(self.get_symbol(symbol))
      return f.min_qty or 0.0
    risk_amount = capital * (risk_percent / 100.0)
    return self.normalize_volume(symbol, risk_amount / per_unit_risk)

  # ── Magic parity (crypto has none) ────────────────────────────────────── #

  def owned_magics(self) -> set:
    return set()

  def _magic_for(self, strategy: Optional[str]) -> Optional[int]:
    return None

  # ── Position queries ──────────────────────────────────────────────────── #

  def get_open_positions(
    self, symbol: str, strategy: Optional[str] = None
  ) -> List[ExchangePosition]:
    resolved = self.get_symbol(symbol)
    return [p for p in self._gateway.get_positions(resolved) if p.volume > 0]

  def get_all_open_positions(
    self, strategy: Optional[str] = None
  ) -> List[ExchangePosition]:
    return [p for p in self._gateway.get_positions() if p.volume > 0]

  # ── Entry ─────────────────────────────────────────────────────────────── #

  def open_position(self, signal: SignalSchema) -> TradeResult:
    action = signal.action.value  # LONG / SHORT
    if action not in ("LONG", "SHORT"):
      return TradeResult.fail("Action Mapping Failed")

    symbol = self.get_symbol(signal.symbol)

    if self._config.volume_decision_enabled:
      if signal.sl:
        price = self._gateway.get_mark_price(symbol)
        risk = (
          signal.risk_percent
          if signal.risk_percent is not None
          else self._config.risk_percentage
        )
        qty = self.calculate_quantity(symbol, price, signal.sl, risk, self._config.capital)
        logger.info(
          "[open_position] RISK mode | price=%s sl=%s risk=%s%% → qty=%s",
          price, signal.sl, risk, qty,
        )
      else:
        f = self._gateway.get_symbol_filter(symbol)
        qty = f.min_qty or 0.0
        logger.warning("[open_position] VOLUME_DECISION but no SL — using min qty=%s", qty)
    else:
      if signal.quantity is None:
        return TradeResult.fail("Missing quantity")
      qty = self.convert_quantity_to_lots(symbol, signal.quantity)

    qty = self.normalize_volume(symbol, qty)
    if qty <= 0:
      return TradeResult.fail("Computed quantity is zero")

    result = self._gateway.place_market_order(
      symbol,
      action,
      qty,
      reduce_only=False,
      client_order_id=self._client_order_id(signal.strategy, signal.signal_id),
    )
    if not result.get("success"):
      return result

    # Attach protective stop if the signal carried one.
    if signal.sl:
      sl_res = self._gateway.set_stop_loss(symbol, action, signal.sl, qty)
      result["sl_update"] = sl_res

    result.setdefault("volume", qty)
    return result

  # ── TP1: partial close ────────────────────────────────────────────────── #

  def partial_close_position(
    self,
    symbol: str,
    close_volume: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
  ) -> TradeResult:
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol, strategy=strategy)
    if not positions:
      return TradeResult.fail("No Positions Found")

    pos = (
      next((p for p in positions if p.ticket == position_ticket), positions[0])
      if position_ticket
      else positions[0]
    )
    safe_volume = self.normalize_volume(symbol, min(close_volume, pos.volume))
    if safe_volume <= 0:
      return TradeResult.fail("Close volume rounds to zero")

    result = self._gateway.place_market_order(
      resolved, pos.side, safe_volume, reduce_only=True
    )
    if result.get("success"):
      result["source_ticket"] = pos.ticket
    return result

  def update_position_sl(
    self,
    symbol: str,
    new_sl: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
  ) -> TradeResult:
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol, strategy=strategy)
    if not positions:
      return TradeResult.fail("No Positions Found")

    pos = (
      next((p for p in positions if p.ticket == position_ticket), positions[0])
      if position_ticket
      else positions[0]
    )
    # Replace any resting stop with the new one (breakeven after TP1).
    self._gateway.cancel_all_orders(resolved)
    result = self._gateway.set_stop_loss(resolved, pos.side, new_sl, pos.volume)
    if result.get("success"):
      result["new_sl"] = new_sl
      result["ticket"] = pos.ticket
    return result

  # ── Full close ────────────────────────────────────────────────────────── #

  def close_all_positions(
    self, symbol: str, reason: str = "CLOSE", strategy: Optional[str] = None
  ) -> TradeResult:
    resolved = self.get_symbol(symbol)
    positions = self.get_open_positions(symbol, strategy=strategy)
    if not positions:
      return TradeResult.fail("No Positions Found")

    self._gateway.cancel_all_orders(resolved)
    success_count = 0
    last_result: Optional[Any] = None
    for pos in positions:
      result = self._gateway.place_market_order(
        resolved, pos.side, pos.volume, reduce_only=True
      )
      if result.get("success"):
        success_count += 1
        last_result = result
      else:
        logger.error("[close_all] Failed to close %s: %s", resolved, result.get("comment"))

    if success_count > 0 and last_result is not None:
      return TradeResult.ok(
        ticket=last_result.get("ticket"),
        source_ticket=positions[0].ticket,
        price=last_result.get("price"),
        volume=last_result.get("volume"),
        comment=f"Closed {success_count} position(s) [{reason}]",
      )
    return TradeResult.fail(f"Failed to close [{reason}]")

  def close_single_position(self, pos: Any, reason: str = "FLAT") -> TradeResult:
    """Close one :class:`ExchangePosition` (its ``symbol`` is already resolved)."""
    result = self._gateway.place_market_order(
      pos.symbol, pos.side, pos.volume, reduce_only=True
    )
    if result.get("success"):
      result.setdefault("comment", f"Closed [{reason}]")
    return result

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _client_order_id(strategy: str, signal_id: Optional[str]) -> str:
    # Binance clientOrderId max length is 36; keep it short and unique-ish.
    suffix = (signal_id or "")[-6:]
    return f"{strategy[:20]}-{suffix}".strip("-")[:36]
