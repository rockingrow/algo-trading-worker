"""
worker/crypto/executor.py
─────────────────────────
Crypto order executor — the CRYPTO counterpart of ``MT5Executor``.

Implements :class:`~worker.interfaces.executor_protocol.TradeExecutorProtocol`
so :class:`~worker.gateways.market_strategy.CryptoMarket` drives it exactly like the
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
from decimal import Decimal
from typing import Any, List, Optional

from worker.gateways.config import ExecutionConfig
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
    """Resolve a signal symbol to the exchange symbol (e.g. BTCUSDT.P → BTCUSDT)."""
    s = base_symbol.upper().replace("/", "").replace(":", "")
    if s.endswith(".P"):
      s = s[:-2]
    if s.endswith(self._quote):
      return s
    if s.endswith("USD"):
      return s[:-3] + self._quote
    return s + self._quote

  def normalize_volume(self, symbol: str, volume: float) -> float:
    """Round *volume* down to the symbol's step size."""
    f = self._gateway.get_symbol_filter(self.get_symbol(symbol))
    step = f.step_size or 0.0
    if step <= 0:
      return round(volume, 8)
    # Epsilon prevents under-floor from float division (e.g. 0.3/0.1 = 2.999…).
    steps = math.floor(volume / step + 1e-9)
    # Count decimal places from the canonical string so 0.001→3 without log10 drift.
    decimals = max(0, -Decimal(str(step)).normalize().as_tuple().exponent)
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

    if not self._config.volume_decision_enabled and signal.quantity is None:
      return TradeResult.fail("Missing quantity")

    qty = self.normalize_volume(symbol, self._resolve_entry_qty(signal, symbol))
    if qty <= 0:
      return TradeResult.fail("Computed quantity is zero")

    conflict = self._netting_conflict(signal, symbol)
    if conflict is not None:
      return conflict

    result = self._gateway.place_market_order(
      symbol,
      action,
      qty,
      reduce_only=False,
      client_order_id=self._client_order_id(signal.strategy, signal.signal_id),
    )
    if not result.get("success"):
      return result

    # Attach protective stop if the signal carried one. A position without its
    # protective stop is unacceptable: if the stop fails to register, roll the
    # entry back (reduce-only close) rather than leave unprotected exposure.
    if signal.sl:
      sl_res = self._gateway.set_stop_loss(symbol, action, signal.sl, qty)
      result["sl_update"] = sl_res
      if not sl_res.get("success"):
        return self._rollback_unprotected_entry(symbol, action, result, sl_res, qty)

    result.setdefault("volume", qty)
    return result

  def _resolve_entry_qty(self, signal: SignalSchema, symbol: str) -> float:
    """Entry quantity before final step-size normalization: risk-based when
    VOLUME_DECISION is on (min qty if the signal carries no SL), otherwise the
    signal's own quantity. Caller guarantees a quantity exists in the latter case."""
    if not self._config.volume_decision_enabled:
      return self.convert_quantity_to_lots(symbol, signal.quantity)
    if signal.sl:
      capital = self._risk_capital()
      if capital is None:
        qty = self._min_qty(symbol)
        logger.error(
          "[open_position] USE_ACCOUNT_EQUITY set but equity unavailable — using min qty=%s",
          qty,
        )
        return qty
      price = self._gateway.get_mark_price(symbol)
      # A missing OR non-positive signal risk (upstream sends 0.0 when it has no
      # opinion) means "unspecified": fall back to RISK_PERCENTAGE rather than
      # sizing at 0%, which would zero risk_cash and floor entry to the min qty.
      use_signal_risk = signal.risk_percent is not None and signal.risk_percent > 0
      risk = signal.risk_percent if use_signal_risk else self._config.risk_percentage
      qty = self.calculate_quantity(symbol, price, signal.sl, risk, capital)
      logger.info(
        "[open_position] RISK mode | price=%s sl=%s risk=%s%% capital=%s → qty=%s",
        price, signal.sl, risk, capital, qty,
      )
      return qty
    qty = self._min_qty(symbol)
    logger.warning("[open_position] VOLUME_DECISION but no SL — using min qty=%s", qty)
    return qty

  def _risk_capital(self) -> Optional[float]:
    """Capital base for risk sizing.

    Mirrors the Forex ``LotSizer``: live account equity when ``USE_ACCOUNT_EQUITY``
    is set, otherwise the fixed configured ``CAPITAL``. Returns ``None`` when
    equity is required but cannot be read, signalling the caller to fall back to
    the symbol's minimum quantity rather than silently sizing off the wrong base.
    """
    if not self._config.use_account_equity:
      return self._config.capital
    account = self._gateway.get_account()
    equity = account.get("equity") if account else None
    if not equity:
      logger.error(
        "[open_position] USE_ACCOUNT_EQUITY set but account equity unavailable (%s).",
        account,
      )
      return None
    return float(equity)

  def _min_qty(self, symbol: str) -> float:
    """The symbol's minimum order quantity (conservative sizing floor)."""
    f = self._gateway.get_symbol_filter(symbol)
    return f.min_qty or 0.0

  def _netting_conflict(self, signal: SignalSchema, symbol: str) -> Optional[TradeResult]:
    """In netting mode, reject an entry that would merge with another strategy's
    position on the same symbol — unless the operator opted in. Returns a failed
    :class:`TradeResult` on conflict, else ``None``."""
    if self._config.allow_multi_strategy_per_symbol or self._db is None:
      return None
    get_for_symbol = getattr(self._db, "get_open_positions_for_flat", None)
    if get_for_symbol is None:
      return None
    open_rows = get_for_symbol(symbol=signal.symbol)
    other_strategies = {r["strategy"] for r in open_rows if r["strategy"] != signal.strategy}
    if other_strategies:
      return TradeResult.fail(
        f"Netting conflict: {symbol} already held by {other_strategies}. "
        "Set CRYPTO_ALLOW_MULTI_STRATEGY_PER_SYMBOL=true to override.",
        retcode=-1,
      )
    return None

  def _rollback_unprotected_entry(
    self, symbol: str, action: str, entry: TradeResult, sl_res: Any, qty: float
  ) -> TradeResult:
    """Close a just-opened position whose protective stop failed to register.

    Returns a *failed* result so the position is never persisted as OPENED — a
    filled entry with no stop is worse than no entry at all.
    """
    close_qty = entry.get("volume") or qty
    logger.error(
      "[open_position] SL placement failed (%s) — closing the just-opened %s %s "
      "(qty=%s) to avoid unprotected exposure.",
      sl_res.get("comment"), action, symbol, close_qty,
    )
    close_res = self._gateway.place_market_order(
      symbol, action, close_qty, reduce_only=True
    )
    if close_res.get("success"):
      fail = TradeResult.fail(
        f"Entry rolled back: stop-loss failed ({sl_res.get('comment')})",
        retcode=sl_res.get("retcode", -1),
      )
    else:
      # Could not flatten — the position is OPEN WITHOUT a stop. Escalate loudly.
      logger.critical(
        "[open_position] ROLLBACK FAILED for %s %s: position is OPEN WITHOUT a "
        "stop and could not be closed (%s). Manual intervention required.",
        action, symbol, close_res.get("comment"),
      )
      fail = TradeResult.fail(
        f"UNPROTECTED POSITION: stop-loss failed ({sl_res.get('comment')}) and "
        f"rollback close failed ({close_res.get('comment')}) — close manually",
        retcode=sl_res.get("retcode", -1),
      )
    fail["sl_update"] = sl_res
    fail["rollback"] = close_res
    return fail

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
      result["source_ticket"] = str(pos.ticket)
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
      result["ticket"] = str(pos.ticket)
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
        logger.error(
          "[close_all] Failed to close %s: %s", resolved, result.get("comment")
        )

    if success_count > 0 and last_result is not None:
      return TradeResult.ok(
        ticket=last_result.get("ticket"),
        source_ticket=str(positions[0].ticket),
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
