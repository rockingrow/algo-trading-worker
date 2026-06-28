"""
worker/crypto/executor.py
─────────────────────────
Crypto order executor — the CRYPTO counterpart of ``ForexExecutor``.

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

import json
import math
import re
import uuid
from decimal import Decimal
from typing import Any, List, Optional

from worker.gateways.config import ExecutionConfig
from worker.gateways.crypto.base import (
  WORKER_ORDER_PREFIX,
  BaseExchangeGateway,
  ExchangePosition,
)
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
    """Resolve a signal symbol to the exchange symbol.

    Handles the common upstream forms: an exchange prefix (``BINANCE:BTCUSDT.P``,
    ``OANDA:XAUUSD``) is dropped to its last ``:``-segment, a ``.P`` perpetual
    suffix is stripped, and a ``USD`` quote is normalized to the configured quote
    asset (e.g. ``BTCUSD`` → ``BTCUSDT``).
    """
    s = base_symbol.upper().split(":")[-1].replace("/", "")
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

    qty = self._enforce_min_notional(symbol, qty)
    if qty is None:
      return TradeResult.fail("Computed quantity below MIN_NOTIONAL and cannot be bumped")

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

    return self._attach_entry_orders(symbol, action, qty, signal, result)

  def _attach_entry_orders(
    self, symbol: str, action: str, qty: float, signal: SignalSchema, result: dict
  ) -> TradeResult:
    """Place SL and TP2 resting orders after the entry market order succeeds."""
    if signal.sl:
      sl_res = self._gateway.set_stop_loss(symbol, action, signal.sl, qty)
      result["sl_update"] = sl_res
      if not sl_res.get("success"):
        return self._rollback_unprotected_entry(symbol, action, result, sl_res, qty)

    if signal.tp2:
      tp_res = self._gateway.set_take_profit(symbol, action, signal.tp2, qty)
      result["tp_update"] = tp_res
      if not tp_res.get("success"):
        logger.warning(
          "[open_position] TP placement failed (%s) for %s — position remains "
          "SL-protected; continuing without a resting take-profit.",
          tp_res.get("comment"), symbol,
        )

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
      if self._config.use_custom_risk_percentage:
        risk = self._config.risk_percentage
      else:
        # A missing OR non-positive signal risk (upstream sends 0.0 when it has no
        # opinion) means "unspecified": fall back to RISK_PERCENTAGE rather than
        # sizing at 0%, which would zero risk_cash and floor entry to the min qty.
        use_signal_risk = signal.risk_percent is not None and signal.risk_percent > 0
        risk = signal.risk_percent if use_signal_risk else self._config.risk_percentage
      # Scale-in: the broker's pre-scaled signal.quantity is ignored in risk mode,
      # so re-apply the scale-in factor here (1.0 for a normal entry). Sizing is
      # linear in risk, so scaling risk scales the resulting qty by the same factor.
      scale_factor = signal.scale_quantity_factor()
      risk *= scale_factor
      qty = self.calculate_quantity(symbol, price, signal.sl, risk, capital)
      logger.info(
        "[open_position] RISK mode | price=%s sl=%s risk=%s%% capital=%s "
        "scale_factor=%s → qty=%s",
        price, signal.sl, risk, capital, scale_factor, qty,
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

  def _enforce_min_notional(self, symbol: str, qty: float) -> Optional[float]:
    """Bump *qty* up to the MIN_NOTIONAL floor if needed.

    Returns the (possibly bumped) qty, or None if a price cannot be fetched
    so the caller can fail cleanly rather than sending a doomed order.
    """
    f = self._gateway.get_symbol_filter(symbol)
    if not f.min_notional or f.min_notional <= 0:
      return qty
    try:
      price = self._gateway.get_mark_price(symbol)
    except Exception:
      logger.exception("[open_position] Could not fetch mark price for notional check (%s)", symbol)
      return None
    if price <= 0 or qty * price >= f.min_notional:
      return qty
    bumped = self.normalize_volume(symbol, f.min_notional / price)
    if bumped <= qty:
      # step-size rounding floored us back below notional — go one step higher
      step = f.step_size or 0.0
      bumped = self.normalize_volume(symbol, bumped + step) if step > 0 else bumped
    logger.warning(
      "[open_position] qty %.8f * price %.4f = %.4f below MIN_NOTIONAL %.4f — bumped to %.8f",
      qty, price, qty * price, f.min_notional, bumped,
    )
    return bumped

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
      symbol, action, close_qty, reduce_only=True,
      client_order_id=self._close_client_order_id(),
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
      resolved, pos.side, safe_volume, reduce_only=True,
      client_order_id=self._close_client_order_id(),
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
    # Moving the stop to breakeven cancels all resting orders (the old stop) —
    # which on Binance also clears the resting take-profit. Re-place the breakeven
    # stop, then re-establish the TP2 target so it survives the move: the strategy
    # keeps the runner's take-profit intact; only the stop changes.
    self._gateway.cancel_all_orders(resolved)
    result = self._gateway.set_stop_loss(resolved, pos.side, new_sl, pos.volume)
    if result.get("success"):
      result["new_sl"] = new_sl
      result["ticket"] = str(pos.ticket)
      tp2 = self._recover_entry_tp2(symbol, strategy)
      if tp2:
        tp_res = self._gateway.set_take_profit(resolved, pos.side, tp2, pos.volume)
        result["tp_update"] = tp_res
        if not tp_res.get("success"):
          logger.warning(
            "[update_position_sl] TP2 re-placement failed (%s) for %s — breakeven "
            "stop is set but the take-profit target is no longer resting.",
            tp_res.get("comment"), resolved,
          )
    return result

  def _recover_entry_tp2(self, symbol: str, strategy: Optional[str]) -> Optional[float]:
    """Recover the original TP2 target from the persisted entry signal.

    The breakeven SL move cancels all resting orders (including the take-profit),
    so re-establishing TP2 needs its price. The active position row stores the
    original entry signal JSON in ``gateway_message``; read TP2 back from there so
    no extra schema column or exchange round-trip is required. *symbol* is the
    raw signal symbol (matching what the DB stores), not the resolved one.
    """
    if self._db is None or not strategy:
      return None
    getter = getattr(self._db, "get_open_positions_by_strategy", None)
    if getter is None:
      return None
    try:
      rows = getter(strategy, symbol)
    except Exception:
      logger.exception("[update_position_sl] Could not read DB to recover TP2 (%s).", symbol)
      return None
    for row in rows:
      raw = row.get("gateway_message")
      if not raw:
        continue
      try:
        tp2 = json.loads(raw).get("tp2")
      except (TypeError, ValueError):
        continue
      if tp2:
        return float(tp2)
    return None

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
        resolved, pos.side, pos.volume, reduce_only=True,
        client_order_id=self._close_client_order_id(),
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
      pos.symbol, pos.side, pos.volume, reduce_only=True,
      client_order_id=self._close_client_order_id(),
    )
    if result.get("success"):
      result.setdefault("comment", f"Closed [{reason}]")
    return result

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _client_order_id(strategy: str, signal_id: Optional[str]) -> str:
    # Binance clientOrderId charset: ^[\.A-Z\:/a-z0-9_-]{1,36}$
    safe = re.sub(r"[^\w.:/\-]", "_", strategy)
    suffix = (signal_id or "")[-6:]
    return f"{safe[:20]}-{suffix}".strip("-")[:36]

  @staticmethod
  def _close_client_order_id() -> str:
    """A unique clientOrderId tagging a worker-placed reduce-only close.

    The ``WORKER_ORDER_PREFIX`` marker lets the exchange event stream recognise
    this fill as the worker's own close and skip it, so it is not re-handled as
    an external (manual/liquidation) close. A uuid suffix keeps it unique among
    open orders; total length (5 + 16 = 21) stays within Binance's 36-char limit.
    """
    return f"{WORKER_ORDER_PREFIX}{uuid.uuid4().hex[:16]}"
