"""
worker/gateways/signal_handler.py
──────────────────────────────────
Translates an incoming SignalSchema payload into the correct sequence of
market strategy calls, following the three-group logic defined in logic.md:

  Group 1  │ LONG / SHORT  → Open a fresh position (clear any stale one first)
  Group 2  │ TP1           → Partial close + move SL to breakeven (entry price)
  Group 3  │ TP2 / SL / R_SL / FLAT → Full close using ACTUAL volume (no signal qty)

SignalHandler queries SQLite first for exit signals to obtain the tracked
source_ticket, ensuring DB updates always target the correct record even
when the broker re-tickets a position after a partial close.
"""

from typing import Any, Callable, Dict, Optional

from worker.gateways.market_strategy import BaseMarketStrategy
from worker.interfaces.db_protocol import PositionStoreProtocol
from worker.logger import get_logger
from worker.schemas.trade_result import TradeResult
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalActionEnum, SignalSchema

logger = get_logger("worker.gateways.signal_handler")

# Actions that route to the shared "full close" handler. FLAT is intentionally
# NOT here — it has its own strategy-scoped handler (_handle_flat).
_FULL_CLOSE_ACTIONS = {
  SignalActionEnum.TP2,
  SignalActionEnum.SL,
  SignalActionEnum.R_SL,
}


class SignalHandler:
  """
  Orchestrates the correct market execution sequence for every incoming signal.

  Responsibilities:
  - Pre-flight checks (stale position cleanup for LONG/SHORT).
  - SQLite lookup for exit signals to resolve the tracked source_ticket.
  - Routing each SignalActionEnum to the appropriate BaseMarketStrategy method.
  - Returning a structured result dict so the caller can log/notify.
  """

  def __init__(
    self, strategy: BaseMarketStrategy, db_service: PositionStoreProtocol
  ) -> None:
    self.strategy = strategy
    self._db = db_service
    # Action → handler dispatch table. Adding a new action group means adding an
    # entry here (and to the relevant set) rather than editing handle()'s body
    # (Open/Closed).
    self._routes: Dict[SignalActionEnum, Callable[[SignalSchema], TradeResult]] = {
      SignalActionEnum.LONG: self._handle_entry,
      SignalActionEnum.SHORT: self._handle_entry,
      SignalActionEnum.TP1: self._handle_tp1,
      **dict.fromkeys(_FULL_CLOSE_ACTIONS, self._handle_full_close),
      SignalActionEnum.FLAT: self._handle_flat,
    }

  # ------------------------------------------------------------------ #
  #  SQLite lookup helper                                                #
  # ------------------------------------------------------------------ #

  def _get_db_position(
    self, strategy_name: str, symbol: str
  ) -> Optional[Dict[str, Any]]:
    """Return the single open/TP1 position for strategy_name+symbol from SQLite, or None.

    Invariant: at most one active position per (strategy, symbol) should exist
    at any time. If more than one is found (data inconsistency from a prior
    crash), keep the oldest row and mark the extras FORCED_CLOSED so the DB
    self-heals rather than silently ignoring the duplicates.
    """
    positions = self._db.get_open_positions_by_strategy(strategy_name, symbol)
    if not positions:
      return None
    if len(positions) > 1:
      logger.error(
        "[SignalHandler] Data inconsistency: %d active DB rows for strategy=%s symbol=%s. "
        "Keeping source_ticket=%s, marking %d extra(s) FORCED_CLOSED.",
        len(positions),
        strategy_name,
        symbol,
        positions[0]["ref_source_id"],
        len(positions) - 1,
      )
      for dup in positions[1:]:
        self._db.update_position_status(
          ref_source_id=dup["ref_source_id"],
          status=PositionStatusEnum.FORCED_CLOSED,
          comment="Duplicate active position — healed by signal handler",
        )
    return positions[0]

  # ------------------------------------------------------------------ #
  #  Template: DB lookup + MT5 guard + inject source_ticket              #
  # ------------------------------------------------------------------ #

  def _execute_exit(
    self,
    signal: SignalSchema,
    strategy_fn: Callable[[SignalSchema], TradeResult],
    *,
    no_db_comment: str,
    no_mt5_comment: str,
  ) -> TradeResult:
    """
    Shared skeleton for all exit-type signals (TP1, full close).

    1. DB lookup — early return if no tracked position.
    2. Live MT5 guard — early return if position already gone.
    3. Execute *strategy_fn*.
    4. Inject source_ticket from DB into a successful result.
    """
    db_pos = self._get_db_position(signal.strategy, signal.symbol)
    if not db_pos:
      logger.warning(
        f"[SignalHandler] No DB record for strategy={signal.strategy} "
        f"symbol={signal.symbol} action={signal.action.value}. "
        "Position may have been closed already."
      )
      return TradeResult.fail(no_db_comment)

    logger.info(
      f"[SignalHandler] DB position found | "
      f"ref_source_id={db_pos['ref_source_id']} ref_id={db_pos['ref_id']} status={db_pos['status']}"
    )

    if not self.strategy.get_open_positions(signal.symbol, strategy=signal.strategy):
      logger.warning(
        f"[SignalHandler] No live MT5 position for strategy={signal.strategy} "
        f"symbol={signal.symbol} action={signal.action.value}. "
        "May have been closed already."
      )
      return TradeResult.fail(no_mt5_comment)

    result = strategy_fn(signal)
    if result.get("success"):
      result["source_ticket"] = db_pos["ref_source_id"]
    return result

  # ------------------------------------------------------------------ #
  #  Public entry-point                                                  #
  # ------------------------------------------------------------------ #

  def handle(self, signal: SignalSchema) -> TradeResult:
    """
    Process one incoming webhook signal end-to-end.

    Returns a result dict with at minimum:
      { "success": bool, "retcode": int, "comment": str, ... }
    """
    action = signal.action
    symbol = signal.symbol

    logger.info(
      f"[SignalHandler] Handling signal | symbol={symbol} "
      f"action={action.value} | ts={signal.timestamp}"
    )

    route = self._routes.get(action)
    if route is None:
      # Unknown action — should never reach here given validated enum
      logger.error(f"[SignalHandler] Unknown action '{action.value}' — skipping.")
      return TradeResult.fail(f"Unknown action: {action.value}")
    return route(signal)

  # ------------------------------------------------------------------ #
  #  Group 1 — Open position (LONG / SHORT)                             #
  # ------------------------------------------------------------------ #

  def _handle_entry(self, signal: SignalSchema) -> TradeResult:
    """
    1. Reject if another strategy already holds this symbol (netting conflict).
    2. Check for any stale position on this symbol and force-close it.
    3. Open a fresh position via strategy.entry (direction in signal.action).
    """
    symbol = signal.symbol
    strategy = signal.strategy

    # Guard: reject if another strategy already holds this symbol in the DB.
    # This MUST run before the stale-cleanup below because cancel_all_orders is
    # symbol-scoped, not strategy-scoped — proceeding while another strategy has
    # resting orders on the symbol would silently wipe their SL/TP.
    other_holders = {
      r["strategy"]
      for r in self._db.get_open_positions_for_flat(symbol=symbol)
      if r.get("strategy") != strategy
    }
    if other_holders:
      logger.warning(
        "[SignalHandler._handle_entry] Netting conflict for %s: already held by "
        "%s — rejecting %s entry to protect their orders.",
        symbol, sorted(other_holders), strategy,
      )
      return TradeResult.fail(
        f"Netting conflict: {symbol} already held by {sorted(other_holders)}.",
        retcode=-1,
      )

    # Step 1 — Pre-flight: close this strategy's stale position to start clean.
    # Scoped to signal.strategy so a concurrent strategy holding a position on
    # the same symbol is untouched (enforced by the guard above).
    forced_closed: list[dict] = []
    cleanup_price: Optional[float] = None
    cleanup_retcode: Optional[int] = None
    stale = self.strategy.get_open_positions(symbol, strategy=strategy)
    if stale:
      logger.warning(
        f"[SignalHandler._handle_entry] Found {len(stale)} stale position(s) "
        f"for strategy={strategy} symbol={symbol}. "
        "Force-closing before entering new trade."
      )
      cleanup = self.strategy.close_all_positions(
        symbol, reason="STALE_CLEANUP", strategy=strategy
      )
      if not cleanup.get("success"):
        logger.error(
          f"[SignalHandler._handle_entry] Failed to clear stale positions: "
          f"{cleanup.get('comment')}. Aborting entry."
        )
        return TradeResult.fail(
          f"Stale position cleanup failed: {cleanup.get('comment')}",
          retcode=cleanup.get("retcode", -1),
        )
      cleanup_price = cleanup.get("price")
      cleanup_retcode = cleanup.get("retcode")

    # Reconcile DB records for any active (OPENED/TP1) row on this (strategy,
    # symbol) — independently of whether the broker still had a live position.
    # A prior position may have been closed externally (SL/liquidation on the
    # exchange, manual close, or a missed close event), leaving an orphaned
    # OPENED row that the broker no longer reports. If we skip it here, the
    # fresh position below would violate the one-active-per-(strategy,symbol)
    # unique index on insert, leaving the new trade live but untracked.
    db_stale = self._db.get_open_positions_by_strategy(signal.strategy, symbol)
    if db_stale:
      comment = (
        "Force-closed by new entry signal"
        if stale
        else "Orphaned DB row cleared by new entry (not live on broker)"
      )
      for pos in db_stale:
        self._db.update_position_status(
          ref_source_id=pos["ref_source_id"],
          status=PositionStatusEnum.FORCED_CLOSED,
          closed_price=cleanup_price,
          gateway_return_code=cleanup_retcode,
          comment=comment,
        )
        forced_closed.append(
          {
            "ref_source_id": pos["ref_source_id"],
            "ref_id": pos["ref_id"],
            "price": cleanup_price,
            "volume": pos.get("volume"),
            "retcode": cleanup_retcode,
          }
        )
      logger.info(
        f"[SignalHandler._handle_entry] Cleared {len(db_stale)} active DB "
        f"record(s) for strategy={strategy} symbol={symbol} "
        f"(broker_had_position={bool(stale)})."
      )

    # Step 2 — Open new position
    result = self.strategy.entry(signal)

    if result.get("success"):
      logger.info(
        f"[SignalHandler._handle_entry] Entry OK | "
        f"action={signal.action.value} ticket={result.get('ticket')} "
        f"vol={result.get('volume')} price={result.get('price')}"
      )
    else:
      logger.error(
        f"[SignalHandler._handle_entry] Entry FAILED | "
        f"retcode={result.get('retcode')} comment={result.get('comment')}"
      )

    if forced_closed:
      result["forced_closed"] = forced_closed
    return result

  # ------------------------------------------------------------------ #
  #  Group 2 — Partial close + move SL to breakeven (TP1)               #
  # ------------------------------------------------------------------ #

  def _handle_tp1(self, signal: SignalSchema) -> TradeResult:
    result = self._execute_exit(
      signal,
      self.strategy.handle_tp1,
      no_db_comment=f"No tracked opening position in DB for {signal.symbol}",
      no_mt5_comment="No open position — likely SL already triggered.",
    )
    if result.get("success"):
      logger.info(
        f"[SignalHandler._handle_tp1] TP1 OK | "
        f"source_ticket={result.get('source_ticket')} "
        f"closed_vol={result.get('volume')} price={result.get('price')}"
      )
    else:
      logger.error(f"[SignalHandler._handle_tp1] TP1 FAILED: {result.get('comment')}")
    return result

  # ------------------------------------------------------------------ #
  #  Group 3 — Full close by ACTUAL volume (TP2 / SL / R_SL / FLAT)    #
  # ------------------------------------------------------------------ #

  def _handle_full_close(self, signal: SignalSchema) -> TradeResult:
    result = self._execute_exit(
      signal,
      self.strategy.handle_full_close,
      no_db_comment=f"No tracked opening position in DB for {signal.symbol} [{signal.action.value}]",
      no_mt5_comment=f"No open positions to close [{signal.action.value}]",
    )
    if result.get("success"):
      logger.info(
        f"[SignalHandler._handle_full_close] Full close OK | "
        f"action={signal.action.value} source_ticket={result.get('source_ticket')} "
        f"vol={result.get('volume')} price={result.get('price')}"
      )
    else:
      logger.error(
        f"[SignalHandler._handle_full_close] Full close FAILED | "
        f"action={signal.action.value} retcode={result.get('retcode')} comment={result.get('comment')}"
      )
    return result

  def _handle_flat(self, signal: SignalSchema) -> TradeResult:
    """FLAT: close all MT5 positions regardless of DB state, then sync DB.

    Unlike TP2/SL/R_SL, FLAT must not be gated on a DB record. When the DB
    is out of sync (position exists in MT5 but not in DB), other full-close
    handlers would bail early and leave the position open. FLAT always
    attempts the MT5 close first, then reconciles the DB afterward.
    """
    result = self.strategy.close_all_positions(
      signal.symbol, reason="FLAT", strategy=signal.strategy
    )

    if not result.get("success"):
      # Strategy-scoped lookup may have returned empty because the DB is out of
      # sync (positions exist in MT5 but have no DB record, so _filter_by_strategy
      # drops them). Retry without the strategy filter to catch those positions.
      logger.warning(
        "[SignalHandler._handle_flat] Strategy-scoped close found no positions for "
        "strategy=%s symbol=%s — retrying without strategy filter (DB may be out of sync)",
        signal.strategy,
        signal.symbol,
      )
      result = self.strategy.close_all_positions(
        signal.symbol, reason="FLAT", strategy=None
      )

    if result.get("success"):
      db_pos = self._get_db_position(signal.strategy, signal.symbol)
      if db_pos:
        result["source_ticket"] = db_pos["ref_source_id"]
      logger.info(
        "[SignalHandler._handle_flat] FLAT OK | source_ticket=%s vol=%s price=%s",
        result.get("source_ticket"),
        result.get("volume"),
        result.get("price"),
      )
      return result

    # MT5 had no positions to close — check if DB has a stale open record
    db_pos = self._get_db_position(signal.strategy, signal.symbol)
    if db_pos:
      logger.warning(
        "[SignalHandler._handle_flat] No MT5 positions but DB has open record — "
        "marking ref_source_id=%s as FLATTED",
        db_pos["ref_source_id"],
      )
      self._db.update_position_status(
        ref_source_id=db_pos["ref_source_id"],
        status=PositionStatusEnum.FLATTED,
        comment="FLAT signal: position not found in MT5 — DB synced",
        message=signal.model_dump_json(),
      )

    logger.error(
      "[SignalHandler._handle_flat] FLAT FAILED | retcode=%s comment=%s",
      result.get("retcode"),
      result.get("comment"),
    )
    return result
