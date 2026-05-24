"""
worker/core/signal_handler.py
─────────────────────────────
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

from worker.core.market_strategy import BaseMarketStrategy
from worker.logger import get_logger
from worker.schemas.broker_schema import SignalActionEnum, SignalSchema
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.trade_result import TradeResult
from worker.services.db_protocol import DBServiceProtocol

logger = get_logger("worker.core.signal_handler")

# Actions that belong to the "full close" group
_FULL_CLOSE_ACTIONS = {
  SignalActionEnum.TP2,
  SignalActionEnum.SL,
  SignalActionEnum.R_SL,
  SignalActionEnum.FLAT,
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

  def __init__(self, strategy: BaseMarketStrategy, db_service: DBServiceProtocol) -> None:
    self.strategy = strategy
    self._db = db_service

  # ------------------------------------------------------------------ #
  #  SQLite lookup helper                                                #
  # ------------------------------------------------------------------ #

  def _get_db_position(self, strategy_name: str, symbol: str) -> Optional[Dict[str, Any]]:
    """Return the first open/TP1 position for strategy_name+symbol from SQLite, or None."""
    positions = self._db.get_open_positions_by_strategy(strategy_name, symbol)
    if not positions:
      return None
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
      return {"success": False, "retcode": -1, "comment": no_db_comment}

    logger.info(
      f"[SignalHandler] DB position found | "
      f"source_ticket={db_pos['source_ticket']} ticket={db_pos['ticket']} status={db_pos['status']}"
    )

    if not self.strategy.get_open_positions(signal.symbol):
      logger.warning(
        f"[SignalHandler] No live MT5 position for {signal.symbol} "
        f"action={signal.action.value}. May have been closed already."
      )
      return {"success": False, "retcode": -1, "comment": no_mt5_comment}

    result = strategy_fn(signal)
    if result.get("success"):
      result["source_ticket"] = db_pos["source_ticket"]
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

    # ── Group 1: Entry ──────────────────────────────────────────────
    if action in (SignalActionEnum.LONG, SignalActionEnum.SHORT):
      return self._handle_entry(signal)

    # ── Group 2: Partial exit (TP1) ─────────────────────────────────
    if action == SignalActionEnum.TP1:
      return self._handle_tp1(signal)

    # ── Group 3: Full exit (TP2 / SL / R_SL / FLAT) ─────────────────
    if action in _FULL_CLOSE_ACTIONS:
      return self._handle_full_close(signal)

    # Unknown action — should never reach here given validated enum
    logger.error(f"[SignalHandler] Unknown action '{action.value}' — skipping.")
    return {
      "success": False,
      "retcode": -1,
      "comment": f"Unknown action: {action.value}",
    }

  # ------------------------------------------------------------------ #
  #  Group 1 — Open position (LONG / SHORT)                             #
  # ------------------------------------------------------------------ #

  def _handle_entry(self, signal: SignalSchema) -> TradeResult:
    """
    1. Check for any stale position on this symbol and force-close it.
    2. Open a fresh position via strategy.entry (direction in signal.action).
    """
    symbol = signal.symbol

    # Step 1 — Pre-flight: close stale positions to start clean
    stale = self.strategy.get_open_positions(symbol)
    if stale:
      logger.warning(
        f"[SignalHandler._handle_entry] Found {len(stale)} stale position(s) "
        f"for {symbol}. Force-closing before entering new trade."
      )
      cleanup = self.strategy.close_all_positions(symbol, reason="STALE_CLEANUP")
      if not cleanup.get("success"):
        logger.error(
          f"[SignalHandler._handle_entry] Failed to clear stale positions: "
          f"{cleanup.get('comment')}. Aborting entry."
        )
        return {
          "success": False,
          "retcode": cleanup.get("retcode", -1),
          "comment": f"Stale position cleanup failed: {cleanup.get('comment')}",
        }

      # Update DB records for force-closed positions
      db_stale = self._db.get_open_positions_by_strategy(signal.strategy, symbol)
      for pos in db_stale:
        self._db.update_position_status(
          source_ticket=pos["source_ticket"],
          status=PositionStatusEnum.FORCED_CLOSED,
          closed_price=cleanup.get("price"),
          mt5_retcode=cleanup.get("retcode"),
          comment="Force-closed by new entry signal",
        )
      logger.info(
        f"[SignalHandler._handle_entry] Stale positions cleared, "
        f"{len(db_stale)} DB record(s) marked FORCED_CLOSED."
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
