"""
worker/core/signal_handler.py
─────────────────────────────
Translates an incoming SignalSchema payload into the correct sequence of
market strategy calls, following the three-group logic defined in logic.md:

  Group 1  │ LONG / SHORT  → Open a fresh position (clear any stale one first)
  Group 2  │ TP1           → Partial close + move SL to breakeven (entry price)
  Group 3  │ TP2 / SL / R_SL / FLAT → Full close using ACTUAL volume (no signal qty)

SignalHandler is market-agnostic: it depends only on BaseMarketStrategy and
knows nothing about MT5, exchange APIs, or any concrete implementation.
"""

from typing import Any, Dict

from worker.core.market_strategy import BaseMarketStrategy
from worker.logger import get_logger
from worker.schemas.broker_schema import SignalActionEnum, SignalSchema

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
  - Routing each SignalActionEnum to the appropriate BaseMarketStrategy method.
  - Returning a structured result dict so the caller can log/notify.
  """

  def __init__(self, strategy: BaseMarketStrategy) -> None:
    self.strategy = strategy

  # ------------------------------------------------------------------ #
  #  Public entry-point                                                  #
  # ------------------------------------------------------------------ #

  def handle(self, signal: SignalSchema) -> Dict[str, Any]:
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

    # ── Group 3: Full exit (TP2 / SL / R_SL) ───────────────────────
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

  def _handle_entry(self, signal: SignalSchema) -> Dict[str, Any]:
    """
    1. Check for any stale position on this symbol and force-close it.
    2. Open a fresh LONG (BUY) or SHORT (SELL) market order.
    3. Hard SL is set on the broker server directly in the entry request.
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
      logger.info("[SignalHandler._handle_entry] Stale positions cleared.")

    # Step 2 — Open new position
    if signal.action == SignalActionEnum.LONG:
      result = self.strategy.entry_long(signal)
    else:
      result = self.strategy.entry_short(signal)

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

  def _handle_tp1(self, signal: SignalSchema) -> Dict[str, Any]:
    """
    1. Verify there is still an open position (guard against fast SL hit).
    2. Delegate to strategy.handle_tp1 which handles qty→lots conversion,
       partial close, and breakeven SL move.
    """
    symbol = signal.symbol

    # Guard: check position still exists
    positions = self.strategy.get_open_positions(symbol)
    if not positions:
      logger.warning(
        f"[SignalHandler._handle_tp1] No open position found for {symbol}. "
        "Likely hit hard SL already — skipping TP1 webhook."
      )
      return {
        "success": False,
        "retcode": -1,
        "comment": "No open position — likely SL already triggered.",
      }

    if signal.quantity is None:
      logger.error("[SignalHandler._handle_tp1] TP1 signal missing 'quantity' field.")
      return {
        "success": False,
        "retcode": -1,
        "comment": "Missing quantity in TP1 signal",
      }

    result = self.strategy.handle_tp1(signal)

    if result.get("success"):
      logger.info(
        f"[SignalHandler._handle_tp1] TP1 OK | "
        f"closed_vol={result.get('volume')} price={result.get('price')}"
      )
    else:
      logger.error(f"[SignalHandler._handle_tp1] TP1 FAILED: {result.get('comment')}")

    return result

  # ------------------------------------------------------------------ #
  #  Group 3 — Full close by ACTUAL volume (TP2 / SL / R_SL / FLAT)    #
  # ------------------------------------------------------------------ #

  def _handle_full_close(self, signal: SignalSchema) -> Dict[str, Any]:
    """
    1. Safety check: if nothing is open, log and return gracefully.
    2. Route to the correct strategy method based on signal action.
    3. State cleanup is implicit — once all positions are 0 the system
       is flat and ready for the next LONG/SHORT cycle.
    """
    symbol = signal.symbol
    action = signal.action

    # Safety check
    positions = self.strategy.get_open_positions(symbol)
    if not positions:
      logger.warning(
        f"[SignalHandler._handle_full_close] No open positions for {symbol} "
        f"on action={action.value}. May have been closed by hard SL already."
      )
      return {
        "success": False,
        "retcode": -1,
        "comment": f"No open positions to close [{action.value}]",
      }

    if action == SignalActionEnum.TP2:
      result = self.strategy.handle_tp2(signal)
    elif action == SignalActionEnum.SL:
      result = self.strategy.handle_sl(signal)
    elif action == SignalActionEnum.FLAT:
      result = self.strategy.handle_flat(signal)
    else:  # R_SL
      result = self.strategy.handle_r_sl(signal)

    if result.get("success"):
      logger.info(
        f"[SignalHandler._handle_full_close] Full close OK | "
        f"action={action.value} vol={result.get('volume')} price={result.get('price')}"
      )
    else:
      logger.error(
        f"[SignalHandler._handle_full_close] Full close FAILED | "
        f"action={action.value} retcode={result.get('retcode')} comment={result.get('comment')}"
      )

    return result
