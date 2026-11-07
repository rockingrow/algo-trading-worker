"""
worker/gateways/trade_notifications.py
──────────────────────────────────────
Trade notifications for :class:`~worker.gateways.processor.BaseSignalProcessor`.

Every action of one trade folds into a single channel message that is rewritten
in place (see :mod:`worker.services.cycle_notification_service`); the
per-action message each call site builds stays the fallback when there is no
cycle to fold into. Shared by the signal path, the ADMIN FLAT and the
market-specific close handlers, which is why it sits in a mixin of its own.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from worker.logger import get_logger
from worker.schemas.cycle_schema import (
  CycleEventSchema,
  CycleOutcomeEnum,
  CycleStatusEnum,
)
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalActionEnum, SignalSchema

# Same logger as the processor: these methods are part of its pipeline,
# and operators filter its log lines by this one name.
log = get_logger("worker.gateways.processor")

# Actions that close (part of) a position, and are therefore the only ones that
# can book a realized PnL onto the cycle.
_CYCLE_EXIT_ACTIONS = frozenset(
  {
    SignalActionEnum.TP1,
    SignalActionEnum.TP2,
    SignalActionEnum.SL,
    SignalActionEnum.R_SL,
    SignalActionEnum.FLAT,
  }
)


# Exit action → DB status, shared by every market.
_CLOSE_STATUS_MAP: Dict[str, PositionStatusEnum] = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
  "FLAT": PositionStatusEnum.FLATTED,
}


def _as_text(value: Any) -> Optional[str]:
  """Broker reference as a string, or None when absent.

  Ticket ids arrive as ints from some gateways and strings from others; the
  cycle stores them as text so the two render identically."""
  return None if value is None else str(value)


class TradeNotificationsMixin:
  """Per-trade cycle messages, with the per-action message as the fallback."""

  # ── Signal-cycle notifications ────────────────────────────────────────── #
  #
  # Every action of one trade folds into a single channel message that is
  # rewritten in place (see
  # :mod:`worker.services.cycle_notification_service`). The per-action message
  # each call site already builds stays the fallback: a broker that sends no
  # ``signal_uxid``, a cycle write that failed, or cycles turned off all mean
  # there is no message to edit, and the action must still be reported.

  def _notify_trade(
    self,
    signal: SignalSchema,
    message: str,
    *,
    event: "CycleEventSchema",
    status: Optional[CycleStatusEnum] = None,
  ) -> None:
    """Report one executed signal — through its cycle, or on its own."""
    self._notify_cycle_or_send(
      message,
      signal_uxid=signal.signal_uxid,
      strategy=signal.strategy,
      symbol=signal.symbol,
      event=event,
      status=status,
    )

  def _notify_cycle_or_send(
    self,
    message: str,
    *,
    signal_uxid: Optional[str],
    strategy: str,
    symbol: str,
    event: "CycleEventSchema",
    status: Optional[CycleStatusEnum] = None,
  ) -> None:
    """Fold *event* into its cycle, falling back to sending *message* as-is.

    Split from :meth:`_notify_trade` because not every cycle action comes from a
    signal — an admin FLAT closes a position the operator named, and takes its
    cycle key off the position row instead.
    """
    if self.ctx.cycle_notifier.record(
      signal_uxid=signal_uxid,
      strategy=strategy,
      symbol=symbol,
      event=event,
      status=status,
    ):
      return
    self.ctx.channel_notifier.send_message(message)

  def _cycle_event(
    self, signal: SignalSchema, result: dict, outcome: CycleOutcomeEnum
  ) -> "CycleEventSchema":
    """Build the timeline entry for one executed signal.

    Levels are taken from the signal (what was asked for) while price/volume come
    from the result (what actually happened), so a fill that slipped shows both
    sides of the story.
    """
    risk_info = self._resolve_risk_info(signal)
    return CycleEventSchema(
      action=signal.action.value,
      outcome=outcome,
      timestamp=signal.timestamp,
      price=result.get("price", signal.price),
      volume=result.get("volume", signal.quantity),
      sl=signal.sl,
      tp1=signal.tp1,
      tp2=signal.tp2,
      risk_percent=risk_info[0] if risk_info else None,
      risk_custom=bool(risk_info[1]) if risk_info else False,
      auto_volume=not self.config.uses_payload_quantity(
        getattr(signal, "use_equity_sizing", None)
      ),
      tp1_percent=self._cycle_tp1_percent(signal),
      is_scale_position=bool(getattr(signal, "is_scale_position", False)),
      # Only a close books money. An entry carries no PnL at all, so it stays
      # None and the timeline omits the line rather than claiming a 0.00 result
      # — the same rule BaseMessagePresenter._exit_pnl_line applies.
      profit=result.get("profit") if signal.action in _CYCLE_EXIT_ACTIONS else None,
      ref_id=_as_text(result.get("ticket")),
      ref_source_id=_as_text(result.get("source_ticket") or result.get("ticket")),
      gateway_return_code=result.get("retcode"),
      reason=result.get("comment") or None,
    )

  def _cycle_tp1_percent(self, signal: SignalSchema) -> Optional[float]:
    """Percent of the position a TP1 closed, mirroring the executor's own choice.

    Only TP1 sizes itself by percent, and only under VOLUME_DECISION_ENABLED —
    otherwise the close is sized from ``signal.quantity`` and no percent applies.
    """
    if signal.action != SignalActionEnum.TP1:
      return None
    if not self.settings.get("volume_decision_enabled", False):
      return None
    if self.settings.get("use_custom_position_tp1_percent", False):
      return self.settings.get("position_tp1_percent")
    if signal.tp1_percent is not None:
      return signal.tp1_percent
    return self.settings.get("position_tp1_percent")

  def _cycle_status(
    self, signal: SignalSchema, result: dict
  ) -> Optional[CycleStatusEnum]:
    """The position's status after this action — the cycle's headline.

    A branch-for-branch mirror of what :meth:`_persist_success` writes to the
    positions table, so the message and the row can never disagree. A failed
    action returns ``FAILED``, which
    :func:`~worker.schemas.cycle_schema.merge_status` keeps from overwriting a
    position that is still open.
    """
    if not result.get("success"):
      return CycleStatusEnum.FAILED
    if signal.action.value in ("LONG", "SHORT"):
      return CycleStatusEnum.OPENED
    # A breakeven SL that could not be placed leaves the remaining volume
    # unprotected, so it is emergency-closed — fully flat, not a still-open
    # partial. If that close itself failed the position is still live, and the
    # status its action implies is the honest one (the message shouts about it).
    failsafe = result.get("sl_failsafe_close")
    if failsafe is not None and failsafe.get("success"):
      return CycleStatusEnum.FORCED_CLOSED
    return CycleStatusEnum.from_position_status(
      _CLOSE_STATUS_MAP.get(signal.action.value)
    )
