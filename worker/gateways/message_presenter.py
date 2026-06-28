"""
worker/gateways/message_presenter.py
────────────────────────────────────
Shared base for the per-market Telegram message presenters.

Each market renders *different* strings (lots vs quantity, broker- vs
exchange-specific labels), so every ``<Market>MessagePresenter`` owns most of its
methods. This base holds only the parts that are byte-identical across markets —
the divider constant and the market-agnostic ``signal_rejected`` /
``position_unprotected_closed`` messages — so they live in one place (DRY).

The structural contract that ``BaseSignalProcessor`` actually dispatches through
is :class:`~worker.interfaces.trade_presenter_protocol.TradePresenterProtocol`;
this base is purely for code reuse, not for polymorphic dispatch.
"""

from __future__ import annotations

import html

from worker.icons import ALARM, GEAR, REJECTED, SHIELD
from worker.schemas.signal_schema import SignalActionEnum, SignalSchema
from worker.services.notification_service import _box

_DIVIDER = "----------------------------------"


class BaseMessagePresenter:
  """Market-agnostic message fragments shared by every concrete presenter."""

  @staticmethod
  def _risk_line(risk_info) -> str:
    """Render the risk percent line, or '' when risk info is unavailable.

    ``risk_info`` is a ``(risk_percent, is_custom)`` tuple produced by the
    processor for LONG/SHORT entries when VOLUME_DECISION_ENABLED is on.
    ``is_custom`` is True when USE_CUSTOM_RISK_PERCENTAGE overrides the signal.
    """
    if risk_info is None:
      return ""
    risk_percent, is_custom = risk_info
    gear = f" {GEAR}" if is_custom else ""
    return f"Risk: <b>{risk_percent}%</b>{gear}\n"

  @staticmethod
  def _override_line(label: str, enabled: bool, value: str) -> str:
    """Render a settings line that overrides-or-falls-back-to the signal.

    These settings are always shown so the operator can see, at a glance, which
    mode is active for the connected worker:
      - declared/active → ``LABEL: ENABLED (<value>)``
      - not declared    → ``LABEL: DISABLED`` (the signal's own value is used)
    """
    body = f"ENABLED ({value})" if enabled else "DISABLED"
    return f"{label}: <b>{body}</b>\n"

  @staticmethod
  def _volume_decision_line(settings_dict: dict) -> str:
    s = settings_dict
    enabled = bool(s.get("volume_decision_enabled", False))
    return f"VOLUME_DECISION_ENABLED: <b>{'ENABLED' if enabled else 'DISABLED'}</b>\n"

  @staticmethod
  def _risk_percentage_line(settings_dict: dict) -> str:
    """Render the RISK_PERCENTAGE override line.

    Enabled when ``use_custom_risk_percentage`` is set; the custom risk percent
    then overrides the signal's own ``risk_percent``.
    """
    s = settings_dict
    return BaseMessagePresenter._override_line(
      "RISK_PERCENTAGE",
      enabled=bool(s.get("use_custom_risk_percentage", False)),
      value=f"{s.get('risk_percentage')}%",
    )

  @staticmethod
  def _tp1_percent_line(settings_dict: dict) -> str:
    """Render the POSITION_TP1_PERCENT override line.

    Enabled when ``use_custom_position_tp1_percent`` is set; the custom percent
    then overrides the signal's own ``tp1_percent`` at TP1 time.
    """
    s = settings_dict
    return BaseMessagePresenter._override_line(
      "POSITION_TP1_PERCENT",
      enabled=bool(s.get("use_custom_position_tp1_percent", False)),
      value=f"{s.get('position_tp1_percent')}%",
    )

  @staticmethod
  def _override_section(settings_dict: dict | None) -> str:
    """Render the override-settings block shown on every order fill, or '' when
    no settings are available.

    Mirrors the config lines of the startup banner so the operator can see, on
    each trade event in the community channel, which override modes are active
    for the connected worker. Emitted as its own divider-wrapped section that
    sits between the position block and the account footer:

        ----------------------------------
        RISK_PERCENTAGE: ENABLED (3.0%)
        USE_ACCOUNT_EQUITY: ENABLED
        POSITION_TP1_PERCENT: ENABLED (50.0%)
        TP1_MOVE_SL_TO_BREAKEVEN: DISABLED

    Only the leading divider is rendered here; the caller's trailing divider
    closes the section before the footer.
    """
    if settings_dict is None:
      return ""
    s = settings_dict
    return (
      f"{_DIVIDER}\n"
      f"{BaseMessagePresenter._volume_decision_line(s)}"
      f"{BaseMessagePresenter._risk_percentage_line(s)}"
      f"USE_ACCOUNT_EQUITY: <b>{'ENABLED' if s.get('use_account_equity', False) else 'DISABLED'}</b>\n"
      f"{BaseMessagePresenter._tp1_percent_line(s)}"
      f"{BaseMessagePresenter._tp1_be_line(s)}"
    )

  @staticmethod
  def _tp1_qty_suffix(signal, settings_dict: dict | None) -> str:
    """Append the TP1 close-percent to the quantity/volume line for TP1 actions only.

    Only shown when the signal action is TP1. Returns '' for LONG/SHORT and all
    other actions, or when no TP1 percent applies.
    """
    if getattr(signal, "action", None) != SignalActionEnum.TP1:
      return ""
    if settings_dict is None:
      return ""
    s = settings_dict
    if s.get("use_custom_position_tp1_percent", False):
      pct = s.get("position_tp1_percent")
      gear = f" {GEAR}"
    else:
      pct = getattr(signal, "tp1_percent", None)
      gear = ""
    if pct is None:
      return ""
    return f" (TP1 {pct}%{gear})"

  @staticmethod
  def _tp1_be_line(settings_dict: dict) -> str:
    """Render the TP1_MOVE_SL_TO_BREAKEVEN override line.

    Enabled when ``tp1_move_sl_to_breakeven`` is explicitly set (True/False) in
    the environment; a ``None`` value means the signal's own flag is used.
    """
    val = settings_dict.get("tp1_move_sl_to_breakeven")
    return BaseMessagePresenter._override_line(
      "TP1_MOVE_SL_TO_BREAKEVEN",
      enabled=val is not None,
      value="true" if val else "false",
    )

  @staticmethod
  def signal_rejected(reason: str, footer: str) -> str:
    return _box(
      f"{REJECTED} <b>Signal Rejected</b>\n\n"
      f"A signal failed validation and was <b>NOT executed</b>.\n"
      f"Reason: <b>{html.escape(reason)}</b>\n"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def position_unprotected_closed(
    signal: SignalSchema, result: dict, footer: str
  ) -> str:
    """Breakeven SL after TP1 could not be placed; the now-unprotected position
    was force-closed (or the close itself failed — manual action needed)."""
    failsafe = result.get("sl_failsafe_close") or {}
    sl_res = result.get("sl_update") or {}
    if failsafe.get("success"):
      head = f"{SHIELD} <b>Unprotected Position — Emergency Closed</b>"
      outcome = (
        f"Closed: <b>{failsafe.get('volume')}</b> @ <b>{failsafe.get('price')}</b>\n"
      )
    else:
      head = f"{ALARM} <b>UNPROTECTED POSITION — STILL OPEN</b>"
      outcome = (
        f"Emergency close <b>FAILED</b>: {failsafe.get('comment')}\n"
        f"<b>Manual intervention required.</b>\n"
      )
    return _box(
      f"{head}\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Reason: breakeven SL failed (<b>{sl_res.get('comment')}</b>)\n"
      f"{outcome}"
      f"{_DIVIDER}\n{footer}"
    )
