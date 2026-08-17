"""
worker/gateways/cycle_presenter.py
──────────────────────────────────
Renders the single Telegram message that reports one signal cycle.

Cycle-shaped, not event-shaped: the body is rebuilt **in full** from the stored
timeline every time a new action arrives, because the message is edited in place
rather than followed by another one. So there is no "format one fill" entry
point here — :func:`format_cycle_message` is the whole surface.

The body is laid out as separate boxes so a long-running trade stays scannable:

    ┌ position ─ the headline status + what the trade is and its levels
    ├ actions  ─ the timeline, one entry per action the worker executed
    └ settings ─ the worker configuration the trade ran under, + account footer

Telegram has no box primitive, so each one is its own ``<pre>`` block: the boxes
are joined with ``</pre><pre>`` and the caller's :func:`_box` closes the outer
pair. Nesting a ``<pre>`` inside another is rejected by Telegram's HTML parser,
which is why the separator closes before it opens.

Market-agnostic (it is the same message for FOREX and CRYPTO); the only market
difference is the volume unit, taken from ``settings_dict["market_type"]``.
"""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any, Optional

from worker import icons
from worker.gateways.message_presenter import _DIVIDER, BaseMessagePresenter
from worker.schemas.cycle_schema import CycleOutcomeEnum, CycleStatusEnum
from worker.schemas.signal_schema import SignalActionEnum
from worker.settings import MarketTypeEnum

#: Headline icon per cycle status. ⏳ while the position is live, 🏁 once it is
#: finished, and the failure icons for the two states where nothing was opened.
_STATUS_ICONS = {
  CycleStatusEnum.OPENED: icons.CYCLE_OPENED,
  CycleStatusEnum.TP1: icons.CYCLE_OPENED,
  CycleStatusEnum.TP2: icons.CYCLE_CLOSED,
  CycleStatusEnum.SL: icons.CYCLE_CLOSED,
  CycleStatusEnum.R_SL: icons.CYCLE_CLOSED,
  CycleStatusEnum.FLATTED: icons.CYCLE_CLOSED,
  CycleStatusEnum.FORCED_CLOSED: icons.CYCLE_CLOSED,
  CycleStatusEnum.TERMINAL_CLOSED: icons.CYCLE_CLOSED,
  CycleStatusEnum.REJECTED: icons.REJECTED,
  CycleStatusEnum.FAILED: icons.FAILED,
}

#: Icon per signal action, shown in the timeline and next to the entry.
_ACTION_ICONS = {
  SignalActionEnum.LONG: icons.LONG,
  SignalActionEnum.SHORT: icons.SHORT,
  SignalActionEnum.TP1: icons.TP1,
  SignalActionEnum.TP2: icons.TP2,
  SignalActionEnum.SL: icons.STOP_LOSS,
  SignalActionEnum.R_SL: icons.SHIELD,
  SignalActionEnum.FLAT: icons.FLAT,
}

#: Icon per execution outcome. The action says what was asked for, the outcome
#: says how it went — a TP1 the broker refused must not look like a TP1 that hit.
_OUTCOME_ICONS = {
  CycleOutcomeEnum.FILLED: icons.SUCCESS,
  CycleOutcomeEnum.FAILED: icons.FAILED,
  CycleOutcomeEnum.REJECTED: icons.REJECTED,
  CycleOutcomeEnum.FORCE_CLOSED: icons.WARNING,
  CycleOutcomeEnum.UNPROTECTED_CLOSED: icons.SHIELD,
  CycleOutcomeEnum.ADMIN_FLAT: icons.ADMIN,
  CycleOutcomeEnum.RECONCILED: icons.SYNC,
  CycleOutcomeEnum.PLATFORM_CLOSED: icons.BROKER,
}

#: Label per outcome, so the timeline reads as a sentence rather than an enum.
_OUTCOME_LABELS = {
  CycleOutcomeEnum.FILLED: "Filled",
  CycleOutcomeEnum.FAILED: "Failed",
  CycleOutcomeEnum.REJECTED: "Rejected",
  CycleOutcomeEnum.FORCE_CLOSED: "Force Closed",
  CycleOutcomeEnum.UNPROTECTED_CLOSED: "Unprotected — Closed",
  CycleOutcomeEnum.ADMIN_FLAT: "Admin FLAT",
  CycleOutcomeEnum.RECONCILED: "Reconciled",
  CycleOutcomeEnum.PLATFORM_CLOSED: "Closed on Platform",
}

#: Boxes are separate ``<pre>`` blocks; the caller's ``_box`` opens the first
#: and closes the last. No newline after the opening tag — a ``<pre>`` that
#: starts with one renders a blank first line in the chat.
_BOX_BREAK = "\n</pre><pre>"

_ENTRY_ACTIONS = (SignalActionEnum.LONG.value, SignalActionEnum.SHORT.value)


def _num(value: Any) -> str:
  """Render a number the way a human writes it, or ``—`` when it is unset.

  Floats that came off a JSON round-trip carry the shape they were stored in, so
  ``2340.0`` renders as ``2340`` and ``0.10400`` as ``0.104`` — a level that
  prints differently from the one the strategy sent invites a support question.
  """
  if value is None:
    return "—"
  if isinstance(value, bool):
    return str(value)
  if isinstance(value, float):
    if value.is_integer():
      return str(int(value))
    trimmed = f"{value:.8f}".rstrip("0").rstrip(".")
    # A value below the 8th decimal trims away to "0"/"-0" — show the plain
    # repr instead of claiming a non-zero price is zero.
    return trimmed if float(trimmed or 0) != 0 else repr(value)
  return str(value)


def _volume(value: Any, unit: str, *, auto: bool = False) -> str:
  """Volume with its market's unit, gear-marked when the worker sized it."""
  if value is None:
    return "—"
  rendered = f"{_num(value)} {unit}".strip()
  return f"{rendered} {icons.GEAR}" if auto else rendered


def _volume_unit(settings_dict: dict | None) -> str:
  """``lot`` for forex, nothing for crypto (a coin amount has no unit word)."""
  market = (settings_dict or {}).get("market_type")
  value = getattr(market, "value", market)
  return "lot" if value == MarketTypeEnum.FOREX.value else ""


def _timestamp(value: Any) -> str:
  """Render an event's stored ISO timestamp; fall back to the raw text."""
  if value is None:
    return ""
  if isinstance(value, datetime):
    return value.strftime("%Y-%m-%d %H:%M:%S")
  try:
    return datetime.fromisoformat(str(value)).strftime("%Y-%m-%d %H:%M:%S")
  except ValueError:
    return str(value)


def _enum(value: Any, enum_cls, default=None):
  """Coerce a stored string back to its enum member, or *default*."""
  if isinstance(value, enum_cls):
    return value
  try:
    return enum_cls(getattr(value, "value", value))
  except ValueError:
    return default


def _joined(parts: list[str]) -> str:
  return " | ".join(p for p in parts if p)


def _entry_event(events: list[dict]) -> dict:
  """The event describing the position itself.

  Normally the LONG/SHORT that opened the cycle. A worker that restarted
  mid-trade — or that only ever saw the exit — has no entry event, so the first
  event stands in: an incomplete header beats no header at all.
  """
  for event in events:
    if event.get("action") in _ENTRY_ACTIONS:
      return event
  return events[0] if events else {}


# ── Box 1: position ─────────────────────────────────────────────────────────


def _position_box(cycle: dict, entry: dict, *, unit: str, symbol: str) -> str:
  """Headline status, what the trade is, and the levels it was opened with."""
  status = _enum(cycle.get("status"), CycleStatusEnum)
  status_label = status.value if status is not None else "PENDING"
  status_icon = _STATUS_ICONS.get(status, icons.UNKNOWN)

  action = entry.get("action") or ""
  action_icon = _ACTION_ICONS.get(_enum(action, SignalActionEnum), icons.SIGNAL)
  headline = f"{action_icon} <b>{action} {symbol}</b>" if action else f"<b>{symbol}</b>"

  lines = [f"[{status_icon} <b>{status_label}</b>]", headline, _DIVIDER]

  price_volume = [
    f"Price: {_num(entry.get('price'))}",
    f"Volume: {_volume(entry.get('volume'), unit, auto=bool(entry.get('auto_volume')))}",
  ]
  lines.append(_joined(price_volume))

  levels = [
    f"{name}: {_num(entry.get(key))}"
    for name, key in (("SL", "sl"), ("TP1", "tp1"), ("TP2", "tp2"))
    if entry.get(key) is not None
  ]
  if levels:
    lines.append(_joined(levels))

  extras = []
  if entry.get("risk_percent") is not None:
    gear = f" {icons.GEAR}" if entry.get("risk_custom") else ""
    extras.append(f"Risk: {_num(entry.get('risk_percent'))}%{gear}")
  if entry.get("is_scale_position"):
    extras.append(f"Scaled {icons.GEAR}")
  if entry.get("ref_source_id"):
    extras.append(f"Ticket: {html.escape(str(entry.get('ref_source_id')))}")
  if extras:
    lines.append(_joined(extras))

  lines.append(_DIVIDER)
  return "\n".join(lines)


# ── Box 2: actions ──────────────────────────────────────────────────────────


def _action_block(event: dict, *, unit: str) -> str:
  """One timeline entry: what was asked, how it went, and what it moved."""
  action = event.get("action") or ""
  outcome = _enum(event.get("outcome"), CycleOutcomeEnum, CycleOutcomeEnum.FILLED)
  icon = _OUTCOME_ICONS.get(outcome, icons.UNKNOWN)
  label = _OUTCOME_LABELS.get(outcome, outcome.value)

  lines = [f"{icon} <b>{action}</b> — {label}"]

  detail = []
  if event.get("price") is not None:
    detail.append(f"Price: {_num(event.get('price'))}")
  if event.get("volume") is not None:
    tp1_percent = event.get("tp1_percent")
    volume = _volume(event.get("volume"), unit, auto=bool(event.get("auto_volume")))
    if tp1_percent is not None:
      volume += f" (TP1 {_num(tp1_percent)}%)"
    detail.append(f"Volume: {volume}")
  if event.get("ref_id"):
    detail.append(f"Ticket: {html.escape(str(event.get('ref_id')))}")
  if detail:
    lines.append(_joined(detail))

  # A fill's comment is the broker's own acknowledgement ("Request executed"),
  # which says nothing a reader of a ✅ line does not already know. Every other
  # outcome's comment is the reason it went that way, so it earns a line.
  if event.get("reason") and outcome is not CycleOutcomeEnum.FILLED:
    reason = html.escape(str(event.get("reason")))
    code = event.get("gateway_return_code")
    # -1 is the worker's own "no broker involved" marker (a policy rejection),
    # so printing it would suggest a broker error code that does not exist.
    if code is not None and code not in (0, -1):
      reason += f" (Code {code})"
    lines.append(f"Reason: {reason}")

  stamp = _timestamp(event.get("timestamp"))
  if stamp:
    lines.append(stamp)
  return "\n".join(lines)


def _actions_box(events: list[dict], *, unit: str) -> str:
  blocks = [_action_block(event, unit=unit) for event in events]
  return "\n".join(
    [f"{icons.SIGNAL} <b>Actions</b>", _DIVIDER, "\n\n".join(blocks), _DIVIDER]
  )


# ── Box 3: settings ─────────────────────────────────────────────────────────


def _settings_box(
  cycle: dict, settings_dict: dict | None, footer: str, *, market: str
) -> str:
  """The worker configuration this trade ran under, plus the account footer.

  Reuses the same line builders as the startup banner and the per-fill override
  block (:class:`BaseMessagePresenter`), so the operator reads one wording for a
  setting wherever it appears.
  """
  lines = [f"{icons.GEAR} <b>Settings</b>"]
  if settings_dict is not None:
    # _override_section opens with its own divider and renders the volume /
    # risk / equity / TP1 / breakeven lines shared with the startup banner.
    section = BaseMessagePresenter._override_section(settings_dict)
    section += BaseMessagePresenter._max_open_orders_line(settings_dict)
    if market:
      section += BaseMessagePresenter._multi_strategy_line(settings_dict, market)
    lines.append(section.rstrip("\n"))
  lines.append(_DIVIDER)
  lines.append(f"Strategy: <b>{html.escape(str(cycle.get('strategy') or '—'))}</b>")
  lines.append(
    f"Signal: <code>{html.escape(str(cycle.get('signal_uxid') or '—'))}</code>"
  )
  if footer:
    lines.append(footer.strip("\n"))
  return "\n".join(lines)


def format_cycle_message(
  cycle: dict,
  *,
  settings_dict: Optional[dict] = None,
  footer: str = "",
) -> str:
  """Render *cycle* — id, symbol, strategy, status and its ``events`` list — in
  full, ready to be wrapped by :func:`~worker.services.notification_service._box`.

  Rebuilt from the events on every call by design: an edit replaces the whole
  message body, so there is nothing to append to.
  """
  events = [event for event in (cycle.get("events") or []) if isinstance(event, dict)]
  symbol = html.escape(str(cycle.get("symbol") or ""))
  unit = _volume_unit(settings_dict)
  market_value = getattr(
    (settings_dict or {}).get("market_type"),
    "value",
    (settings_dict or {}).get("market_type"),
  )
  entry = _entry_event(events)

  boxes = [_position_box(cycle, entry, unit=unit, symbol=symbol)]
  if events:
    boxes.append(_actions_box(events, unit=unit))
  boxes.append(
    _settings_box(cycle, settings_dict, footer, market=str(market_value or ""))
  )
  return _BOX_BREAK.join(boxes)
