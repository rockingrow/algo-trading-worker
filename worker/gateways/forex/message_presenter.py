"""
worker/gateways/forex/message_presenter.py
──────────────────────────────────────────
Builds the Telegram message strings for FOREX trade lifecycle events.

Platform-agnostic (lots terminology, no MetaTrader5 dependency); it is the
forex-market counterpart of ``CryptoMessagePresenter`` and is bound by
``ForexSignalProcessor``. Message *formatting* is decoupled from signal
*processing* and persistence (Single Responsibility). These are pure functions of
their inputs — trivial to test without a platform, NATS, or a DB.

The market-agnostic messages (``signal_rejected``, ``position_unprotected_closed``)
are inherited from
:class:`~worker.gateways.message_presenter.BaseMessagePresenter`.
"""

from __future__ import annotations

from typing import Any

from worker.gateways.message_presenter import _DIVIDER, BaseMessagePresenter
from worker.icons import (
  ADMIN,
  CONNECTED,
  FAILED,
  GEAR,
  STOP,
  SUCCESS,
  WARNING,
)
from worker.schemas.signal_schema import SignalSchema
from worker.services.notification_service import _box


def format_volume(volume: Any, auto_calculated: bool = False) -> str:
  """Format volume with a gear icon when it was auto-calculated."""
  icon = GEAR if auto_calculated else ""
  return f"{volume} lot {icon}".strip() if auto_calculated else f"{volume} lot"


class ForexMessagePresenter(BaseMessagePresenter):
  """Renders boxed HTML Telegram messages for the forex signal processor."""

  @staticmethod
  def startup(settings_dict: dict, footer: str) -> str:
    s = settings_dict
    volume_config = (
      f"VOLUME_DECISION_ENABLED: <b>{s.get('volume_decision_enabled', False)}</b>\n"
      f"CAPITAL: <b>{s.get('capital')} {s.get('capital_currency', '')}</b>\n"
      f"{ForexMessagePresenter._risk_percentage_line(s)}"
      f"USE_ACCOUNT_EQUITY: <b>{'ENABLED' if s.get('use_account_equity', False) else 'DISABLED'}</b>\n"
      f"{ForexMessagePresenter._tp1_percent_line(s)}"
      f"{ForexMessagePresenter._tp1_be_line(s)}"
    )
    return _box(
      f"{CONNECTED} <b>[Connected] FOREX Worker</b>\n\n{volume_config}{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def shutdown(footer: str) -> str:
    return _box(f"{STOP} <b>[Disconnected] FOREX Worker</b>{footer}")

  @staticmethod
  def force_closed(symbol: str, strategy: str, fc: dict, footer: str) -> str:
    return _box(
      f"{WARNING} <b>Force Closed (New Entry)</b>\n\n"
      f"Symbol: <b>{symbol}</b>\n"
      f"Strategy: <b>{strategy}</b>\n"
      f"Price: <b>{fc.get('price')}</b>\n"
      f"Volume: <b>{format_volume(fc.get('volume'), auto_calculated=False)}</b>\n"
      f"Ticket: <b>{fc.get('ref_id')}</b>\n"
      f"Source Ticket: <b>{fc.get('ref_source_id')}</b>\n"
      f"{_DIVIDER}\n"
      f"{footer}"
    )

  @staticmethod
  def order_filled(
    signal: SignalSchema, result: dict, pos_ticket: Any, footer: str,
    risk_info=None, settings_dict: dict | None = None,
  ) -> str:
    volume = format_volume(result.get("volume"), auto_calculated=True)
    qty_suffix = ForexMessagePresenter._tp1_qty_suffix(signal, settings_dict)
    return _box(
      f"{SUCCESS} <b>Order Filled</b>\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Action: <b>{signal.action.value}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Volume: <b>{volume}{qty_suffix}</b>\n"
      f"Ticket: <b>{result.get('ticket')}</b>\n"
      f"Source Ticket: <b>{pos_ticket}</b>\n"
      f"{ForexMessagePresenter._scale_lines(signal)}"
      f"{ForexMessagePresenter._risk_line(risk_info)}"
      f"{ForexMessagePresenter._override_section(settings_dict)}"
      f"{_DIVIDER}\n"
      f"{footer}"
    )

  @staticmethod
  def _scale_lines(signal: SignalSchema) -> str:
    """Render the scale-in (averaging) block, or '' for a normal entry.

    Shown only when the signal is flagged ``is_scale_position``. TP1/TP2/SL are
    the broker's already-scaled values and are displayed verbatim. The volume
    shown above is the executed lot (auto-sized × scale factor in VOLUME_DECISION
    mode, or the broker's scaled quantity in payload mode).
    """
    if not getattr(signal, "is_scale_position", False):
      return ""
    lines = f"{GEAR} <b>Scaled Position</b>\n"
    if signal.tp1 is not None:
      lines += f"TP1: <b>{signal.tp1}</b>\n"
    if signal.tp2 is not None:
      lines += f"TP2: <b>{signal.tp2}</b>\n"
    if signal.sl is not None:
      lines += f"SL: <b>{signal.sl}</b>\n"
    return lines

  @staticmethod
  def order_failed(signal: SignalSchema, result: dict, footer: str) -> str:
    return _box(
      f"{FAILED} <b>Order Failed</b>\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Action: <b>{signal.action.value}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Error: <b>{result.get('comment')}</b> (Code <b>{result.get('retcode')}</b>)\n"
      f"{_DIVIDER}\n"
      f"{footer}"
    )

  @staticmethod
  def admin_flat_closed(db_pos: dict, result: dict, footer: str) -> str:
    return _box(
      f"{ADMIN} <b>Admin FLAT Closed</b>\n\n"
      f"Symbol: <b>{db_pos['symbol']}</b>\n"
      f"Strategy: <b>{db_pos['strategy']}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Volume: <b>{result.get('volume')} lot</b>\n"
      f"Ticket: <b>{result.get('ticket')}</b>\n"
      f"Source Ticket: <b>{db_pos['ref_source_id']}</b>\n"
      f"{_DIVIDER}\n"
      f"{footer}"
    )
