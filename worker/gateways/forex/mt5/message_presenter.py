"""
worker/gateways/forex/mt5/message_presenter.py
──────────────────────────────────────────────
Builds the Telegram message strings for trade lifecycle events.

Pulled out of ``Mt5SignalProcessor`` so message *formatting* is decoupled from
signal *processing* and persistence (Single Responsibility). These are pure
functions of their inputs — trivial to test without MT5, NATS, or a DB.
"""

from __future__ import annotations

import html
from typing import Any

from worker.schemas.signal_schema import SignalSchema
from worker.services.notification_service import _box

_DIVIDER = "----------------------------------"


def format_volume(volume: Any, auto_calculated: bool = False) -> str:
  """Format volume with a gear icon when it was auto-calculated."""
  icon = "⚙️" if auto_calculated else ""
  return f"{volume} lot {icon}".strip() if auto_calculated else f"{volume} lot"


class TradeMessagePresenter:
  """Renders boxed HTML Telegram messages for the signal processor."""

  @staticmethod
  def startup(settings_dict: dict, footer: str) -> str:
    s = settings_dict
    volume_config = (
      f"VOLUME_DECISION_ENABLED: <b>{s.get('volume_decision_enabled', False)}</b>\n"
      f"CAPITAL: <b>{s.get('capital')} {s.get('capital_currency', '')}</b>\n"
      f"RISK_PERCENTAGE: <b>{s.get('risk_percentage')}%</b>\n"
      f"USE_ACCOUNT_EQUITY: <b>{s.get('use_account_equity', False)}</b>\n"
      f"POSITION_TP1_PERCENT: <b>{s.get('position_tp1_percent', 0)}%</b>\n"
    )
    return _box(
      f"🟢 <b>[Connected] MT5 Worker</b>\n\n{volume_config}{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def shutdown(footer: str) -> str:
    return _box(f"🛑 <b>[Disconnected] MT5 Worker</b>{footer}")

  @staticmethod
  def force_closed(symbol: str, strategy: str, fc: dict, footer: str) -> str:
    return _box(
      f"⚠️ <b>Force Closed (New Entry)</b>\n\n"
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
    signal: SignalSchema, result: dict, pos_ticket: Any, footer: str
  ) -> str:
    return _box(
      f"✅ <b>Order Filled</b>\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Action: <b>{signal.action.value}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Volume: <b>{format_volume(result.get('volume'), auto_calculated=True)}</b>\n"
      f"Ticket: <b>{result.get('ticket')}</b>\n"
      f"Source Ticket: <b>{pos_ticket}</b>\n"
      f"{_DIVIDER}\n"
      f"{footer}"
    )

  @staticmethod
  def order_failed(signal: SignalSchema, result: dict, footer: str) -> str:
    return _box(
      f"❌ <b>Order Failed</b>\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Action: <b>{signal.action.value}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Error: <b>{result.get('comment')}</b> (Code <b>{result.get('retcode')}</b>)\n"
      f"{_DIVIDER}\n"
      f"{footer}"
    )

  @staticmethod
  def signal_rejected(reason: str, footer: str) -> str:
    return _box(
      f"🚫 <b>Signal Rejected</b>\n\n"
      f"A signal failed validation and was <b>NOT executed</b>.\n"
      f"Reason: <b>{html.escape(reason)}</b>\n"
      f"{_DIVIDER}\n"
      f"{footer}"
    )

  @staticmethod
  def admin_flat_closed(db_pos: dict, result: dict, footer: str) -> str:
    return _box(
      f"⚡ <b>Admin FLAT Closed</b>\n\n"
      f"Symbol: <b>{db_pos['symbol']}</b>\n"
      f"Strategy: <b>{db_pos['strategy']}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Volume: <b>{result.get('volume')} lot</b>\n"
      f"Ticket: <b>{result.get('ticket')}</b>\n"
      f"Source Ticket: <b>{db_pos['ref_source_id']}</b>\n"
      f"{_DIVIDER}\n"
      f"{footer}"
    )
