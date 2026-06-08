"""
worker/crypto/message_presenter.py
──────────────────────────────────
Telegram message strings for the crypto worker's trade lifecycle events.

Mirrors :class:`~worker.gateways.mt5.message_presenter.TradeMessagePresenter` but with
exchange-appropriate labels (quantity instead of lots, exchange close reasons).
Pure functions of their inputs — trivial to test without an exchange, NATS, or
a DB.
"""

from __future__ import annotations

from typing import Any

from worker.schemas.signal_schema import SignalSchema
from worker.services.notification_service import _box

_DIVIDER = "----------------------------------"


class CryptoMessagePresenter:
  """Renders boxed HTML Telegram messages for the crypto signal processor."""

  @staticmethod
  def startup(settings_dict: dict, footer: str) -> str:
    s = settings_dict
    cfg = (
      f"EXCHANGE: <b>{s.get('crypto_exchange')}</b>\n"
      f"TESTNET: <b>{s.get('binance_testnet', False)}</b>\n"
      f"VOLUME_DECISION_ENABLED: <b>{s.get('volume_decision_enabled', False)}</b>\n"
      f"CAPITAL: <b>{s.get('capital')} {s.get('capital_currency', '')}</b>\n"
      f"RISK_PERCENTAGE: <b>{s.get('risk_percentage')}%</b>\n"
      f"POSITION_TP1_PERCENT: <b>{s.get('position_tp1_percent', 0)}%</b>\n"
    )
    return _box(f"🟢 <b>[Connected] Crypto Worker</b>\n\n{cfg}{_DIVIDER}\n{footer}")

  @staticmethod
  def shutdown(footer: str) -> str:
    return _box(f"🛑 <b>[Disconnected] Crypto Worker</b>{footer}")

  @staticmethod
  def force_closed(symbol: str, strategy: str, fc: dict, footer: str) -> str:
    return _box(
      f"⚠️ <b>Force Closed (New Entry)</b>\n\n"
      f"Symbol: <b>{symbol}</b>\n"
      f"Strategy: <b>{strategy}</b>\n"
      f"Price: <b>{fc.get('price')}</b>\n"
      f"Quantity: <b>{fc.get('volume')}</b>\n"
      f"Order: <b>{fc.get('ref_id')}</b>\n"
      f"Source: <b>{fc.get('ref_source_id')}</b>\n"
      f"{_DIVIDER}\n{footer}"
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
      f"Quantity: <b>{result.get('volume')}</b>\n"
      f"Order: <b>{result.get('ticket')}</b>\n"
      f"Source: <b>{pos_ticket}</b>\n"
      f"{CryptoMessagePresenter._sl_line(signal, result)}"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def _sl_line(signal: SignalSchema, result: dict) -> str:
    """Render the stop-loss status line, or '' when no SL was involved.

    Present for entries (initial SL) and TP1 (breakeven SL) — both carry an
    ``sl_update``. Absent for plain full-close exits, which carry none.
    """
    sl_res = result.get("sl_update")
    if sl_res is None:
      return ""
    price = sl_res.get("new_sl") or getattr(signal, "sl", None)
    if sl_res.get("success"):
      suffix = f" <b>{price}</b>" if price else ""
      return f"SL:{suffix} ✅\n"
    return f"⚠️ <b>SL NOT SET</b> ({sl_res.get('comment')})\n"

  @staticmethod
  def order_failed(signal: SignalSchema, result: dict, footer: str) -> str:
    return _box(
      f"❌ <b>Order Failed</b>\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Action: <b>{signal.action.value}</b>\n"
      f"Error: <b>{result.get('comment')}</b> (Code <b>{result.get('retcode')}</b>)\n"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def exchange_close(event: Any, footer: str) -> str:
    icon = {"SL": "🛑", "TP": "✅", "LIQUIDATION": "⚠️", "MANUAL": "🖐"}.get(
      event.reason.value, "❓"
    )
    return _box(
      f"{icon} <b>Exchange Close [{event.reason.value}]</b>\n\n"
      f"Symbol: <b>{event.symbol}</b>\n"
      f"Close Price: <b>{event.close_price}</b>\n"
      f"Quantity: <b>{event.close_volume}</b>\n"
      f"Realized PnL: <b>{event.realized_pnl}</b>\n"
      f"Order: <b>{event.order_id}</b>\n"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def admin_flat_closed(db_pos: dict, result: dict, footer: str) -> str:
    return _box(
      f"⚡ <b>Admin FLAT Closed</b>\n\n"
      f"Symbol: <b>{db_pos['symbol']}</b>\n"
      f"Strategy: <b>{db_pos['strategy']}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Quantity: <b>{result.get('volume')}</b>\n"
      f"Order: <b>{result.get('ticket')}</b>\n"
      f"Source: <b>{db_pos['ref_source_id']}</b>\n"
      f"{_DIVIDER}\n{footer}"
    )
