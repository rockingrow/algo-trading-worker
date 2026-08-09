"""
worker/crypto/message_presenter.py
──────────────────────────────────
Telegram message strings for the crypto worker's trade lifecycle events.

Mirrors :class:`~worker.gateways.forex.message_presenter.ForexMessagePresenter` but with
exchange-appropriate labels (quantity instead of lots, exchange close reasons).
Pure functions of their inputs — trivial to test without an exchange, NATS, or
a DB.

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
  MANUAL,
  STOP,
  SUCCESS,
  SYNC,
  UNKNOWN,
  WARNING,
)
from worker.schemas.signal_schema import SignalSchema
from worker.services.notification_service import _box


class CryptoMessagePresenter(BaseMessagePresenter):
  """Renders boxed HTML Telegram messages for the crypto signal processor."""

  @staticmethod
  def startup(settings_dict: dict, footer: str) -> str:
    s = settings_dict
    exchange = s.get("crypto_exchange")
    exchange_display = getattr(exchange, "value", exchange)
    cfg = (
      f"EXCHANGE: <b>{exchange_display}</b>\n"
      f"TESTNET: <b>{s.get('crypto_testnet', False)}</b>\n"
      f"POSITION MODE: <b>{'HEDGE' if s.get('crypto_hedge_mode', True) else 'ONE-WAY'}</b>\n"
      f"{CryptoMessagePresenter._volume_decision_line(s)}"
      f"CAPITAL: <b>{s.get('capital')} {s.get('capital_currency', '')}</b>\n"
      f"{CryptoMessagePresenter._risk_percentage_line(s)}"
      f"USE_ACCOUNT_EQUITY: <b>{'ENABLED' if s.get('use_account_equity', False) else 'DISABLED'}</b>\n"
      f"{CryptoMessagePresenter._tp1_percent_line(s)}"
      f"{CryptoMessagePresenter._tp1_be_line(s)}"
      f"{CryptoMessagePresenter._max_open_orders_line(s)}"
    )
    return _box(
      f"{CONNECTED} <b>[Connected] Crypto Worker</b>\n\n{cfg}{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def shutdown(footer: str) -> str:
    return _box(f"{STOP} <b>[Disconnected] Crypto Worker</b>{footer}")

  @staticmethod
  def force_closed(symbol: str, strategy: str, fc: dict, footer: str) -> str:
    return _box(
      f"{WARNING} <b>Force Closed (New Entry)</b>\n\n"
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
    signal: SignalSchema,
    result: dict,
    pos_ticket: Any,
    footer: str,
    risk_info=None,
    settings_dict: dict | None = None,
  ) -> str:
    qty = result.get("volume")
    qty_suffix = CryptoMessagePresenter._tp1_qty_suffix(signal, settings_dict)
    return _box(
      f"{SUCCESS} <b>Order Filled</b>\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Action: <b>{signal.action.value}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Quantity: <b>{qty}{qty_suffix}</b>\n"
      f"Order: <b>{result.get('ticket')}</b>\n"
      f"Source: <b>{pos_ticket}</b>\n"
      f"{CryptoMessagePresenter._scale_lines(signal)}"
      f"{CryptoMessagePresenter._sl_line(signal, result)}"
      f"{CryptoMessagePresenter._risk_line(risk_info)}"
      f"{CryptoMessagePresenter._override_section(settings_dict)}"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def _scale_lines(signal: SignalSchema) -> str:
    """Render the scale-in (averaging) block, or '' for a normal entry.

    Shown only when the signal is flagged ``is_scale_position``. TP1/TP2 are the
    broker's already-scaled values and are displayed verbatim. The quantity/SL
    shown above are likewise the executed/broker-scaled values.
    """
    if not getattr(signal, "is_scale_position", False):
      return ""
    lines = f"{GEAR} <b>Scaled Position</b>\n"
    if signal.tp1 is not None:
      lines += f"TP1: <b>{signal.tp1}</b>\n"
    if signal.tp2 is not None:
      lines += f"TP2: <b>{signal.tp2}</b>\n"
    return lines

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
      return f"SL:{suffix} {SUCCESS}\n"
    return f"{WARNING} <b>SL NOT SET</b> ({sl_res.get('comment')})\n"

  @staticmethod
  def order_failed(signal: SignalSchema, result: dict, footer: str) -> str:
    return _box(
      f"{FAILED} <b>Order Failed</b>\n\n"
      f"Symbol: <b>{signal.symbol}</b>\n"
      f"Strategy: <b>{signal.strategy}</b>\n"
      f"Action: <b>{signal.action.value}</b>\n"
      f"Error: <b>{result.get('comment')}</b> (Code <b>{result.get('retcode')}</b>)\n"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def position_reconciled_closed(db_pos: dict, close_price: Any, footer: str) -> str:
    """The periodic reconciler found a DB-open position that no longer exists on
    the exchange (a missed fill event) and synced the DB to live state."""
    return _box(
      f"{SYNC} <b>Position Reconciled — Closed on Exchange</b>\n\n"
      f"A fill event was <b>missed</b>; the DB was synced from live exchange state.\n"
      f"Symbol: <b>{db_pos.get('symbol')}</b>\n"
      f"Strategy: <b>{db_pos.get('strategy')}</b>\n"
      f"Approx Close: <b>{close_price}</b>\n"
      f"Source: <b>{db_pos.get('ref_source_id')}</b>\n"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def position_imported_manual(pos: Any, ref_source_id: Any, footer: str) -> str:
    """The periodic reconciler found a position open on the exchange that the DB
    was not tracking (opened directly via the exchange UI / app / a third party)
    and imported it so the worker now manages it like any other position."""
    return _box(
      f"{MANUAL} <b>Manual Position Imported</b>\n\n"
      f"A position opened <b>directly on the exchange</b> was detected and is now tracked.\n"
      f"Symbol: <b>{pos.symbol}</b>\n"
      f"Side: <b>{pos.side}</b>\n"
      f"Quantity: <b>{pos.volume}</b>\n"
      f"Entry: <b>{pos.price_open}</b>\n"
      f"Source: <b>{ref_source_id}</b>\n"
      f"{_DIVIDER}\n{footer}"
    )

  @staticmethod
  def exchange_close(event: Any, footer: str) -> str:
    icon = {"SL": STOP, "TP": SUCCESS, "LIQUIDATION": WARNING, "MANUAL": MANUAL}.get(
      event.reason.value, UNKNOWN
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
      f"{ADMIN} <b>Admin FLAT Closed</b>\n\n"
      f"Symbol: <b>{db_pos['symbol']}</b>\n"
      f"Strategy: <b>{db_pos['strategy']}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Quantity: <b>{result.get('volume')}</b>\n"
      f"Order: <b>{result.get('ticket')}</b>\n"
      f"Source: <b>{db_pos['ref_source_id']}</b>\n"
      f"{_DIVIDER}\n{footer}"
    )
