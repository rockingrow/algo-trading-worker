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

from worker.gateways.message_presenter import _DIVIDER, BaseMessagePresenter, pnl_line
from worker.icons import (
  ADMIN,
  CONNECTED,
  FAILED,
  GEAR,
  STOP,
  SUCCESS,
  SYNC,
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
      f"{ForexMessagePresenter._volume_decision_line(s)}"
      f"CAPITAL: <b>{s.get('capital')} {s.get('capital_currency', '')}</b>\n"
      f"{ForexMessagePresenter._risk_percentage_line(s)}"
      f"USE_ACCOUNT_EQUITY: <b>{'ENABLED' if s.get('use_account_equity', False) else 'DISABLED'}</b>\n"
      f"{ForexMessagePresenter._tp1_percent_line(s)}"
      f"{ForexMessagePresenter._tp1_be_line(s)}"
      f"{ForexMessagePresenter._max_open_orders_line(s)}"
      f"{ForexMessagePresenter._multi_strategy_line(s, 'FOREX')}"
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
      f"{pnl_line(fc.get('profit'))}"
      f"{_DIVIDER}\n"
      f"{footer}"
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
    # The gear marks a volume the worker sized itself. Under
    # VOLUME_DECISION_ENABLED=false the lot comes straight from signal.quantity,
    # so the icon must be dropped — mirrors _risk_line, which the processor
    # likewise suppresses when volume decision is off.
    auto_sized = bool((settings_dict or {}).get("volume_decision_enabled", False))
    volume = format_volume(result.get("volume"), auto_calculated=auto_sized)
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
      f"{ForexMessagePresenter._exit_pnl_line(signal, result)}"
      f"{ForexMessagePresenter._stops_lines(signal, result)}"
      f"{ForexMessagePresenter._scale_lines(signal)}"
      f"{ForexMessagePresenter._risk_line(risk_info)}"
      f"{ForexMessagePresenter._override_section(settings_dict)}"
      f"{_DIVIDER}\n"
      f"{footer}"
    )

  @staticmethod
  def _stops_lines(signal: SignalSchema, result: dict) -> str:
    """Render the SL/TP actually registered with the broker.

    Reports what the position really carries, not what the signal asked for. The
    broker's minimum stop distance can move either level (``StopValidator``), and
    reading the signal's own numbers back out of a notification while the
    terminal holds different ones is how an adjusted stop goes unnoticed — so a
    level that was moved is shown with the signal's value beside it.
    """
    lines = ""
    for label, placed, requested in (
      ("SL", result.get("sl"), signal.sl),
      ("TP", result.get("tp"), signal.tp2),
    ):
      if placed is None:
        # No such stop on the position. Worth saying only when one was asked for
        # — otherwise the signal simply did not carry that level.
        if requested is not None:
          lines += f"{label}: <b>none</b> {WARNING} (signal asked {requested})\n"
        continue
      if requested is not None and placed != requested:
        lines += f"{label}: <b>{placed}</b> {WARNING} (signal asked {requested})\n"
      else:
        lines += f"{label}: <b>{placed}</b>\n"
    return lines

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
  def position_reconciled_closed(
    db_pos: dict, close_price: Any, footer: str, profit: Any = None
  ) -> str:
    """The periodic reconciler found a DB-open position that no longer exists on
    the platform (a close no terminal event reported — e.g. a TP1 partial that
    closed the whole position) and synced the DB to live state.

    *profit* is the position's **total** realized PnL, read from the platform's
    deal history rather than from a close the worker witnessed — so it spans the
    position's whole life (entry costs and any earlier partial included), which
    the label states outright.
    """
    return _box(
      f"{SYNC} <b>Position Reconciled — Closed on Broker</b>\n\n"
      f"The position is <b>no longer open</b> on the platform; the DB was synced "
      f"from live state.\n"
      f"Symbol: <b>{db_pos.get('symbol')}</b>\n"
      f"Strategy: <b>{db_pos.get('strategy')}</b>\n"
      f"Volume: <b>{db_pos.get('volume')} lot</b>\n"
      f"Approx Close: <b>{close_price}</b>\n"
      f"Source Ticket: <b>{db_pos.get('ref_source_id')}</b>\n"
      f"{pnl_line(profit, label='PnL (position total)')}"
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
      f"{pnl_line(result.get('profit'))}"
      f"{_DIVIDER}\n"
      f"{footer}"
    )
