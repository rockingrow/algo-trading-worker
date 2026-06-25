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

from worker.icons import ALARM, REJECTED, SHIELD
from worker.schemas.signal_schema import SignalSchema
from worker.services.notification_service import _box

_DIVIDER = "----------------------------------"


class BaseMessagePresenter:
  """Market-agnostic message fragments shared by every concrete presenter."""

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
