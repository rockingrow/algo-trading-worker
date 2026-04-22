"""
worker/services/callback_service.py
────────────────────────────────────
Sends trade lifecycle callbacks to the broker API.

  POST  /trades                       — called when a new trade is opened or rejected
  PATCH /trades/{account_id}/{ticket} — called when a trade is updated (partial close, SL/TP close)
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import MetaTrader5 as mt5

from worker.helpers import http_request
from worker.logger import get_logger
from worker.schemas.broker_schema import SignalSchema, SignalStatusEnum

log = get_logger("worker.callback_service")


class CallbackService:
  def __init__(self, broker_api_url: str, account_id: str, api_key: str) -> None:
    self._base_url = broker_api_url.rstrip("/")
    self._account_id = account_id
    self._headers = {"X-API-KEY": api_key}

  # ── helpers ────────────────────────────────────────────────────────────── #

  def _account_info(self) -> Dict[str, Any]:
    info = mt5.account_info()
    if info is None:
      return {"leverage": None, "balance_init": None, "balance": None}
    return {
      "leverage": info.leverage,
      "balance": info.balance,
    }

  # ── public API ─────────────────────────────────────────────────────────── #

  def notify_opened(
    self,
    signal: SignalSchema,
    ticket: int,
    comment: str,
    volume: float,
    price: float,
    balance_init: float,
  ) -> None:
    """POST /trades — trade opened successfully."""
    info = self._account_info()
    payload: Dict[str, Any] = {
      "account_id": self._account_id,
      "account_leverage": info["leverage"],
      "account_balance_init": balance_init,
      "account_balance": info["balance"],
      "ticket": ticket,
      "comment": comment,
      "magic": f"{signal.action.value}|{signal.signal_id}", # Only set at beginning LONG/SHORT
      "symbol": signal.symbol,
      "action": signal.action.value,
      "price": price,
      "quantity": volume,
      "sl": signal.sl,
      "tp1": signal.tp1,
      "tp2": signal.tp2,
      "is_running": True,
      "risk_percent": signal.risk_percent,
      "status": SignalStatusEnum.OPENED.value,
      "reject_reason": None,
    }
    self._post_trade(payload)

  def notify_rejected(
    self,
    signal: SignalSchema,
    reject_reason: str,
    balance_init: float,
  ) -> None:
    """POST /trades — trade rejected."""
    info = self._account_info()
    payload: Dict[str, Any] = {
      "account_id": self._account_id,
      "account_leverage": info["leverage"],
      "account_balance_init": balance_init,
      "account_balance": info["balance"],
      "ticket": None,
      "comment": f"{signal.action.value} {signal.symbol} — rejected: {reject_reason}",
      "magic": f"{signal.action.value}|{signal.signal_id}", # Only set at beginning LONG/SHORT or REJECTED
      "symbol": signal.symbol,
      "action": signal.action.value,
      "price": signal.price,
      "quantity": signal.quantity,
      "sl": signal.sl,
      "tp1": signal.tp1,
      "tp2": signal.tp2,
      "is_running": False,
      "risk_percent": signal.risk_percent,
      "status": SignalStatusEnum.REJECTED.value,
      "reject_reason": reject_reason,
    }
    self._post_trade(payload)

  def notify_closed(
    self,
    ticket: str,
    price: float,
    quantity: float = 0.0,
    sl: Optional[float] = None,
  ) -> None:
    """PATCH /trades/{self._account_id}/{ticket} — trade fully closed (SL or TP2)."""
    info = self._account_info()
    payload: Dict[str, Any] = {
      "account_balance": info["balance"],
      "price": price,
      "quantity": quantity,
      "sl": sl,
      "is_running": False,
      "status": SignalStatusEnum.CLOSED.value,
      "reject_reason": None,
    }
    self._patch_trade(ticket, payload)

  def notify_partially_closed(
    self,
    ticket: str,
    price: float,
    remaining_quantity: float,
  ) -> None:
    """PATCH /trades/{self._account_id}/{ticket} — trade partially closed (TP1 hit)."""
    info = self._account_info()
    payload: Dict[str, Any] = {
      "account_balance": info["balance"],
      "price": price,
      "quantity": remaining_quantity,
      "is_running": True,
      "status": SignalStatusEnum.PARTIALLY_CLOSED.value,
    }
    self._patch_trade(ticket, payload)

  def notify_updated(
    self,
    ticket: str,
    extra_fields: Dict[str, Any],
  ) -> None:
    """PATCH /trades/{self._account_id}/{ticket} — generic update (e.g. SL moved to breakeven)."""
    info = self._account_info()
    payload = {"account_balance": info["balance"], **extra_fields}
    self._patch_trade(ticket, payload)

  # ── transport ──────────────────────────────────────────────────────────── #

  def _post_trade(self, payload: Dict[str, Any]) -> None:
    url = f"{self._base_url}/trades"
    try:
      http_request.post(url, payload, headers=self._headers)
      log.info("Callback POST /trades status=%s", payload.get("status"))
    except Exception as exc:
      log.error("Callback POST /trades failed: %s", exc)

  def _patch_trade(self, ticket: str, payload: Dict[str, Any]) -> None:
    url = f"{self._base_url}/trades/{self._account_id}/{ticket}"
    try:
      http_request.patch(url, payload, headers=self._headers)
      log.info("Callback PATCH /trades/%s/%s status=%s", self._account_id, ticket, payload.get("status"))
    except Exception as exc:
      log.error("Callback PATCH /trades/%s/%s failed: %s", self._account_id, ticket, exc)
