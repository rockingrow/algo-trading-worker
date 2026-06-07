"""
worker/gateways/crypto/signal_processor.py
──────────────────────────────────────────
CRYPTO/CEX-specific signal processor.

Implements the broker-specific hooks of
:class:`~worker.core.base_signal_processor.BaseSignalProcessor`: the exchange
gateway (built by :class:`~worker.gateways.crypto.factory.ExchangeFactory`), the
crypto executor, and the exchange user-data event stream. Everything
market-agnostic — the NATS loop, signal persistence, notifications, position CDC
— is inherited from the base.

This module imports **no** MetaTrader5 / ``worker.gateways.mt5.*`` code, so the
CRYPTO path never initializes any Forex dependency.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from pydantic import ValidationError

from worker.gateways.crypto.binance.user_data_stream import (
  ExchangeCloseEvent,
  ExchangeCloseReason,
)
from worker.gateways.crypto.executor import CryptoExecutor
from worker.gateways.crypto.factory import ExchangeFactory
from worker.gateways.crypto.message_presenter import CryptoMessagePresenter
from worker.gateways.processor import BaseSignalProcessor
from worker.logger import get_logger
from worker.schemas.admin_schema import AdminActionEnum, AdminSignalSchema
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.position_schema import PositionStatusEnum

log = get_logger("worker.gateways.crypto.signal_processor")

# Exchange-triggered close reason → DB status.
_EXCHANGE_CLOSE_STATUS = {
  ExchangeCloseReason.SL: PositionStatusEnum.SL,
  ExchangeCloseReason.TP: PositionStatusEnum.TP2,
  ExchangeCloseReason.LIQUIDATION: PositionStatusEnum.FORCED_CLOSED,
  ExchangeCloseReason.MANUAL: PositionStatusEnum.TERMINAL_CLOSED,
}


class CryptoSignalProcessor(BaseSignalProcessor):
  """Exchange gateway + crypto executor + user-data stream over the shared skeleton."""

  name = "CRYPTO"
  presenter = CryptoMessagePresenter

  # ── Broker hooks ──────────────────────────────────────────────────────── #

  def _build_executor(self) -> CryptoExecutor:
    self.gateway = ExchangeFactory.create(self.settings)
    return CryptoExecutor(
      gateway=self.gateway,
      config=self.config,
      db=self.ctx.db_service,
      quote_asset=self.settings.get("crypto_quote_asset", "USDT"),
    )

  def _connect_broker(self) -> bool:
    return self.gateway.connect()

  def _disconnect_broker(self) -> None:
    self.gateway.close()

  def _account_footer(self) -> str:
    return self.gateway.get_account_footer()

  @property
  def _account_id(self) -> str:
    return str(self.settings.get("crypto_exchange", "CRYPTO"))

  def _magic_for(self, strategy: str) -> Optional[int]:
    return None  # crypto exchanges have no magic-number equivalent

  def _position_cdc_kwargs(self) -> Dict[str, Any]:
    return {
      "account_info_fn": self._account_snapshot,
      "account_name": self.settings.get("mt5_name"),
      "strategy_magic_map": {},
    }

  def _account_snapshot(self) -> Optional[dict]:
    info = self.gateway.get_account()
    if not info:
      return None
    return {"balance": info.get("balance"), "leverage": None}

  def _start_broker_jobs(self, stop_event) -> None:
    # Exchange-side event ingestion (Binance → websocket user data stream).
    event_stream = self.gateway.create_event_stream(self._on_exchange_close)
    if event_stream is not None:
      event_stream.start(stop_event=stop_event)
    else:
      log.info("[CRYPTO Process] Exchange has no push event stream; skipping.")

  # ── Exchange-triggered close handler (from the user data stream) ──────── #

  def _on_exchange_close(self, event: ExchangeCloseEvent) -> None:
    status = _EXCHANGE_CLOSE_STATUS.get(event.reason, PositionStatusEnum.TERMINAL_CLOSED)
    log.info(
      "[Crypto Event] Exchange close | symbol=%s reason=%s price=%s",
      event.symbol, event.reason.value, event.close_price,
    )

    # Match by resolved exchange symbol: the DB stores the original signal symbol
    # (e.g. BTCUSD) while the event carries the exchange symbol (e.g. BTCUSDT).
    open_rows = self.ctx.db_service.get_open_positions_for_flat()
    matched = [
      row for row in open_rows
      if self.executor.get_symbol(row["symbol"]) == event.symbol
    ]
    if not matched:
      log.warning("[Crypto Event] No open DB position for %s — ignoring.", event.symbol)
      return

    for row in matched:
      self.ctx.db_service.log_position(
        strategy=row["strategy"],
        ticket=event.order_id,
        source_ticket=row["source_ticket"],
        symbol=row["symbol"],
        action=event.reason.value,
        volume=event.close_volume,
        price=event.close_price,
        sl=None,
        tp1=None,
        mt5_retcode=0,
        comment=f"Exchange close [{event.reason.value}]",
        author=LogAuthorEnum.EXCHANGE.value,
        market_type=self._market_type,
      )
      self.ctx.db_service.update_position_status(
        source_ticket=row["source_ticket"],
        status=status,
        new_ticket=event.order_id,
        closed_price=event.close_price,
        mt5_retcode=0,
        comment=f"Exchange close [{event.reason.value}]",
      )
    self.ctx.channel_notifier.send_message(
      CryptoMessagePresenter.exchange_close(event, self.gateway.get_account_footer())
    )

  # ── ADMIN FLAT (crypto reconciles by resolved symbol) ─────────────────── #

  def _handle_admin_message(self, raw: str) -> None:
    try:
      admin = AdminSignalSchema(**json.loads(raw))
    except (json.JSONDecodeError, ValidationError) as err:
      log.error("[ADMIN] Parse error: %s", err)
      return

    if admin.action != AdminActionEnum.FLAT:
      log.warning("[ADMIN] Unknown action: %s", admin.action)
      return

    if admin.account_id and admin.account_id != self._account_id:
      log.info(
        "[ADMIN FLAT] Skipping: account_id=%s != worker account=%s",
        admin.account_id, self._account_id,
      )
      return

    if not self._ensure_connected():
      return

    if admin.symbol:
      positions = self.executor.get_open_positions(admin.symbol, strategy=admin.strategy)
    else:
      positions = self.executor.get_all_open_positions(strategy=admin.strategy)

    closed: dict[str, dict] = {}
    for pos in positions:
      result = self.executor.close_single_position(pos, reason="FLAT")
      if result.get("success"):
        closed[pos.symbol] = result
        log.info("[ADMIN FLAT] Closed %s qty=%s", pos.symbol, result.get("volume"))
      else:
        log.error("[ADMIN FLAT] Failed to close %s: %s", pos.symbol, result.get("comment"))

    # Reconcile DB.
    db_positions = self.ctx.db_service.get_open_positions_for_flat(
      strategy=admin.strategy, symbol=admin.symbol
    )
    for db_pos in db_positions:
      resolved = self.executor.get_symbol(db_pos["symbol"])
      result = closed.get(resolved)
      if result is not None:
        self.ctx.db_service.update_position_status(
          source_ticket=db_pos["source_ticket"],
          status=PositionStatusEnum.FLATTED,
          new_ticket=result.get("ticket"),
          closed_price=result.get("price"),
          mt5_retcode=0,
          comment=result.get("comment", ""),
          message=raw,
        )
        self.ctx.channel_notifier.send_message(
          CryptoMessagePresenter.admin_flat_closed(
            db_pos, result, self.gateway.get_account_footer()
          )
        )
      else:
        log.warning(
          "[ADMIN FLAT] %s in DB but not closed on exchange — marking FLATTED",
          db_pos["symbol"],
        )
        self.ctx.db_service.update_position_status(
          source_ticket=db_pos["source_ticket"],
          status=PositionStatusEnum.FLATTED,
          comment="Admin FLAT (position not found on exchange)",
          message=raw,
        )
