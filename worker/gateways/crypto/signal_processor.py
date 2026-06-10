"""
worker/gateways/crypto/signal_processor.py
──────────────────────────────────────────
CRYPTO/CEX-specific signal processor.

Implements the broker-specific hooks of
:class:`~worker.gateways.processor.BaseSignalProcessor`: the exchange
gateway (built by :class:`~worker.gateways.crypto.factory.ExchangeFactory`), the
crypto executor, and the exchange user-data event stream. Everything
market-agnostic — the NATS loop, signal persistence, notifications, position CDC
— is inherited from the base.

This module imports **no** MetaTrader5 / ``worker.gateways.mt5.*`` code, so the
CRYPTO path never initializes any Forex dependency.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from worker.gateways.crypto.binance.user_data_stream import (
  ExchangeCloseEvent,
  ExchangeCloseReason,
)
from worker.gateways.crypto.executor import CryptoExecutor
from worker.gateways.crypto.factory import ExchangeFactory
from worker.gateways.crypto.message_presenter import CryptoMessagePresenter
from worker.gateways.processor import BaseSignalProcessor
from worker.logger import get_logger
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

    if len(matched) > 1:
      # Multiple strategies held the same symbol simultaneously in netting mode.
      # One exchange fill closes the shared net position; all matched rows are
      # marked closed with the same total fill volume.
      log.warning(
        "[Crypto Event] %d DB rows matched %s — netting mode: one exchange fill "
        "closes all. Check CRYPTO_ALLOW_MULTI_STRATEGY_PER_SYMBOL config.",
        len(matched), event.symbol,
      )

    close_comment = (
      f"Exchange close [{event.reason.value}] (shared fill — netting mode)"
      if len(matched) > 1
      else f"Exchange close [{event.reason.value}]"
    )
    for row in matched:
      self.ctx.db_service.log_position(
        strategy=row["strategy"],
        ref_id=event.order_id,
        ref_source_id=row["ref_source_id"],
        symbol=row["symbol"],
        action=event.reason.value,
        volume=event.close_volume,  # total exchange fill volume
        price=event.close_price,
        sl=None,
        tp1=None,
        gateway_return_code=0,
        comment=close_comment,
        author=LogAuthorEnum.EXCHANGE.value,
        market_type=self._market_type,
      )
      self.ctx.db_service.update_position_status(
        ref_source_id=row["ref_source_id"],
        status=status,
        ref_id=event.order_id,
        closed_price=event.close_price,
        gateway_return_code=0,
        comment=close_comment,
      )
    self.ctx.channel_notifier.send_message(
      CryptoMessagePresenter.exchange_close(event, self.gateway.get_account_footer())
    )

  # ── ADMIN FLAT match keys (crypto reconciles by resolved symbol) ──────── #

  def _flat_match_key(self, pos: Any) -> Any:
    # ExchangePosition.symbol is already the resolved exchange symbol.
    return pos.symbol

  def _flat_db_match_keys(self, db_pos: dict) -> set:
    # The DB stores the original signal symbol; resolve it to the exchange form.
    return {self.executor.get_symbol(db_pos["symbol"])}
