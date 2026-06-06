"""
worker/crypto/signal_processor.py
─────────────────────────────────
CRYPTO/CEX-specific signal processor — the counterpart of ``Mt5SignalProcessor``.

Owns the dependencies specific to a centralized exchange: the exchange gateway
(built by :class:`~worker.gateways.crypto.factory.ExchangeFactory`), the crypto executor,
the strategy/handler stack, the NATS subscriber/publisher, and the crypto
background jobs (position CDC + the exchange user-data event stream).

Receives a :class:`WorkerContext` for market-agnostic infrastructure (DB,
outbox/direct notifiers, NotificationJob).

This module imports **no** MetaTrader5 / ``worker.gateways.mt5.*`` code, so the CRYPTO
path never initializes any Forex dependency.
"""

from __future__ import annotations

import json
from typing import Optional

from pydantic import ValidationError

from worker.context import WorkerContext
from worker.core.config import ExecutionConfig
from worker.core.market_strategy import MarketStrategyFactory
from worker.core.signal_handler import SignalHandler
from worker.gateways.crypto.binance.user_data_stream import (
  ExchangeCloseEvent,
  ExchangeCloseReason,
)
from worker.gateways.crypto.executor import CryptoExecutor
from worker.gateways.crypto.factory import ExchangeFactory
from worker.gateways.crypto.message_presenter import CryptoMessagePresenter
from worker.jobs.cdc_job import PositionCDC
from worker.logger import get_logger
from worker.schemas.admin_schema import AdminActionEnum, AdminSignalSchema
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalSchema
from worker.services.nats_service import NATSPublisher, NATSSubscriber
from worker.settings import NATS_REQUIRED_LISTENING_SUBJECTS, MarketTypeEnum

log = get_logger("worker.gateways.crypto.signal_processor")

_CLOSE_STATUS_MAP = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
  "FLAT": PositionStatusEnum.FLATTED,
}

# Exchange-triggered close reason → DB status.
_EXCHANGE_CLOSE_STATUS = {
  ExchangeCloseReason.SL: PositionStatusEnum.SL,
  ExchangeCloseReason.TP: PositionStatusEnum.TP2,
  ExchangeCloseReason.LIQUIDATION: PositionStatusEnum.FORCED_CLOSED,
  ExchangeCloseReason.MANUAL: PositionStatusEnum.TERMINAL_CLOSED,
}


def _parse_nats_subjects(raw: str) -> list[str | NatsSubjectEnum]:
  parsed: set[str | NatsSubjectEnum] = set()
  for s in raw.split(","):
    s = s.strip()
    if not s:
      continue
    try:
      parsed.add(NatsSubjectEnum(s))
    except ValueError:
      parsed.add(s)
  return list(NATS_REQUIRED_LISTENING_SUBJECTS | parsed)


class CryptoSignalProcessor:
  """Composes exchange gateway + NATS + executor/handler, drives the signal loop."""

  def __init__(self, ctx: WorkerContext, settings_dict: dict) -> None:
    self.ctx = ctx
    self.settings = settings_dict

    self.gateway = ExchangeFactory.create(settings_dict)
    self.config = ExecutionConfig.from_dict(settings_dict)
    self.executor = CryptoExecutor(
      gateway=self.gateway,
      config=self.config,
      db=ctx.db_service,
      quote_asset=settings_dict.get("crypto_quote_asset", "USDT"),
    )
    self.strategy = MarketStrategyFactory.create(
      executor=self.executor, config=self.config
    )
    self.handler = SignalHandler(self.strategy, ctx.db_service)

    self.subscriber: Optional[NATSSubscriber] = None
    self.publisher: Optional[NATSPublisher] = None
    self._footer: str = ""
    self._market_type = self._market_type_value(settings_dict.get("market_type"))
    self._account_id = str(settings_dict.get("crypto_exchange", "CRYPTO"))

  @staticmethod
  def _market_type_value(mt) -> str:
    return mt.value if isinstance(mt, MarketTypeEnum) else str(mt or "CRYPTO")

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  def connect(self) -> bool:
    if not self.gateway.connect():
      log.error("[Crypto Process] Could not connect to exchange. Exiting.")
      return False

    self.subscriber = NATSSubscriber(
      url=self.settings["nats_url"],
      subjects=_parse_nats_subjects(self.settings.get("signal_subjects", "")),
      publish_subjects=[NatsSubjectEnum.TRADE],
      token=self.settings.get("nats_token"),
      account_footer_fn=self.gateway.get_account_footer,
      enqueue_fn=self.ctx.nats_enqueue,
    )
    self.subscriber.connect()

    self.publisher = NATSPublisher(
      url=self.settings["nats_url"],
      publish_subjects=[NatsSubjectEnum.TRADE],
      token=self.settings.get("nats_token"),
    )
    self.publisher.connect()

    self._footer = self.gateway.get_account_footer()
    return True

  def shutdown(self) -> None:
    if self.subscriber is not None:
      self.subscriber.close()
    if self.publisher is not None:
      self.publisher.close()
    self.gateway.close()

  def send_startup_notification(self) -> None:
    self.ctx.direct_notifier.send_message(
      CryptoMessagePresenter.startup(self.settings, self._footer)
    )

  def send_shutdown_notification(self) -> None:
    self.ctx.direct_notifier.send_message(
      CryptoMessagePresenter.shutdown(self._footer)
    )

  def start_market_jobs(self, stop_event) -> None:
    # Change-data-capture → NATS TRADE (shared with FOREX).
    PositionCDC(
      account_id=self._account_id,
      publisher=self.publisher,
      db_service=self.ctx.db_service,
      account_info_fn=self._account_snapshot,
      account_name=self.settings.get("mt5_name"),
      market_type=self.settings.get("market_type"),
      strategy_magic_map={},
    ).start(stop_event=stop_event)

    # Exchange-side event ingestion (Binance → websocket user data stream).
    event_stream = self.gateway.create_event_stream(self._on_exchange_close)
    if event_stream is not None:
      event_stream.start(stop_event=stop_event)
    else:
      log.info("[Crypto Process] Exchange has no push event stream; skipping.")

  def _account_snapshot(self) -> Optional[dict]:
    info = self.gateway.get_account()
    if not info:
      return None
    return {"balance": info.get("balance"), "leverage": None}

  # ── Main loop ─────────────────────────────────────────────────────────── #

  def run(self, stop_event) -> None:
    for subject, raw in self.subscriber.listen(stop_event=stop_event):
      self._process_message(subject, raw)

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

  # ── Admin FLAT ────────────────────────────────────────────────────────── #

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

  # ── Per-message processing ────────────────────────────────────────────── #

  def _process_message(self, subject, raw) -> None:
    if subject == NatsSubjectEnum.ADMIN:
      self._handle_admin_message(raw)
      return
    try:
      signal = SignalSchema(**json.loads(raw))
    except json.JSONDecodeError as err:
      log.error("[Crypto Process] Malformed JSON: %s", err)
      return
    except ValidationError as err:
      log.error("[Crypto Process] Signal validation failed: %s", err)
      return

    log.info(
      "[Crypto Process] Processing Signal: %s | %s | TV Time: %s",
      signal.symbol, signal.action.value, signal.timestamp,
    )

    result = self.handler.handle(signal)

    self.ctx.db_service.log_position(
      strategy=signal.strategy,
      ticket=result.get("ticket"),
      source_ticket=result.get("source_ticket", result.get("ticket")),
      symbol=signal.symbol,
      action=signal.action.value,
      volume=result.get("volume", signal.quantity),
      price=result.get("price", signal.price),
      sl=getattr(signal, "sl", None),
      tp1=getattr(signal, "tp1", None),
      mt5_retcode=result.get("retcode", -1),
      comment=result.get("comment", ""),
      message=signal.model_dump_json(),
      author=LogAuthorEnum.BROKER.value,
      market_type=self._market_type,
    )

    if result.get("success"):
      action_val = signal.action.value
      pos_ticket = result.get("source_ticket", result.get("ticket"))
      signal_json = signal.model_dump_json()

      for fc in result.get("forced_closed", []):
        self.ctx.channel_notifier.send_message(
          CryptoMessagePresenter.force_closed(
            signal.symbol, signal.strategy, fc, self.gateway.get_account_footer()
          )
        )

      if action_val in ("LONG", "SHORT"):
        self.ctx.db_service.insert_position(
          ticket=pos_ticket,
          strategy=signal.strategy,
          symbol=signal.symbol,
          action=action_val.lower(),
          volume=result.get("volume", signal.quantity),
          opened_price=result.get("price", signal.price),
          mt5_retcode=result.get("retcode"),
          comment=result.get("comment", ""),
          message=signal_json,
          magic=None,
          market_type=self._market_type,
        )
      else:
        status = _CLOSE_STATUS_MAP.get(action_val)
        if status:
          self.ctx.db_service.update_position_status(
            source_ticket=pos_ticket,
            status=status,
            new_ticket=result.get("ticket"),
            closed_price=result.get("price"),
            mt5_retcode=result.get("retcode"),
            comment=result.get("comment", ""),
            message=signal_json,
          )
      msg = CryptoMessagePresenter.order_filled(
        signal, result, pos_ticket, self.gateway.get_account_footer()
      )
    else:
      msg = CryptoMessagePresenter.order_failed(
        signal, result, self.gateway.get_account_footer()
      )
    self.ctx.channel_notifier.send_message(msg)
