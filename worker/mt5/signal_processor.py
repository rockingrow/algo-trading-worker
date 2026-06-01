"""
worker/mt5/signal_processor.py
──────────────────────────────
MT5/FOREX-specific signal processor.

Owns all dependencies that are specific to the MT5 broker: the MT5 bridge
itself, the NATS subscriber/publisher (which embeds account-footer info),
the executor, the strategy/handler stack, and the MT5-specific background
jobs (health thread, terminal-close detection, position CDC).

Receives a :class:`WorkerContext` for market-agnostic infrastructure
(DB, outbox/direct notifiers, NotificationJob).

Lifecycle (all called from inside the child process):

    processor = Mt5SignalProcessor(ctx, settings_dict)
    if not processor.connect(): return
    processor.send_startup_notification()
    processor.start_market_jobs(stop_event)
    try:
        processor.run(stop_event)
    finally:
        processor.shutdown()
        processor.send_shutdown_notification()

All heavy MetaTrader5 imports are at module level — this module must
only be imported from the child process (i.e. lazy-imported inside
:func:`worker.mt5_worker.mt5_worker_main`) so the parent FastAPI process
never loads the MT5 C extension.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Optional

from pydantic import ValidationError

from worker.context import WorkerContext
from worker.core.config import ExecutionConfig
from worker.core.market_strategy import MarketStrategyFactory
from worker.core.signal_handler import SignalHandler
from worker.jobs.cdc_job import PositionCDC
from worker.jobs.mt5_event_job import MT5EventJob
from worker.logger import get_logger
from worker.mt5.bridge import MT5
from worker.mt5.executor import MT5Executor
from worker.mt5.message_presenter import TradeMessagePresenter
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalSchema
from worker.services.nats_service import NATSPublisher, NATSSubscriber
from worker.services.notification_service import _box
from worker.settings import MT5_HEALTH_INTERVAL, NATS_REQUIRED_LISTENING_SUBJECTS

log = get_logger("worker.mt5.signal_processor")

_CLOSE_STATUS_MAP = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
  "FLAT": PositionStatusEnum.FLATTED,
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


# ── Child-process helpers ────────────────────────────────────────────────── #


def _mt5_health_thread(bridge, notifier, footer_fn, stop_event, log) -> None:
  """
  Runs in a daemon thread inside the child process.
  Proactively detects MT5 disconnect and relaunches/reconnects the terminal
  without waiting for a ZMQ signal to arrive.
  """
  while not stop_event.is_set():
    time.sleep(MT5_HEALTH_INTERVAL)
    if stop_event.is_set():
      break
    try:
      if not bridge.is_connected():
        log.warning(
          "[MT5 Health] MT5 disconnected — attempting to relaunch/reconnect..."
        )
        notifier.send_message(
          _box(f"⚠️ <b>[Disconnected] MT5 — reconnecting…</b>{footer_fn()}")
        )
        reconnected = bridge.reconnect(max_attempts=15, delay_seconds=10.0)
        if reconnected:
          log.info("[MT5 Health] MT5 reconnected successfully.")
          notifier.send_message(_box(f"🟢 <b>[Connected] MT5</b>{footer_fn()}"))
        else:
          log.error(
            "[MT5 Health] MT5 reconnect failed after 15 attempts — killing and restarting terminal64.exe..."
          )
          notifier.send_message(
            _box(
              f"🔴 <b>[Disconnected] MT5 reconnect failed</b>\n\n"
              f"Killing and restarting terminal64.exe…{footer_fn()}"
            )
          )
          restarted = bridge.restart_terminal(startup_wait=15.0)
          if restarted:
            log.info("[MT5 Health] terminal64.exe restarted — retrying reconnect...")
            reconnected = bridge.reconnect(max_attempts=15, delay_seconds=10.0)
            if reconnected:
              log.info("[MT5 Health] MT5 reconnected after terminal restart.")
              notifier.send_message(
                _box(f"🟢 <b>[Connected] MT5 after terminal restart</b>{footer_fn()}")
              )
            else:
              log.error(
                "[MT5 Health] MT5 still unreachable after terminal restart — manual intervention required."
              )
              notifier.send_message(
                _box(
                  f"🔴 <b>MT5 CRASHED</b>\n\n"
                  f"Failed to reconnect even after restarting terminal64.exe.\n"
                  f"Please restart the terminal manually.{footer_fn()}"
                )
              )
          else:
            log.error(
              "[MT5 Health] terminal64.exe restart failed (path not configured or exe missing) — manual intervention required."
            )
            notifier.send_message(
              _box(
                f"🔴 <b>MT5 CRASHED</b>\n\n"
                f"terminal64.exe restart failed — path not configured or exe missing.\n"
                f"Please restart the terminal manually.{footer_fn()}"
              )
            )
    except Exception as exc:
      log.exception("[MT5 Health] Unexpected error in health thread: %s", exc)


def _ensure_mt5_connected(bridge, notifier, footer: str, log) -> bool:
  """Return True if MT5 is (or becomes) connected; False if reconnect fails."""
  if bridge.is_connected():
    return True
  log.warning("[MT5 Process] MT5 connection lost. Reconnecting...")
  notifier.send_message(_box(f"⚠️ <b>[Disconnected] MT5 — reconnecting…</b>{footer}"))
  reconnected = bridge.reconnect(max_attempts=0, delay_seconds=10.0)
  if reconnected:
    notifier.send_message(_box(f"🟢 <b>[Connected] MT5</b>{footer}"))
  else:
    notifier.send_message(
      _box(f"🔴 <b>[Disconnected] MT5 reconnect failed — signal dropped</b>{footer}")
    )
  return reconnected


# ── Processor ────────────────────────────────────────────────────────────── #


class Mt5SignalProcessor:
  """Composes MT5 broker bridge + NATS + executor/handler, drives the signal loop."""

  def __init__(self, ctx: WorkerContext, settings_dict: dict) -> None:
    self.ctx = ctx
    self.settings = settings_dict

    self.bridge = MT5(
      server=settings_dict["mt5_server"],
      login=settings_dict["mt5_login"],
      password=settings_dict["mt5_password"],
      path=settings_dict.get("mt5_path"),
    )

    self.config = ExecutionConfig.from_dict(settings_dict)
    self.executor = MT5Executor(
      magic_number=settings_dict["magic_number"],
      slippage_deviation=settings_dict["slippage_deviation"],
      config=self.config,
    )
    self.strategy = MarketStrategyFactory.create(
      executor=self.executor, config=self.config
    )
    self.handler = SignalHandler(self.strategy, ctx.db_service)

    self.subscriber: Optional[NATSSubscriber] = None
    self.publisher: Optional[NATSPublisher] = None
    self._footer: str = ""

  # ── Lifecycle ────────────────────────────────────────────────────────── #

  def connect(self) -> bool:
    if not self.bridge.reconnect(max_attempts=0, delay_seconds=10.0):
      log.error("[MT5 Process] Could not connect to MT5. Exiting.")
      return False

    self.subscriber = NATSSubscriber(
      url=self.settings["nats_url"],
      subjects=_parse_nats_subjects(self.settings.get("signal_subjects", "")),
      publish_subjects=[NatsSubjectEnum.TRADE],
      token=self.settings.get("nats_token"),
      account_footer_fn=self.bridge.get_account_footer,
      enqueue_fn=self.ctx.nats_enqueue,
    )
    self.subscriber.connect()

    self.publisher = NATSPublisher(
      url=self.settings["nats_url"],
      publish_subjects=[NatsSubjectEnum.TRADE],
      token=self.settings.get("nats_token"),
    )
    self.publisher.connect()

    self._footer = self.bridge.get_account_footer()
    return True

  def shutdown(self) -> None:
    if self.subscriber is not None:
      self.subscriber.close()
    if self.publisher is not None:
      self.publisher.close()
    self.bridge.shutdown()

  def send_startup_notification(self) -> None:
    self.ctx.direct_notifier.send_message(
      TradeMessagePresenter.startup(self.settings, self._footer)
    )

  def send_shutdown_notification(self) -> None:
    self.ctx.direct_notifier.send_message(
      TradeMessagePresenter.shutdown(self._footer)
    )

  def start_market_jobs(self, stop_event) -> None:
    threading.Thread(
      target=_mt5_health_thread,
      args=(self.bridge, self.ctx.notifier, self.bridge.get_account_footer, stop_event, log),
      name="mt5-health",
      daemon=True,
    ).start()

    MT5EventJob(
      magic_number=self.settings["magic_number"],
      db_service=self.ctx.db_service,
      notifier=self.ctx.channel_notifier,
    ).start(stop_event=stop_event)

    PositionCDC(
      account_id=str(self.settings["mt5_login"]),
      publisher=self.publisher,
      db_service=self.ctx.db_service,
      account_info_fn=self.bridge.get_account_status,
      account_name=self.settings.get("mt5_name"),
      market_type=self.settings.get("market_type"),
    ).start(stop_event=stop_event)

  # ── Main loop ────────────────────────────────────────────────────────── #

  def run(self, stop_event) -> None:
    for subject, raw in self.subscriber.listen(stop_event=stop_event):
      self._process_message(subject, raw)

  def _process_message(self, subject, raw) -> None:
    if subject == NatsSubjectEnum.ADMIN:
      return
    try:
      signal = SignalSchema(**json.loads(raw))
    except json.JSONDecodeError as err:
      log.error("[MT5 Process] Malformed JSON: %s", err)
      return
    except ValidationError as err:
      log.error("[MT5 Process] Signal validation failed: %s", err)
      return

    if not _ensure_mt5_connected(self.bridge, self.ctx.notifier, self._footer, log):
      return

    log.info(
      "[MT5 Process] Processing Signal: %s | %s | TV Time: %s",
      signal.symbol,
      signal.action.value,
      signal.timestamp,
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
      author="broker",
    )

    if result.get("success"):
      action_val = signal.action.value
      pos_ticket = result.get("source_ticket", result.get("ticket"))
      signal_json = signal.model_dump_json()

      for fc in result.get("forced_closed", []):
        self.ctx.channel_notifier.send_message(
          TradeMessagePresenter.force_closed(
            signal.symbol, fc, self.bridge.get_account_footer()
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
      msg = TradeMessagePresenter.order_filled(
        signal, result, pos_ticket, self.bridge.get_account_footer()
      )
    else:
      msg = TradeMessagePresenter.order_failed(
        signal, result, self.bridge.get_account_footer()
      )
    self.ctx.channel_notifier.send_message(msg)
