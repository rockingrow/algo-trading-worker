import asyncio
import multiprocessing
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from worker.logger import get_logger
from worker.services.db_service import DBService
from worker.services.mt5_process import MT5ProcessManager
from worker.settings import settings

log = get_logger("worker.app")

_WATCHDOG_INTERVAL = 10  # seconds between liveness checks


def _worker_process_main(settings_dict: dict, stop_event) -> None:
  """
  Entry point for the child process.
  All service imports are local so the parent process never loads MT5 / NATS / DB.
  """
  import json

  from pydantic import ValidationError

  from worker.core.market_strategy import MarketStrategyFactory
  from worker.core.signal_handler import SignalHandler
  from worker.logger import get_logger
  from worker.mt5.executor import MT5Executor
  from worker.mt5.mt5 import MT5
  from worker.schemas.broker_schema import SignalActionEnum, SignalSchema
  from worker.schemas.nats_schema import NatsSubjectEnum
  from worker.schemas.position_schema import PositionStatusEnum
  from worker.services.db_service import DBService
  from worker.services.job_service import MT5EventJob
  from worker.services.mt5_process import (
    _box,
    _ensure_mt5_connected,
    _format_volume,
    _handle_flat_signal,
    _mt5_health_thread,
  )
  from worker.services.nats_service import NATSPublisher, NATSSubscriber
  from worker.services.notification_service import TelegramNotification
  from worker.services.position_watcher import PositionWatcher

  log = get_logger("worker.mt5_process")
  log.info("[MT5 Process] Started (PID=%d)", multiprocessing.current_process().pid)

  bridge = MT5(
    server=settings_dict["mt5_server"],
    login=settings_dict["mt5_login"],
    password=settings_dict["mt5_password"],
    path=settings_dict.get("mt5_path"),
  )

  # ── 1. Connect (blocking, unlimited retries) ──────────────────────────── #
  connected = bridge.reconnect(max_attempts=0, delay_seconds=10.0)
  if not connected:
    log.error("[MT5 Process] Could not connect to MT5. Exiting.")
    return

  # ── 2. Set up NATS subscriber ─────────────────────────────────────────── #
  subscriber = NATSSubscriber(
    url=settings_dict["nats_url"],
    subjects=[NatsSubjectEnum.SIGNAL, NatsSubjectEnum.ADMIN],
    publish_subjects=[NatsSubjectEnum.TRADE],
    token=settings_dict.get("nats_token"),
    account_footer_fn=bridge.get_account_footer,
  )
  subscriber.connect()

  # ── 2b. Set up NATS publisher (TRADE subject → broker) ───────────────── #
  publisher = NATSPublisher(
    url=settings_dict["nats_url"],
    publish_subjects=[NatsSubjectEnum.TRADE],
    token=settings_dict.get("nats_token"),
  )
  publisher.connect()

  # ── 3. Set up trading components ──────────────────────────────────────── #
  executor = MT5Executor(
    magic_number=settings_dict["magic_number"],
    slippage_deviation=settings_dict["slippage_deviation"],
  )
  strategy = MarketStrategyFactory.create(executor=executor)
  handler = SignalHandler(strategy)
  db_service = DBService()
  db_service.initialize()

  notifier = TelegramNotification()
  channel_notifier = TelegramNotification(
    chat_id=settings_dict.get("telegram_chat_channel_id")
    or settings_dict.get("telegram_chat_id")
  )
  footer = bridge.get_account_footer()

  volume_enabled = settings_dict.get("volume_decision_enabled", False)
  capital = settings_dict.get("capital")
  capital_currency = settings_dict.get("capital_currency", "")
  risk_percentage = settings_dict.get("risk_percentage")
  use_account_equity = settings_dict.get("use_account_equity", False)

  volume_config = (
    f"VOLUME_DECISION_ENABLED: <b>{volume_enabled}</b>\n"
    f"CAPITAL: <b>{capital} {capital_currency}</b>\n"
    f"RISK_PERCENTAGE: <b>{risk_percentage}%</b>\n"
    f"USE_ACCOUNT_EQUITY: <b>{use_account_equity}</b>\n"
    f"POSITION_TP1_PERCENT: <b>{settings_dict.get('position_tp1_percent', 0)}%</b>\n"
  )

  notifier.send_message(
    _box(
      f"🟢 <b>[Connected] MT5 Worker</b>\n\n"
      f"{volume_config}"
      f"----------------------------------\n"
      f"{footer}"
    )
  )
  log.info("[MT5 Process] Worker loop started.")

  # ── 4. Start MT5 health-check thread ─────────────────────────────────── #
  health_thread = threading.Thread(
    target=_mt5_health_thread,
    args=(bridge, notifier, bridge.get_account_footer, stop_event, log),
    name="mt5-health",
    daemon=True,
  )
  health_thread.start()

  # ── 4b. Start terminal-close polling job ──────────────────────────────── #
  event_job = MT5EventJob(
    magic_number=settings_dict["magic_number"],
    db_service=db_service,
    notifier=channel_notifier,
  )
  event_job.start(stop_event=stop_event)

  # ── 4c. Start position watcher (SQLite positions → NATS TRADE) ───────── #
  position_watcher = PositionWatcher(
    account_id=str(settings_dict["mt5_login"]),
    publisher=publisher,
    account_info_fn=bridge.get_account_status,
    account_name=settings_dict.get("mt5_name"),
    market_type=settings_dict.get("market_type"),
  )
  position_watcher.start(stop_event=stop_event)

  # ── 5. Signal processing loop ─────────────────────────────────────────── #
  try:
    for subject, raw in subscriber.listen(stop_event=stop_event):
      if subject == NatsSubjectEnum.ADMIN:
        # TODO: handle ADMIN messages
        continue
      try:
        signal = SignalSchema(**json.loads(raw))
      except json.JSONDecodeError as err:
        log.error("[MT5 Process] Malformed JSON: %s", err)
        continue
      except ValidationError as err:
        log.error("[MT5 Process] Signal validation failed: %s", err)
        continue
      if not _ensure_mt5_connected(bridge, notifier, footer, log):
        continue

      # ── FLAT: close all open positions for strategy+symbol ─────────────── #
      if signal.action == SignalActionEnum.FLAT:
        _handle_flat_signal(
          signal, executor, db_service, notifier, channel_notifier, bridge, log
        )
        continue

      log.info(
        "[MT5 Process] Processing Signal: %s | %s | TV Time: %s",
        signal.symbol,
        signal.action.value,
        signal.timestamp,
      )

      result = handler.handle(signal)

      db_service.log_position(
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
        if action_val in ("LONG", "SHORT"):
          db_service.insert_position(
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
          _close_status_map = {
            "TP1": PositionStatusEnum.TP1,
            "TP2": PositionStatusEnum.TP2,
            "SL": PositionStatusEnum.SL,
            "R_SL": PositionStatusEnum.R_SL,
          }
          status = _close_status_map.get(action_val)
          if status:
            db_service.update_position_status(
              source_ticket=pos_ticket,
              status=status,
              new_ticket=result.get("ticket"),
              closed_price=result.get("price"),
              mt5_retcode=result.get("retcode"),
              comment=result.get("comment", ""),
              message=signal_json,
            )
        msg = _box(
          f"✅ <b>Order Filled</b>\n\n"
          f"Symbol: <b>{signal.symbol}</b>\n"
          f"Action: <b>{signal.action.value}</b>\n"
          f"Price: <b>{result.get('price')}</b>\n"
          f"Volume: <b>{_format_volume(result.get('volume'), auto_calculated=True)}</b>\n"
          f"Ticket: <b>{result.get('ticket')}</b>\n"
          f"Source Ticket: <b>{pos_ticket}</b>\n"
          f"----------------------------------\n"
          f"{bridge.get_account_footer()}"
        )
      else:
        msg = _box(
          f"❌ <b>Order Failed</b>\n\n"
          f"Symbol: <b>{signal.symbol}</b>\n"
          f"Action: <b>{signal.action.value}</b>\n"
          f"Price: <b>{result.get('price')}</b>\n"
          f"Error: <b>{result.get('comment')}</b> (Code <b>{result.get('retcode')}</b>)\n"
          f"----------------------------------\n"
          f"{bridge.get_account_footer()}"
        )
      channel_notifier.send_message(msg)

  except KeyboardInterrupt:
    log.info("[MT5 Process] Received shutdown signal.")
  except Exception as e:
    log.exception("[MT5 Process] Unexpected error: %s", e)
  finally:
    subscriber.close()
    publisher.close()
    bridge.shutdown()
    notifier.send_message(_box(f"🛑 <b>[Disconnected] MT5 Worker</b>{footer}"))
    log.info("[MT5 Process] Exiting.")


async def _watchdog(manager: MT5ProcessManager) -> None:
  """Restart the MT5 child process if it dies unexpectedly."""
  while True:
    await asyncio.sleep(_WATCHDOG_INTERVAL)
    if manager.stopping:
      break
    if not manager.is_alive:
      log.warning("MT5 worker process died unexpectedly — restarting...")
      try:
        await asyncio.to_thread(manager.restart)
        log.info("MT5 worker process restarted successfully.")
      except Exception as exc:
        log.exception("Failed to restart MT5 worker process: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
  # 1. Initialize Database (fast, local SQLite)
  db_service = DBService()
  db_service.initialize()
  log.info("Database initialized.")

  # 2. Build the settings dict passed to the child process.
  #    The child process imports MetaTrader5 — the parent (this process) never does.
  #    This is the key fix: the MT5 C extension's GIL-holding calls only happen
  #    in the child process, so the FastAPI event loop is never frozen.
  settings_dict = {
    "mt5_server": settings.mt5_server,
    "mt5_login": settings.mt5_login,
    "mt5_password": settings.mt5_password,
    "mt5_path": settings.mt5_path,
    "mt5_name": settings.mt5_name,
    "market_type": settings.market_type,
    "nats_url": settings.nats_url,
    "nats_token": settings.nats_token,
    "magic_number": settings.magic_number,
    "slippage_deviation": settings.slippage_deviation,
    "broker_api_url": settings.broker_api_url,
    "broker_api_key": settings.broker_api_key,
    "telegram_enabled": settings.telegram_enabled,
    "telegram_bot_token": settings.telegram_bot_token,
    "telegram_chat_id": settings.telegram_chat_id,
    "telegram_chat_channel_id": settings.telegram_chat_channel_id,
    "volume_decision_enabled": settings.volume_decision_enabled,
    "capital": settings.capital,
    "capital_currency": settings.capital_currency,
    "risk_percentage": settings.risk_percentage,
    "use_account_equity": settings.use_account_equity,
    "position_tp1_percent": settings.position_tp1_percent,
  }

  manager = MT5ProcessManager(settings_dict, _worker_process_main)
  app.state.mt5_manager = manager

  # 3. Spawn child process in a thread so lifespan can yield immediately.
  #    Process.start() itself is fast (fork/spawn), but we offload it anyway
  #    to keep the event loop fully responsive from the first millisecond.
  await asyncio.to_thread(manager.start)

  watchdog_task = asyncio.create_task(_watchdog(manager))

  log.info(
    "FastAPI lifespan started. MT5 worker running in subprocess. API is now accepting requests."
  )
  yield

  # 4. Shutdown: stop watchdog then child process.
  watchdog_task.cancel()
  try:
    await watchdog_task
  except asyncio.CancelledError:
    pass

  log.info("Shutting down MT5 worker subprocess...")
  await asyncio.to_thread(manager.stop)
  log.info("MT5 worker subprocess stopped.")


def create_app() -> FastAPI:
  """Build and return the FastAPI application with all routes wired up."""
  app = FastAPI(
    title="MT5 Trading Worker",
    description="Connects to MT5 and executes trades based on NATS signals.",
    version="2.0.0",
    lifespan=lifespan,
  )

  return app
