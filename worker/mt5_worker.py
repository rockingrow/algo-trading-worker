import json
import multiprocessing
import threading

from pydantic import ValidationError

from worker.logger import get_logger
from worker.mt5.manager import (
  _ensure_mt5_connected,
  _format_volume,
  _handle_flat_signal,
  _mt5_health_thread,
)
from worker.schemas.broker_schema import SignalActionEnum, SignalSchema
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.services.notification_service import TelegramNotification, _box
from worker.settings import NATS_REQUIRED_LISTENING_SUBJECTS

log = get_logger("worker.mt5.manager")

_CLOSE_STATUS_MAP = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
}


def _parse_nats_subjects(raw: str) -> list[NatsSubjectEnum]:
  parsed: set[NatsSubjectEnum] = set()
  for s in raw.split(","):
    s = s.strip()
    try:
      parsed.add(NatsSubjectEnum(s))
    except ValueError:
      log.warning("[MT5 Process] Unknown NATS subject: %r — skipping", s)
  return list(NATS_REQUIRED_LISTENING_SUBJECTS | parsed)


def _process_message(
  subject, raw, bridge, notifier, footer, executor, handler, db_service, channel_notifier
) -> None:
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
  if not _ensure_mt5_connected(bridge, notifier, footer, log):
    return
  if signal.action == SignalActionEnum.FLAT:
    _handle_flat_signal(signal, executor, db_service, notifier, channel_notifier, bridge, log)
    return

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
      status = _CLOSE_STATUS_MAP.get(action_val)
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


def worker_initialized(settings_dict: dict, stop_event) -> None:
  """Entry point for the MT5 child process.

  Safe imports (schemas, services, helpers) are at module level.
  Only imports that trigger MetaTrader5 / heavy lib loading are kept local
  so the parent FastAPI process never loads the MT5 C extension.
  """
  from worker.core.market_strategy import MarketStrategyFactory
  from worker.core.signal_handler import SignalHandler
  from worker.jobs.cdc_job import PositionCDC
  from worker.jobs.mt5_event_job import MT5EventJob
  from worker.mt5.executor import MT5Executor
  from worker.mt5.mt5 import MT5
  from worker.services.db_service import DBService
  from worker.services.nats_service import NATSPublisher, NATSSubscriber

  log.info("[MT5 Process] Started (PID=%d)", multiprocessing.current_process().pid)

  bridge = MT5(
    server=settings_dict["mt5_server"],
    login=settings_dict["mt5_login"],
    password=settings_dict["mt5_password"],
    path=settings_dict.get("mt5_path"),
  )

  connected = bridge.reconnect(max_attempts=0, delay_seconds=10.0)
  if not connected:
    log.error("[MT5 Process] Could not connect to MT5. Exiting.")
    return

  subscriber = NATSSubscriber(
    url=settings_dict["nats_url"],
    subjects=_parse_nats_subjects(settings_dict.get("signal_subjects", "")),
    publish_subjects=[NatsSubjectEnum.TRADE],
    token=settings_dict.get("nats_token"),
    account_footer_fn=bridge.get_account_footer,
  )
  subscriber.connect()

  publisher = NATSPublisher(
    url=settings_dict["nats_url"],
    publish_subjects=[NatsSubjectEnum.TRADE],
    token=settings_dict.get("nats_token"),
  )
  publisher.connect()

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

  volume_config = (
    f"VOLUME_DECISION_ENABLED: <b>{settings_dict.get('volume_decision_enabled', False)}</b>\n"
    f"CAPITAL: <b>{settings_dict.get('capital')} {settings_dict.get('capital_currency', '')}</b>\n"
    f"RISK_PERCENTAGE: <b>{settings_dict.get('risk_percentage')}%</b>\n"
    f"USE_ACCOUNT_EQUITY: <b>{settings_dict.get('use_account_equity', False)}</b>\n"
    f"POSITION_TP1_PERCENT: <b>{settings_dict.get('position_tp1_percent', 0)}%</b>\n"
  )
  notifier.send_message(
    _box(f"🟢 <b>[Connected] MT5 Worker</b>\n\n{volume_config}----------------------------------\n{footer}")
  )
  log.info("[MT5 Process] Worker loop started.")

  threading.Thread(
    target=_mt5_health_thread,
    args=(bridge, notifier, bridge.get_account_footer, stop_event, log),
    name="mt5-health",
    daemon=True,
  ).start()

  MT5EventJob(
    magic_number=settings_dict["magic_number"],
    db_service=db_service,
    notifier=channel_notifier,
  ).start(stop_event=stop_event)

  PositionCDC(
    account_id=str(settings_dict["mt5_login"]),
    publisher=publisher,
    db_service=db_service,
    account_info_fn=bridge.get_account_status,
    account_name=settings_dict.get("mt5_name"),
    market_type=settings_dict.get("market_type"),
  ).start(stop_event=stop_event)

  try:
    for subject, raw in subscriber.listen(stop_event=stop_event):
      _process_message(subject, raw, bridge, notifier, footer, executor, handler, db_service, channel_notifier)
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
