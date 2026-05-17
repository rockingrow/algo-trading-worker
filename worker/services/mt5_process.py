"""
worker/services/mt5_process.py
──────────────────────────────
Runs the blocking MT5 + NATS worker inside a **separate OS process** so the
GIL-holding MetaTrader5 C extension never freezes the FastAPI/uvicorn event loop.

Architecture
────────────
  FastAPI process  ──start/stop──▶  MT5Worker process
                                    ├─ MT5 reconnect loop
                                    └─ NATS listen loop

The FastAPI process only manages the subprocess lifetime; all MT5/NATS blocking
code lives in the child process and is therefore 100% GIL-isolated.
"""

from __future__ import annotations

import multiprocessing
import threading
import time
from typing import Optional

from worker.schemas.broker_schema import SignalActionEnum
from worker.schemas.position_schema import PositionStatusEnum

_MT5_HEALTH_INTERVAL = 15  # seconds between MT5 connection health checks


def _box(text: str) -> str:
  return f"<pre>{text.strip()}</pre>"


def _format_volume(volume: float, auto_calculated: bool = False) -> str:
  """Format volume with icon if auto-calculated."""
  icon = "⚙️" if auto_calculated else ""
  return f"{volume} lot {icon}".strip() if auto_calculated else f"{volume} lot"


# ---------------------------------------------------------------------------
# MT5 health-check thread — runs alongside the ZMQ signal loop
# ---------------------------------------------------------------------------


def _mt5_health_thread(bridge, notifier, footer_fn, stop_event, log) -> None:
  """
  Runs in a daemon thread inside the child process.
  Proactively detects MT5 disconnect and relaunches/reconnects the terminal
  without waiting for a ZMQ signal to arrive.
  """
  while not stop_event.is_set():
    time.sleep(_MT5_HEALTH_INTERVAL)
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


# ---------------------------------------------------------------------------
# Signal-loop helpers
# ---------------------------------------------------------------------------


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


def _handle_flat_signal(
  signal, executor, db_service, notifier, channel_notifier, bridge, log
) -> None:
  """
  Execute a FLAT signal: close all OPENED/TP1 positions for a strategy+symbol.

  Steps:
    1. Query DB for positions with status OPENED or TP1.
    2. Close them all via MT5 at market price (100 % volume).
    3. Write a row to position_logs and update positions.status → FLATTED.
    4. Send a Telegram notification.
  """
  strategy = signal.strategy
  symbol = signal.symbol

  db_positions = db_service.get_open_positions_by_strategy(strategy, symbol)
  if not db_positions:
    log.warning("[FLAT] No open DB positions | strategy=%s symbol=%s", strategy, symbol)
    notifier.send_message(
      _box(
        f"⚡ <b>FLAT [{symbol}]</b>\n\nNo open positions found for strategy <b>{strategy}</b>"
      )
    )
    return

  log.info(
    "[FLAT] Closing %d position(s) | strategy=%s symbol=%s",
    len(db_positions),
    strategy,
    symbol,
  )

  result = executor.close_all_positions(symbol, reason="FLAT")

  for pos in db_positions:
    source_ticket = pos["source_ticket"]
    db_service.log_position(
      strategy=strategy,
      ticket=result.get("ticket"),
      source_ticket=source_ticket,
      symbol=symbol,
      action="FLAT",
      volume=pos["volume"],
      price=result.get("price", 0.0),
      sl=None,
      tp1=None,
      mt5_retcode=result.get("retcode", -1),
      comment=result.get("comment", "FLAT command"),
      author="broker",
    )
    if result.get("success"):
      db_service.update_position_status(
        source_ticket=source_ticket,
        status=PositionStatusEnum.FLATTED,
        new_ticket=result.get("ticket"),
        closed_price=result.get("price"),
        mt5_retcode=result.get("retcode"),
        comment="FLAT by webhook",
      )

  footer = bridge.get_account_footer()
  if result.get("success"):
    msg = _box(
      f"⚡ <b>FLAT Executed</b>\n\n"
      f"Symbol: <b>{symbol}</b>\n"
      f"Strategy: <b>{strategy}</b>\n"
      f"Positions closed: <b>{len(db_positions)}</b>\n"
      f"Price: <b>{result.get('price')}</b>\n"
      f"Volume: <b>{_format_volume(result.get('volume'), auto_calculated=True)}</b>\n"
      f"----------------------------------\n"
      f"{footer}"
    )
    log.info(
      "[FLAT] Done | strategy=%s symbol=%s price=%s",
      strategy,
      symbol,
      result.get("price"),
    )
  else:
    msg = _box(
      f"❌ <b>FLAT Failed</b>\n\n"
      f"Symbol: <b>{symbol}</b>\n"
      f"Strategy: <b>{strategy}</b>\n"
      f"Error: <b>{result.get('comment')}</b> (Code <b>{result.get('retcode')}</b>)\n"
      f"----------------------------------\n"
      f"{footer}"
    )
    log.error(
      "[FLAT] Failed | strategy=%s symbol=%s comment=%s",
      strategy,
      symbol,
      result.get("comment"),
    )

  channel_notifier.send_message(msg)


# ---------------------------------------------------------------------------
# Child-process entry point
# ---------------------------------------------------------------------------


def _worker_process_main(
  settings_dict: dict, stop_event: multiprocessing.Event
) -> None:
  """
  Entry point that runs inside the child process.
  Imports MT5 / ZMQ only here so the parent process never loads the C extension.
  """

  import json

  from pydantic import ValidationError

  from worker.core.market_strategy import MarketStrategyFactory
  from worker.core.signal_handler import SignalHandler
  from worker.logger import get_logger
  from worker.mt5.executor import MT5Executor
  from worker.mt5.mt5 import MT5
  from worker.schemas.broker_schema import SignalSchema
  from worker.schemas.nats_schema import NatsSubjectEnum
  from worker.services.db_service import DBService
  from worker.services.job_service import MT5EventJob
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
        # For entries (LONG/SHORT), `ticket` is the position.
        # For exits (TP1/TP2/SL/R_SL), `ticket` is the new exit deal, but `source_ticket` is the original position.
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


# ---------------------------------------------------------------------------
# Manager used from the FastAPI lifespan
# ---------------------------------------------------------------------------


class MT5ProcessManager:
  """
  Manages a child process that runs the MT5 + ZMQ worker.
  The parent (FastAPI) process never imports MetaTrader5 directly, so its
  event loop is completely free from GIL interference.
  """

  def __init__(self, settings_dict: dict) -> None:
    self._settings_dict = settings_dict
    self._process: Optional[multiprocessing.Process] = None
    self._stop_event = multiprocessing.Event()
    self._stopping = False

  def start(self) -> None:
    """Spawn the child process."""
    self._stop_event.clear()
    self._process = multiprocessing.Process(
      target=_worker_process_main,
      args=(self._settings_dict, self._stop_event),
      name="mt5-worker",
      daemon=True,
    )
    self._process.start()

  def stop(self) -> None:
    """Signal the child to shut down gracefully, then force-kill if needed."""
    self._stopping = True
    if self._process and self._process.is_alive():
      self._stop_event.set()
      self._process.join(timeout=15)  # wait for Telegram notification to send
      if self._process.is_alive():
        self._process.terminate()
        self._process.join(timeout=5)
        if self._process.is_alive():
          self._process.kill()

  def restart(self) -> None:
    """Restart the child process after an unexpected crash."""
    self._stopping = False
    self._stop_event.clear()
    self._process = multiprocessing.Process(
      target=_worker_process_main,
      args=(self._settings_dict, self._stop_event),
      name="mt5-worker",
      daemon=True,
    )
    self._process.start()

  @property
  def is_alive(self) -> bool:
    return self._process is not None and self._process.is_alive()

  @property
  def stopping(self) -> bool:
    return self._stopping
