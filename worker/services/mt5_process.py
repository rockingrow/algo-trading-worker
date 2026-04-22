"""
worker/services/mt5_process.py
──────────────────────────────
Runs the blocking MT5 + ZMQ worker inside a **separate OS process** so the
GIL-holding MetaTrader5 C extension never freezes the FastAPI/uvicorn event loop.

Architecture
────────────
  FastAPI process  ──start/stop──▶  MT5Worker process
                                    ├─ MT5 reconnect loop
                                    └─ ZMQ listen loop

The FastAPI process only manages the subprocess lifetime; all MT5/ZMQ blocking
code lives in the child process and is therefore 100% GIL-isolated.
"""

from __future__ import annotations

import multiprocessing
import threading
import time
from typing import Optional

_MT5_HEALTH_INTERVAL = 15  # seconds between MT5 connection health checks

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
        log.warning("[MT5 Health] MT5 disconnected — attempting to relaunch/reconnect...")
        notifier.send_message(f"⚠️ <b>MT5 disconnected — reconnecting…</b>{footer_fn()}")
        reconnected = bridge.reconnect(max_attempts=15, delay_seconds=5.0)
        if reconnected:
          log.info("[MT5 Health] MT5 reconnected successfully.")
          notifier.send_message(f"🟢 <b>MT5 reconnected</b>{footer_fn()}")
        else:
          log.error("[MT5 Health] MT5 reconnect failed after 15 attempts — terminal may have crashed.")
          notifier.send_message(
            f"🔴 <b>MT5 CRASHED</b>\n\n"
            f"Failed to relaunch MetaTrader 5 after 15 attempts.\n"
            f"Please restart the terminal manually.{footer_fn()}"
          )
    except Exception as exc:
      log.exception("[MT5 Health] Unexpected error in health thread: %s", exc)


# ---------------------------------------------------------------------------
# Child-process entry point
# ---------------------------------------------------------------------------


def _worker_process_main(settings_dict: dict, stop_event: multiprocessing.Event) -> None:
  """
  Entry point that runs inside the child process.
  Imports MT5 / ZMQ only here so the parent process never loads the C extension.
  """

  from worker.core.market_strategy import MarketStrategyFactory
  from worker.core.signal_handler import SignalHandler
  from worker.logger import get_logger
  from worker.mt5.executor import MT5Executor
  from worker.mt5.mt5 import MT5
  from worker.services.callback_service import CallbackService
  from worker.services.db_service import DBService
  from worker.services.notifications_service import TelegramNotification
  from worker.services.zmq_service import ZMQ

  log = get_logger("worker.mt5_process")
  log.info("[MT5 Process] Started (PID=%d)", multiprocessing.current_process().pid)

  bridge = MT5(
    server=settings_dict["mt5_server"],
    login=settings_dict["mt5_login"],
    password=settings_dict["mt5_password"],
    path=settings_dict.get("mt5_path"),
  )

  # ── 1. Connect (blocking, unlimited retries) ──────────────────────────── #
  connected = bridge.reconnect(max_attempts=0, delay_seconds=5.0)
  if not connected:
    log.error("[MT5 Process] Could not connect to MT5. Exiting.")
    return

  # ── 2. Set up ZMQ subscriber ──────────────────────────────────────────── #
  subscriber = ZMQ(
    host=settings_dict["zmq_sub_host"],
    curve_server_public_key=settings_dict.get("zmq_curve_server_public_key"),
    curve_client_public_key=settings_dict.get("zmq_curve_client_public_key"),
    curve_client_secret_key=settings_dict.get("zmq_curve_client_secret_key"),
  )
  subscriber.connect()

  # ── 3. Set up trading components ──────────────────────────────────────── #
  executor = MT5Executor(
    magic_number=settings_dict["magic_number"],
    slippage_deviation=settings_dict["slippage_deviation"],
  )
  strategy = MarketStrategyFactory.create(executor=executor)
  handler = SignalHandler(strategy)
  db_service = DBService()
  db_service.initialize()

  account_status = bridge.get_account_status() or {}
  callback_service = CallbackService(
    broker_api_url=settings_dict["broker_api_url"],
    account_id=str(settings_dict["mt5_login"]),
    api_key=settings_dict["broker_api_key"],
  )
  balance_at_start = account_status.get("balance", 0.0)

  notifier = TelegramNotification()
  footer = bridge.get_account_footer()

  zmq_host = settings_dict["zmq_sub_host"]
  notifier.send_message(f"🟢 <b>MT5 Worker connected</b>\n📡 ZMQ subscribed to <code>{zmq_host}</code>{footer}")
  log.info("[MT5 Process] Worker loop started.")

  # ── 4. Start MT5 health-check thread ─────────────────────────────────── #
  health_thread = threading.Thread(
    target=_mt5_health_thread,
    args=(bridge, notifier, bridge.get_account_footer, stop_event, log),
    name="mt5-health",
    daemon=True,
  )
  health_thread.start()

  # ── 5. Signal processing loop ─────────────────────────────────────────── #
  try:
    for signal in subscriber.listen(stop_event=stop_event):
      if not bridge.is_connected():
        log.warning("[MT5 Process] MT5 connection lost. Reconnecting...")
        notifier.send_message(f"⚠️ <b>MT5 connection lost — reconnecting…</b>{footer}")
        reconnected = bridge.reconnect(max_attempts=0, delay_seconds=5.0)
        if reconnected:
          notifier.send_message(f"🟢 <b>MT5 reconnected</b>{footer}")
        else:
          notifier.send_message(
            f"🔴 <b>MT5 reconnect failed — signal dropped</b>{footer}"
          )
          continue

      log.info(
        "[MT5 Process] Processing Signal: %s | %s | TV Time: %s",
        signal.symbol,
        signal.action,
        signal.timestamp,
      )

      result = handler.handle(signal)

      db_service.log_order(
        ticket=result.get("ticket"),
        source_ticket=result.get("source_ticket", result.get("ticket")),
        symbol=signal.symbol,
        action=signal.action,
        volume=result.get("volume", signal.quantity),
        price=result.get("price", signal.price),
        sl=getattr(signal, "sl", None),
        tp1=getattr(signal, "tp1", None),
        mt5_retcode=result.get("retcode", -1),
        comment=result.get("comment", ""),
        message=signal.model_dump_json(),
      )

      if result.get("success"):
        # For entries (LONG/SHORT), `ticket` is the position. 
        # For exits (TP1/TP2/SL/R_SL), `ticket` is the new exit deal, but `source_ticket` is the original position.
        # We want to callback with the same steady position ticket across all states.
        pos_ticket = result.get("source_ticket", result.get("ticket"))
        action_val = signal.action.value
        
        if action_val in ("LONG", "SHORT"):
          callback_service.notify_opened(
            signal=signal,
            ticket=pos_ticket,
            comment=result.get("comment", ""),
            volume=result.get("volume", signal.quantity),
            price=result.get("price", signal.price),
            balance_init=balance_at_start,
          )
        elif action_val == "TP1":
          callback_service.notify_partially_closed(
            ticket=str(pos_ticket),
            price=result.get("price", signal.price),
            remaining_quantity=result.get("volume", signal.quantity),
          )
        elif action_val in ("TP2", "SL", "R_SL"):
          callback_service.notify_closed(
            ticket=str(pos_ticket),
            price=result.get("price", signal.price),
            quantity=result.get("volume", signal.quantity),
            sl=getattr(signal, "sl", None),
          )
        msg = (
          f"✅ <b>Order Filled</b>\n\n"
          f"Symbol: {signal.symbol}\n"
          f"Action: {signal.action}\n"
          f"Volume: {result.get('volume')}\n"
          f"Ticket: {result.get('ticket')}{footer}"
        )
      else:
        callback_service.notify_rejected(
          signal=signal,
          reject_reason=result.get("comment", "Unknown error"),
          balance_init=balance_at_start,
        )
        msg = (
          f"❌ <b>Order Failed</b>\n\n"
          f"Symbol: {signal.symbol}\n"
          f"Action: {signal.action}\n"
          f"Error: {result.get('comment')} (Code {result.get('retcode')}){footer}"
        )
      notifier.send_message(msg)

  except KeyboardInterrupt:
    log.info("[MT5 Process] Received shutdown signal.")
  except Exception as e:
    log.exception("[MT5 Process] Unexpected error: %s", e)
  finally:
    subscriber.close()
    bridge.shutdown()
    notifier.send_message(f"🛑 <b>MT5 Worker disconnected</b>{footer}")
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
