"""
worker/mt5/manager.py
─────────────────────
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
import time
from typing import Optional

from worker.schemas.position_schema import PositionStatusEnum
from worker.services.notification_service import _box
from worker.settings import MT5_HEALTH_INTERVAL


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
# MT5 Manager used from the FastAPI lifespan
# ---------------------------------------------------------------------------
class MT5Manager:
  """
  Manages a child process that runs a worker function.
  The parent (FastAPI) process delegates all blocking work to the child so its
  event loop is completely free from GIL interference.
  """

  def __init__(
    self, settings_dict: dict, worker_fn, process_name: str = "worker"
  ) -> None:
    self._settings_dict = settings_dict
    self._worker_fn = worker_fn
    self._process_name = process_name
    self._process: Optional[multiprocessing.Process] = None
    self._stop_event = multiprocessing.Event()
    self._stopping = False

  def _spawn(self) -> multiprocessing.Process:
    return multiprocessing.Process(
      target=self._worker_fn,
      args=(self._settings_dict, self._stop_event),
      name=self._process_name,
      daemon=True,
    )

  def start(self) -> None:
    """Spawn the child process."""
    self._stop_event.clear()
    self._process = self._spawn()
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
    self._process = self._spawn()
    self._process.start()

  @property
  def is_alive(self) -> bool:
    return self._process is not None and self._process.is_alive()

  @property
  def stopping(self) -> bool:
    return self._stopping
