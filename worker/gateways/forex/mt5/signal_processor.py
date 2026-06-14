"""
worker/gateways/forex/mt5/signal_processor.py
─────────────────────────────────────────────
MT5/FOREX-specific signal processor.

Implements the broker-specific hooks of
:class:`~worker.gateways.processor.BaseSignalProcessor`: the MT5 bridge,
executor, reconnect handling, and the MT5 background jobs (health thread,
terminal-close detection). Everything market-agnostic — the NATS loop, signal
persistence, notifications, position CDC — is inherited from the base.

All heavy MetaTrader5 imports are at module level, so this module must only be
imported from the child process (lazy-imported inside
:func:`worker.forex_worker.forex_worker_main`) — never by the parent FastAPI process.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

from worker.context import WorkerContext
from worker.gateways.forex.mt5.bridge import MT5
from worker.gateways.forex.mt5.executor import MT5Executor
from worker.gateways.forex.mt5.message_presenter import TradeMessagePresenter
from worker.gateways.processor import BaseSignalProcessor
from worker.jobs.mt5_event_job import MT5EventJob
from worker.logger import get_logger
from worker.services.notification_service import _box
from worker.settings import MT5_HEALTH_INTERVAL

log = get_logger("worker.gateways.forex.mt5.signal_processor")


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


class Mt5SignalProcessor(BaseSignalProcessor):
  """MT5 broker bridge + executor + MT5 jobs over the shared signal skeleton."""

  name = "MT5"
  presenter = TradeMessagePresenter

  def __init__(self, ctx: WorkerContext, settings_dict: dict) -> None:
    super().__init__(ctx, settings_dict)
    self._footer = ""

  # ── Broker hooks ──────────────────────────────────────────────────────── #

  def _build_executor(self) -> MT5Executor:
    self.bridge = MT5(
      server=self.settings["mt5_server"],
      login=self.settings["mt5_login"],
      password=self.settings["mt5_password"],
      path=self.settings.get("mt5_path"),
    )
    return MT5Executor(
      slippage_deviation=self.settings["slippage_deviation"],
      config=self.config,
      db=self.ctx.db_service,
      strategy_magic_map=self.settings.get("strategy_magic_map") or {},
    )

  def _connect_broker(self) -> bool:
    return self.bridge.reconnect(max_attempts=0, delay_seconds=10.0)

  def _disconnect_broker(self) -> None:
    self.bridge.shutdown()

  def _account_footer(self) -> str:
    return self.bridge.get_account_footer()

  @property
  def _account_id(self) -> str:
    return str(self.settings["mt5_login"])

  def _magic_for(self, strategy: str) -> Optional[int]:
    return self.executor._magic_for(strategy)

  def _position_cdc_kwargs(self) -> Dict[str, Any]:
    return {
      "account_info_fn": self.bridge.get_account_status,
      "account_name": self.settings.get("mt5_name"),
      "strategy_magic_map": self.settings.get("strategy_magic_map") or {},
    }

  def _ensure_connected(self) -> bool:
    return _ensure_mt5_connected(self.bridge, self.ctx.notifier, self._footer, log)

  def _start_broker_jobs(self, stop_event) -> None:
    threading.Thread(
      target=_mt5_health_thread,
      args=(self.bridge, self.ctx.notifier, self.bridge.get_account_footer, stop_event, log),
      name="mt5-health",
      daemon=True,
    ).start()

    MT5EventJob(
      magic_numbers=self.executor.owned_magics(),
      db_service=self.ctx.db_service,
      notifier=self.ctx.channel_notifier,
    ).start(stop_event=stop_event)

  # ── ADMIN FLAT match keys (MT5 reconciles against live tickets) ───────── #

  def _flat_match_key(self, pos: Any) -> Any:
    # The live MT5 ticket is an int, but ref_id/ref_source_id are stored as str
    # throughout the app layer — normalise so the two sides actually compare equal.
    return str(pos.ticket)

  def _flat_db_match_keys(self, db_pos: dict) -> set:
    # Check both ref_id and ref_source_id so re-ticketed positions (after a
    # partial close) still match the live ticket. Already str, but coerce
    # defensively and drop None so the set never carries a non-matching key.
    return {
      str(k)
      for k in (db_pos.get("ref_id"), db_pos.get("ref_source_id"))
      if k is not None
    }
