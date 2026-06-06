"""
worker/gateways/mt5/signal_processor.py
───────────────────────────────────────
MT5/FOREX-specific signal processor.

Implements the broker-specific hooks of
:class:`~worker.core.base_signal_processor.BaseSignalProcessor`: the MT5 bridge,
executor, reconnect handling, and the MT5 background jobs (health thread,
terminal-close detection). Everything market-agnostic — the NATS loop, signal
persistence, notifications, position CDC — is inherited from the base.

All heavy MetaTrader5 imports are at module level, so this module must only be
imported from the child process (lazy-imported inside
:func:`worker.mt5_worker.mt5_worker_main`) — never by the parent FastAPI process.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional

from pydantic import ValidationError

from worker.context import WorkerContext
from worker.core.base_signal_processor import BaseSignalProcessor
from worker.gateways.mt5.bridge import MT5
from worker.gateways.mt5.executor import MT5Executor
from worker.gateways.mt5.message_presenter import TradeMessagePresenter
from worker.jobs.mt5_event_job import MT5EventJob
from worker.logger import get_logger
from worker.schemas.admin_schema import AdminActionEnum, AdminSignalSchema
from worker.schemas.position_schema import PositionStatusEnum
from worker.services.notification_service import _box
from worker.settings import MT5_HEALTH_INTERVAL

log = get_logger("worker.gateways.mt5.signal_processor")


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

  # ── ADMIN FLAT (MT5 reconciles against live tickets) ──────────────────── #

  def _handle_admin_message(self, raw: str) -> None:
    try:
      admin = AdminSignalSchema(**json.loads(raw))
    except (json.JSONDecodeError, ValidationError) as err:
      log.error("[ADMIN] Parse error: %s", err)
      return

    if admin.action != AdminActionEnum.FLAT:
      log.warning("[ADMIN] Unknown action: %s", admin.action)
      return

    worker_account_id = str(self.settings["mt5_login"])
    if admin.account_id and admin.account_id != worker_account_id:
      log.info(
        "[ADMIN FLAT] Skipping: account_id=%s does not match worker account=%s",
        admin.account_id,
        worker_account_id,
      )
      return

    if not _ensure_mt5_connected(self.bridge, self.ctx.notifier, self._footer, log):
      return

    # Step 1: Query MT5 by parameters and close positions first.
    # Do NOT gate on DB state — MT5 is the source of truth for live positions.
    if admin.symbol:
      mt5_positions = self.executor.get_open_positions(admin.symbol, strategy=admin.strategy)
    else:
      mt5_positions = self.executor.get_all_open_positions(strategy=admin.strategy)

    if not mt5_positions:
      log.warning(
        "[ADMIN FLAT] No open MT5 positions found (strategy=%s, symbol=%s)",
        admin.strategy, admin.symbol,
      )
    else:
      log.info(
        "[ADMIN FLAT] Closing %d MT5 position(s) (strategy=%s, symbol=%s)",
        len(mt5_positions), admin.strategy, admin.symbol,
      )

    attempted_tickets: set[int] = set()
    closed_tickets: set[int] = set()
    close_results: dict[int, dict] = {}

    for pos in mt5_positions:
      attempted_tickets.add(pos.ticket)
      result = self.executor.close_single_position(pos, reason="FLAT")
      if result.get("success"):
        closed_tickets.add(pos.ticket)
        close_results[pos.ticket] = result
        log.info(
          "[ADMIN FLAT] Closed MT5 ticket=%s price=%s vol=%s",
          pos.ticket, result.get("price"), result.get("volume"),
        )
      else:
        log.error(
          "[ADMIN FLAT] Failed to close ticket=%s: %s",
          pos.ticket, result.get("comment"),
        )

    # Step 2: Reconcile DB — find open records matching closed MT5 tickets and update.
    # Both ticket and source_ticket are checked so re-ticketed positions (after TP1)
    # are still matched correctly.
    # Positions in attempted_tickets but not closed_tickets had a failed close order —
    # they are still open in MT5, so the DB record is left untouched.
    db_positions = self.ctx.db_service.get_open_positions_for_flat(
      strategy=admin.strategy,
      symbol=admin.symbol,
    )

    for db_pos in db_positions:
      db_ticket = db_pos.get("ticket")
      db_source_ticket = db_pos.get("source_ticket")
      matched_ticket = (
        db_ticket if db_ticket in closed_tickets
        else db_source_ticket if db_source_ticket in closed_tickets
        else None
      )
      if matched_ticket is not None:
        result = close_results[matched_ticket]
        self.ctx.db_service.update_position_status(
          source_ticket=db_source_ticket,
          status=PositionStatusEnum.FLATTED,
          new_ticket=result.get("ticket"),
          closed_price=result.get("price"),
          mt5_retcode=result.get("retcode"),
          comment=result.get("comment", ""),
          message=raw,
        )
        self.ctx.channel_notifier.send_message(
          TradeMessagePresenter.admin_flat_closed(
            db_pos, result, self.bridge.get_account_footer()
          )
        )
      elif db_ticket not in attempted_tickets and db_source_ticket not in attempted_tickets:
        # DB has an open record but the position was never seen in MT5 —
        # it was already closed externally; sync the DB.
        log.warning(
          "[ADMIN FLAT] ticket=%s in DB but not found in MT5 — marking FLATTED",
          db_ticket,
        )
        self.ctx.db_service.update_position_status(
          source_ticket=db_source_ticket,
          status=PositionStatusEnum.FLATTED,
          comment="Admin FLAT (position already closed in MT5)",
          message=raw,
        )
