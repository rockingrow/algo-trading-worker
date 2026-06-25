"""
worker/gateways/forex/signal_processor.py
─────────────────────────────────────────
FOREX signal processor — platform-agnostic over a
:class:`~worker.gateways.forex.base.BasePlatformGateway`.

Implements the broker-specific hooks of
:class:`~worker.gateways.processor.BaseSignalProcessor`: the platform gateway
(built by :class:`~worker.gateways.forex.factory.PlatformFactory`), the forex
executor, reconnect handling, and the platform background jobs (health thread,
terminal-close detection). Everything market-agnostic — the NATS loop, signal
persistence, notifications, position CDC — is inherited from the base.

Nothing here imports MetaTrader5 directly; the concrete platform (and its heavy
native stack) is loaded only by the factory inside the child process
(lazy-imported via :func:`worker.market.forex_worker_main`).
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

from worker.context import WorkerContext
from worker.gateways.forex.executor import ForexExecutor
from worker.gateways.forex.factory import PlatformFactory
from worker.gateways.forex.message_presenter import ForexMessagePresenter
from worker.gateways.processor import BaseSignalProcessor
from worker.icons import CONNECTED, DISCONNECTED, WARNING
from worker.logger import get_logger
from worker.services.notification_service import _box
from worker.settings import MT5_HEALTH_INTERVAL

log = get_logger("worker.gateways.forex.signal_processor")


# ── Child-process helpers ────────────────────────────────────────────────── #


def _health_thread(gateway, notifier, footer_fn, stop_event, log) -> None:
  """Runs in a daemon thread inside the child process.

  Proactively detects a platform disconnect and relaunches/reconnects the
  terminal without waiting for a signal to arrive.
  """
  name = gateway.name
  while not stop_event.is_set():
    time.sleep(MT5_HEALTH_INTERVAL)
    if stop_event.is_set():
      break
    try:
      if not gateway.is_connected():
        log.warning(
          "[%s Health] %s disconnected — attempting to relaunch/reconnect...", name, name
        )
        notifier.send_message(
          _box(f"{WARNING} <b>[Disconnected] {name} — reconnecting…</b>{footer_fn()}")
        )
        reconnected = gateway.reconnect(max_attempts=15, delay_seconds=10.0)
        if reconnected:
          log.info("[%s Health] %s reconnected successfully.", name, name)
          notifier.send_message(_box(f"{CONNECTED} <b>[Connected] {name}</b>{footer_fn()}"))
        else:
          log.error(
            "[%s Health] %s reconnect failed after 15 attempts — killing and restarting terminal...",
            name, name,
          )
          notifier.send_message(
            _box(
              f"{DISCONNECTED} <b>[Disconnected] {name} reconnect failed</b>\n\n"
              f"Killing and restarting terminal…{footer_fn()}"
            )
          )
          restarted = gateway.restart_terminal(startup_wait=15.0)
          if restarted:
            log.info("[%s Health] terminal restarted — retrying reconnect...", name)
            reconnected = gateway.reconnect(max_attempts=15, delay_seconds=10.0)
            if reconnected:
              log.info("[%s Health] %s reconnected after terminal restart.", name, name)
              notifier.send_message(
                _box(f"{CONNECTED} <b>[Connected] {name} after terminal restart</b>{footer_fn()}")
              )
            else:
              log.error(
                "[%s Health] %s still unreachable after terminal restart — manual intervention required.",
                name, name,
              )
              notifier.send_message(
                _box(
                  f"{DISCONNECTED} <b>{name} CRASHED</b>\n\n"
                  f"Failed to reconnect even after restarting the terminal.\n"
                  f"Please restart the terminal manually.{footer_fn()}"
                )
              )
          else:
            log.error(
              "[%s Health] terminal restart failed (path not configured or exe missing) — manual intervention required.",
              name,
            )
            notifier.send_message(
              _box(
                f"{DISCONNECTED} <b>{name} CRASHED</b>\n\n"
                f"terminal restart failed — path not configured or exe missing.\n"
                f"Please restart the terminal manually.{footer_fn()}"
              )
            )
    except Exception as exc:
      log.exception("[%s Health] Unexpected error in health thread: %s", name, exc)


def _ensure_gateway_connected(gateway, notifier, footer: str, log) -> bool:
  """Return True if the platform is (or becomes) connected; False if reconnect fails."""
  name = gateway.name
  if gateway.is_connected():
    return True
  log.warning("[%s Process] %s connection lost. Reconnecting...", name, name)
  notifier.send_message(_box(f"{WARNING} <b>[Disconnected] {name} — reconnecting…</b>{footer}"))
  reconnected = gateway.reconnect(max_attempts=0, delay_seconds=10.0)
  if reconnected:
    notifier.send_message(_box(f"{CONNECTED} <b>[Connected] {name}</b>{footer}"))
  else:
    notifier.send_message(
      _box(f"{DISCONNECTED} <b>[Disconnected] {name} reconnect failed — signal dropped</b>{footer}")
    )
  return reconnected


# ── Processor ────────────────────────────────────────────────────────────── #


class ForexSignalProcessor(BaseSignalProcessor):
  """Platform gateway + forex executor + platform jobs over the shared skeleton."""

  name = "FOREX"
  presenter = ForexMessagePresenter

  def __init__(self, ctx: WorkerContext, settings_dict: dict) -> None:
    super().__init__(ctx, settings_dict)
    self._footer = ""

  # ── Broker hooks ──────────────────────────────────────────────────────── #

  def _build_executor(self) -> ForexExecutor:
    self.gateway = PlatformFactory.create(self.settings)
    return ForexExecutor(
      gateway=self.gateway,
      config=self.config,
      db=self.ctx.db_service,
      strategy_magic_map=self.settings.get("strategy_magic_map") or {},
    )

  def _connect_broker(self) -> bool:
    return self.gateway.reconnect(max_attempts=0, delay_seconds=10.0)

  def _disconnect_broker(self) -> None:
    self.gateway.close()

  def _account_footer(self) -> str:
    return self.gateway.get_account_footer()

  @property
  def _account_id(self) -> str:
    return str(self.settings["mt5_login"])

  def _magic_for(self, strategy: str) -> Optional[int]:
    return self.executor._magic_for(strategy)

  def _position_cdc_kwargs(self) -> Dict[str, Any]:
    return {
      "account_info_fn": self.gateway.get_account,
      "account_name": self.settings.get("mt5_name"),
      "strategy_magic_map": self.settings.get("strategy_magic_map") or {},
    }

  def _ensure_connected(self) -> bool:
    return _ensure_gateway_connected(self.gateway, self.ctx.notifier, self._footer, log)

  def _start_broker_jobs(self, stop_event) -> None:
    threading.Thread(
      target=_health_thread,
      args=(self.gateway, self.ctx.notifier, self.gateway.get_account_footer, stop_event, log),
      name="forex-health",
      daemon=True,
    ).start()

    job = self.gateway.create_close_detection_job(
      magic_numbers=self.executor.owned_magics(),
      db_service=self.ctx.db_service,
      notifier=self.ctx.channel_notifier,
    )
    if job is not None:
      job.start(stop_event=stop_event)

  # ── ADMIN FLAT match keys (forex reconciles against live tickets) ──────── #

  def _flat_match_key(self, pos: Any) -> Any:
    # The live ticket is an int, but ref_id/ref_source_id are stored as str
    # throughout the app layer — normalise so the two sides compare equal.
    return str(pos.ticket)

  def _flat_db_match_keys(self, db_pos: dict) -> set:
    # Check both ref_id and ref_source_id so re-ticketed positions (after a
    # partial close) still match the live ticket.
    return {
      str(k)
      for k in (db_pos.get("ref_id"), db_pos.get("ref_source_id"))
      if k is not None
    }
