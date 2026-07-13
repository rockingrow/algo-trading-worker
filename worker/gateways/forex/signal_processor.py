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
from typing import Any, Dict, Optional

from worker.context import WorkerContext
from worker.gateways.forex.executor import ForexExecutor
from worker.gateways.forex.factory import PlatformFactory
from worker.gateways.forex.message_presenter import ForexMessagePresenter
from worker.gateways.processor import BaseSignalProcessor
from worker.icons import CONNECTED, DISCONNECTED, WARNING
from worker.logger import get_logger
from worker.services.notification_service import _box
from worker.settings import (
  MT5_HEALTH_INTERVAL,
  MT5_HEALTH_INTERVAL_WEEKEND,
  is_market_weekend,
)

log = get_logger("worker.gateways.forex.signal_processor")


# ── Child-process helpers ────────────────────────────────────────────────── #


def _health_thread(gateway, notifier, footer_fn, stop_event, log) -> None:
  """Runs in a daemon thread inside the child process.

  Proactively detects a platform disconnect and relaunches/reconnects the
  terminal without waiting for a signal to arrive.

  On weekends the FOREX trade server is offline for the broker's weekly
  maintenance, so a disconnect is expected. The loop then backs off to
  ``MT5_HEALTH_INTERVAL_WEEKEND`` and skips the weekday relaunch/reconnect storm
  — it just checks quietly (and sends a single notice) instead of flooding the
  logs and Telegram with attempts that cannot succeed until the market reopens.
  """
  name = gateway.name
  weekend_notice_sent = False
  while not stop_event.is_set():
    weekend = is_market_weekend()
    interval = MT5_HEALTH_INTERVAL_WEEKEND if weekend else MT5_HEALTH_INTERVAL
    # Interruptible wait so shutdown isn't delayed by the (long) weekend interval.
    if stop_event.wait(interval):
      break
    try:
      if gateway.is_connected():
        weekend_notice_sent = False
        continue

      if weekend:
        # Market closed for the weekend — the trade server is down for
        # maintenance. Reconnecting cannot succeed until it reopens, so stay
        # quiet: log once at INFO and notify only on the first detection.
        log.info(
          "[%s Health] %s disconnected during weekend market close — "
          "backing off, next check in %ds.",
          name, name, interval,
        )
        if not weekend_notice_sent:
          notifier.send_message(
            _box(
              f"{WARNING} <b>[Market Closed] {name} disconnected (weekend)</b>\n\n"
              f"Broker trade server is offline for weekend maintenance. "
              f"Reconnect attempts are paused until the market reopens."
              f"{footer_fn()}"
            )
          )
          weekend_notice_sent = True
        # A single lightweight attempt re-establishes the session promptly once
        # the server returns, without the weekday retry storm.
        gateway.reconnect(max_attempts=1, delay_seconds=0)
        continue

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
        log.warning(
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
  _gateway_setting_key = "forex_platform"

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
