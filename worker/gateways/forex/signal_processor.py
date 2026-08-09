"""
worker/gateways/forex/signal_processor.py
─────────────────────────────────────────
FOREX signal processor — platform-agnostic over a
:class:`~worker.gateways.forex.base.BasePlatformGateway`.

Implements the broker-specific hooks of
:class:`~worker.gateways.processor.BaseSignalProcessor`: the platform gateway
(built by :class:`~worker.gateways.forex.factory.PlatformFactory`), the forex
executor, reconnect handling, and the platform background jobs (health thread,
terminal-close detection, and the
:class:`~worker.gateways.forex.reconcile_job.ForexReconcileJob` broker↔DB
backstop). Everything market-agnostic — the NATS loop, signal persistence,
notifications, position CDC — is inherited from the base.

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
from worker.gateways.forex.reconcile_job import ForexReconcileJob
from worker.gateways.processor import BaseSignalProcessor
from worker.icons import CONNECTED, DISCONNECTED, WARNING
from worker.logger import get_logger
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.services.notification_service import _box
from worker.settings import (
  MT5_HEALTH_INTERVAL,
  MT5_HEALTH_INTERVAL_WEEKEND,
  is_market_closed,
)

log = get_logger("worker.gateways.forex.signal_processor")

# MT5 ``account_info().margin_mode`` value for ACCOUNT_MARGIN_MODE_RETAIL_HEDGING
# — the only mode in which one symbol can hold several independent positions
# (0 = RETAIL_NETTING, 1 = EXCHANGE). Hard-coded rather than read off the mt5
# module so this file stays importable without the native MetaTrader5 stack.
MT5_MARGIN_MODE_HEDGING = 2


# ── Child-process helpers ────────────────────────────────────────────────── #


def _health_thread(gateway, notifier, footer_fn, stop_event, log) -> None:
  """Runs in a daemon thread inside the child process.

  Proactively detects a platform disconnect and relaunches/reconnects the
  terminal without waiting for a signal to arrive.

  Over the weekend-closed window (Fri 22:00 → Sun 22:00 UTC) the FOREX trade
  server is offline for the broker's weekly maintenance, so a connection is
  impossible and reconnecting only floods the logs. The loop then **closes** the
  MT5 connection once, backs off to ``MT5_HEALTH_INTERVAL_WEEKEND``, and stays
  idle until the market reopens — at which point (the first check after the
  window) it reconnects through the normal path.
  """
  name = gateway.name
  closed_for_weekend = False
  while not stop_event.is_set():
    market_closed = is_market_closed()
    interval = MT5_HEALTH_INTERVAL_WEEKEND if market_closed else MT5_HEALTH_INTERVAL
    # Interruptible wait so shutdown isn't delayed by the (long) weekend interval.
    if stop_event.wait(interval):
      break
    try:
      if market_closed:
        # Market closed for the weekend. The broker trade server is offline for
        # maintenance, so keeping (or retrying) a connection is pointless. Close
        # it once and stay idle at the slow weekend cadence until the market
        # reopens; the paired MT5EventJob likewise pauses its polling.
        if not closed_for_weekend:
          _close_for_weekend(gateway, notifier, footer_fn, name, log)
          closed_for_weekend = True
        continue

      # Market is open. If the connection was closed for the weekend, the market
      # has now reopened — fall through to the normal reconnect path to bring it
      # back up.
      if closed_for_weekend:
        closed_for_weekend = False
        log.info("[%s Health] Market reopened — reconnecting %s...", name, name)

      if gateway.is_connected():
        continue

      _reconnect_or_restart(gateway, notifier, footer_fn, name, log)
    except Exception as exc:
      log.exception("[%s Health] Unexpected error in health thread: %s", name, exc)


def _close_for_weekend(gateway, notifier, footer_fn, name, log) -> None:
  """Close the platform connection for the weekend and notify once."""
  log.info(
    "[%s Health] Weekend market close — closing %s connection until the market reopens.",
    name,
    name,
  )
  notifier.send_message(
    _box(
      f"{DISCONNECTED} <b>[Market Closed] {name}</b>\n\n"
      f"Broker trade server is offline for the weekend. Connection closed — "
      f"{name} will reconnect automatically when the market reopens."
      f"{footer_fn()}"
    )
  )
  try:
    gateway.close()
  except Exception:
    log.exception("[%s Health] Error closing %s for the weekend.", name, name)


def _reconnect_or_restart(gateway, notifier, footer_fn, name, log) -> None:
  """Attempt a plain reconnect; escalate to a terminal restart on failure."""
  log.warning(
    "[%s Health] %s disconnected — attempting to relaunch/reconnect...", name, name
  )
  notifier.send_message(
    _box(f"{WARNING} <b>[Disconnected] {name} — reconnecting…</b>{footer_fn()}")
  )
  if gateway.reconnect(max_attempts=15, delay_seconds=10.0):
    log.info("[%s Health] %s reconnected successfully.", name, name)
    notifier.send_message(_box(f"{CONNECTED} <b>[Connected] {name}</b>{footer_fn()}"))
    return

  log.warning(
    "[%s Health] %s reconnect failed after 15 attempts — killing and restarting terminal...",
    name,
    name,
  )
  notifier.send_message(
    _box(
      f"{DISCONNECTED} <b>[Disconnected] {name} reconnect failed</b>\n\n"
      f"Killing and restarting terminal…{footer_fn()}"
    )
  )
  _restart_terminal_and_reconnect(gateway, notifier, footer_fn, name, log)


def _restart_terminal_and_reconnect(gateway, notifier, footer_fn, name, log) -> None:
  """Kill/relaunch the terminal and retry the reconnect once more."""
  if not gateway.restart_terminal(startup_wait=15.0):
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
    return

  log.info("[%s Health] terminal restarted — retrying reconnect...", name)
  if gateway.reconnect(max_attempts=15, delay_seconds=10.0):
    log.info("[%s Health] %s reconnected after terminal restart.", name, name)
    notifier.send_message(
      _box(f"{CONNECTED} <b>[Connected] {name} after terminal restart</b>{footer_fn()}")
    )
    return

  log.error(
    "[%s Health] %s still unreachable after terminal restart — manual intervention required.",
    name,
    name,
  )
  notifier.send_message(
    _box(
      f"{DISCONNECTED} <b>{name} CRASHED</b>\n\n"
      f"Failed to reconnect even after restarting the terminal.\n"
      f"Please restart the terminal manually.{footer_fn()}"
    )
  )


def _ensure_gateway_connected(gateway, notifier, footer: str, log) -> bool:
  """Return True if the platform is (or becomes) connected; False if reconnect fails."""
  name = gateway.name
  if gateway.is_connected():
    return True
  log.warning("[%s Process] %s connection lost. Reconnecting...", name, name)
  notifier.send_message(
    _box(f"{WARNING} <b>[Disconnected] {name} — reconnecting…</b>{footer}")
  )
  reconnected = gateway.reconnect(max_attempts=0, delay_seconds=10.0)
  if reconnected:
    notifier.send_message(_box(f"{CONNECTED} <b>[Connected] {name}</b>{footer}"))
  else:
    notifier.send_message(
      _box(
        f"{DISCONNECTED} <b>[Disconnected] {name} reconnect failed — signal dropped</b>{footer}"
      )
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
    connected = self.gateway.reconnect(max_attempts=0, delay_seconds=10.0)
    if connected:
      self._warn_if_multi_strategy_needs_hedging()
    return connected

  def _warn_if_multi_strategy_needs_hedging(self) -> None:
    """Escalate when FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL is on but the account
    cannot actually hold parallel positions.

    The toggle relies on the platform keeping one position *per magic*. That is
    only true on a **hedging** account: a netting account merges every order on
    a symbol into a single position, so a second strategy's entry silently grows
    the first strategy's position instead of opening its own. The second
    strategy then owns nothing — each of its TP1/TP2/SL signals fails with "No
    open position" while the merged volume runs on unmanaged. Nothing else in
    the pipeline can detect that, so it is checked once, loudly, at connect.

    Warn-only by design: the operator may be mid-migration between accounts, and
    refusing to trade at all is worse than trading with the toggle's guarantee
    reduced to the pre-existing one-strategy-per-symbol behaviour.
    """
    if not self.settings.get("forex_allow_multi_strategy_per_symbol", False):
      return
    account = self.gateway.get_account() or {}
    margin_mode = account.get("margin_mode")
    # Absent → the platform does not report a margin mode (non-MT5 gateway or a
    # stubbed account): nothing to assert, stay quiet rather than cry wolf.
    if margin_mode is None or margin_mode == MT5_MARGIN_MODE_HEDGING:
      return
    log.error(
      "[%s Process] FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL is ENABLED but the "
      "account margin_mode=%s is not hedging (%s). Positions from different "
      "strategies on the same symbol will be MERGED by the broker and only the "
      "first strategy will own the resulting position.",
      self.name,
      margin_mode,
      MT5_MARGIN_MODE_HEDGING,
    )
    self.ctx.notifier.send_message(
      _box(
        f"{WARNING} <b>[Config] Multi-strategy per symbol needs a hedging account</b>\n\n"
        f"<b>FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL</b> is enabled, but this "
        f"account is not in hedging mode (margin_mode=<b>{margin_mode}</b>).\n"
        f"Two strategies entering the same symbol will be merged into one "
        f"broker position — the second strategy will not own a position and its "
        f"exits will fail.\n"
        f"Either switch to a hedging account or set the flag back to false."
        f"{self.gateway.get_account_footer()}"
      )
    )

  def _disconnect_broker(self) -> None:
    self.gateway.close()

  def _account_footer(self) -> str:
    return self.gateway.get_account_footer()

  @property
  def _account_id(self) -> str:
    return str(self.settings["mt5_login"])

  def _magic_for(self, strategy: str) -> Optional[int]:
    return self.executor._magic_for(strategy)

  def _set_executor_magic_map(self, mapping: Dict[str, int]) -> None:
    """Refresh the executor's magic map when the broker pushes a
    ``strategy_magic_map`` (see ``BaseSignalProcessor._apply_strategy_magic_map``),
    so magic resolution and ``owned_magics()`` reflect it. The push arrives in the
    WORKER_CONNECTED_ACK on connect, before ``start_market_jobs`` builds the
    close-detection job from ``owned_magics()``."""
    self.executor.set_strategy_magic_map(mapping)

  def _replay_blocked_reason(self) -> Optional[str]:
    """Block the ACK's signal replay until this worker actually holds a magic map.

    Every FOREX order is routed by its strategy's magic, so with an empty map
    ``_magic_for`` raises and each replayed signal is rejected as an unknown
    strategy. The map is broker-owned now (``STRATEGY_MAGIC_MAP`` ships empty),
    so an ACK that carries a replay but no usable ``strategy_magic_map`` — the
    section omitted, or dropped by the salvage pass — is a broker-side config
    fault, not a per-signal one. Report it once instead of rejecting the batch
    signal-by-signal.
    """
    if self.settings.get("strategy_magic_map"):
      return None
    return (
      "this worker has no strategy_magic_map (the ACK pushed none, or it was "
      "dropped) — FOREX orders cannot be routed without a magic per strategy"
    )

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
      args=(
        self.gateway,
        self.ctx.notifier,
        self.gateway.get_account_footer,
        stop_event,
        log,
      ),
      name="forex-health",
      daemon=True,
    ).start()

    job = self.gateway.create_close_detection_job(
      # The bound method, not its result: the magic map is broker-owned and
      # re-pushed on every WORKER_CONNECTED_ACK (a NATS reconnect re-runs the
      # handshake), so a set snapshotted here would go stale and the job would
      # stop recognising this worker's own tickets.
      magic_numbers=self.executor.owned_magics,
      db_service=self.ctx.db_service,
      notifier=self.ctx.channel_notifier,
    )
    if job is not None:
      job.start(stop_event=stop_event)

    # Durability backstop: the close-detection job above only reports closes the
    # *terminal* initiated — a position closed by our own order_send (e.g. a TP1
    # partial whose volume equalled the whole position) is deliberately skipped
    # there, and a close during downtime is never seen at all. This periodic
    # reconciler marks any DB-open row that no longer matches a live platform
    # position as closed, so a missed close self-heals instead of leaving the row
    # stuck OPENED/TP1 and blocking later signals.
    ForexReconcileJob(
      db_service=self.ctx.db_service,
      executor=self.executor,
      handler=self._on_missed_close,
      gateway=self.gateway,
      magic_for=self._magic_for,
    ).start(stop_event=stop_event)

  # ── Reconciler backstop (from ForexReconcileJob) ──────────────────────── #

  def _on_missed_close(self, row: Dict[str, Any]) -> None:
    """Mark a DB-open position closed after the reconciler confirmed it no longer
    exists on the platform (a close the terminal-event job never reported).

    The exact reason/price is unknown — the deal-history event is what carries
    those — so the close is recorded as ``TERMINAL_CLOSED`` (the same status
    ``MT5EventJob`` uses for a platform-side close) with a best-effort mid price
    and a comment flagging it as reconciled. The CDC job then propagates the
    status downstream as usual.
    """
    close_price = self._best_effort_price(row.get("symbol"))
    comment = "Reconciled: closed on broker (close event missed)"

    self.ctx.db_service.log_position(
      strategy=row.get("strategy"),
      ref_id=None,
      ref_source_id=row.get("ref_source_id"),
      symbol=row["symbol"],
      action=PositionStatusEnum.TERMINAL_CLOSED.value,
      volume=row.get("volume"),
      price=close_price,
      sl=None,
      tp1=None,
      gateway_return_code=0,
      comment=comment,
      author=LogAuthorEnum.TERMINAL.value,
      market_type=self._market_type,
    )
    self.ctx.db_service.update_position_status(
      ref_source_id=row.get("ref_source_id"),
      status=PositionStatusEnum.TERMINAL_CLOSED,
      closed_price=close_price,
      gateway_return_code=0,
      comment=comment,
    )
    self.ctx.channel_notifier.send_message(
      ForexMessagePresenter.position_reconciled_closed(
        row, close_price, self._account_footer()
      )
    )

  def _best_effort_price(self, symbol: Optional[str]) -> Optional[float]:
    """Current mid price as an approximate close price; ``None`` if unreadable."""
    try:
      tick = self.gateway.get_tick(self.executor.get_symbol(symbol))
    except Exception as exc:  # pragma: no cover - best effort
      log.warning("[FOREX Reconcile] tick unavailable for %s: %s", symbol, exc)
      return None
    if tick is None:
      return None
    return (tick.bid + tick.ask) / 2

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
