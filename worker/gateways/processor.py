"""
worker/gateways/processor.py
────────────────────────────
Market-agnostic signal-processor skeleton (Template Method pattern).

Both the FOREX (MT5) and CRYPTO (CEX) gateways run the *same* algorithm:

    connect → subscribe to NATS → for each message:
        ADMIN  → validate + reconcile a FLAT (public fanout ``ADMIN``, or the
                 worker's private ``ADMIN.<market>.<gateway>.<account_id>``)
        SIGNAL → handle, persist the position, notify

Only a handful of steps actually differ per broker (how to connect, how to read
the account footer, which jobs to start, how a FLAT reconciles against the
broker's live positions, …). :class:`BaseSignalProcessor` implements the
invariant skeleton once and declares those variation points as ``@abstractmethod``
hooks, so every concrete ``<Market>SignalProcessor`` *must* define them and can
share everything else.

The base imports no broker SDK (no MetaTrader5, no exchange client), so it is
safe to import on any market's path.
"""

from __future__ import annotations

import json
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import ValidationError

from worker.context import WorkerContext
from worker.gateways import guard
from worker.gateways.config import ExecutionConfig
from worker.gateways.market_strategy import MarketStrategyFactory
from worker.gateways.signal_handler import SignalHandler
from worker.interfaces.trade_presenter_protocol import TradePresenterProtocol
from worker.logger import get_logger
from worker.schemas.admin_schema import (
  AdminActionEnum,
  AdminFlatSchema,
  AdminMessageSchema,
  PrivateAdminFlatSchema,
  PrivateAdminSignalControlSchema,
)
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalActionEnum, SignalSchema
from worker.schemas.system_schema import (
  SystemActionEnum,
  SystemRetrySignalsSchema,
  SystemSchema,
  SystemStrategyMagicMapSchema,
  SystemWorkerConnectedErrorSchema,
  SystemWorkerConnectedSchema,
)
from worker.services.nats_service import NATSPublisher, NATSSubscriber
from worker.settings import (
  MAX_RETRY_TIMEOUT,
  NATS_REQUIRED_LISTENING_SUBJECTS,
  MarketTypeEnum,
)

log = get_logger("worker.gateways.processor")

# How long an account footer is reused before refetching. Signals can arrive in
# bursts; fetching the live account (a REST round-trip for a CEX) once per signal
# adds avoidable latency/rate-limit pressure, and a few-seconds-stale balance in a
# notification is harmless.
_FOOTER_TTL = 30  # seconds

# How long to wait for the broker's WORKER_CONNECTED reply before retrying.
_HANDSHAKE_TIMEOUT = 5  # seconds
# Backoff between handshake retries (index by attempt, capped at the last value).
# The handshake is idempotent on the broker side and gates trading (crypto needs
# default_leverage before it's safe to trade), so it retries until it succeeds
# rather than falling back to running without config.
_HANDSHAKE_BACKOFF = (5, 10, 20)  # seconds
# Random delay added before the very first request only, to desynchronise a
# reconnect storm: a NATS/broker restart makes every connected worker reconnect
# and re-announce at roughly the same instant, so without jitter the broker
# receives N simultaneous WORKER_CONNECTED requests.
_HANDSHAKE_JITTER_MAX = 0.5  # seconds
# Consecutive timeouts after which a retry is escalated from WARNING to ERROR
# (forwarded to Telegram via TelegramLogHandler) so an operator is alerted that
# the broker looks unreachable rather than just transiently slow.
_HANDSHAKE_ALERT_THRESHOLD = 3

# Entry actions that open a fresh position — the only ones the MAX_OPEN_ORDERS
# cap applies to (exits must always be allowed so positions can be closed).
_ENTRY_ACTIONS = (SignalActionEnum.LONG, SignalActionEnum.SHORT)

# Exit action → DB status, shared by every market.
_CLOSE_STATUS_MAP: Dict[str, PositionStatusEnum] = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
  "FLAT": PositionStatusEnum.FLATTED,
}


def _seconds_since(ts: Optional[datetime], now: datetime) -> Optional[float]:
  """Seconds between *ts* and *now*, treating a naive *ts* as UTC. Returns
  None when *ts* is missing so the RETRY_SIGNALS handler can drop a payload
  with no timestamp instead of guessing an age."""
  if ts is None:
    return None
  if ts.tzinfo is None:
    ts = ts.replace(tzinfo=timezone.utc)
  return (now - ts).total_seconds()


def parse_nats_subjects(raw: str) -> list[str | NatsSubjectEnum]:
  """Parse a comma-separated subject string, always including the required ones."""
  parsed: set[str | NatsSubjectEnum] = set()
  for s in raw.split(","):
    s = s.strip()
    if not s:
      continue
    try:
      parsed.add(NatsSubjectEnum(s))
    except ValueError:
      parsed.add(s)
  return list(NATS_REQUIRED_LISTENING_SUBJECTS | parsed)


def parse_strategy_subjects(raw: str) -> list[str]:
  """Return only the strategy-name subjects from ``NATS_SUBJECTS``.

  Every entry in ``NATS_SUBJECTS`` that is not one of the control subjects
  (see :class:`NatsSubjectEnum`) is a strategy name (e.g. ``MT5_GOLD``). The
  WORKER_CONNECTED handshake ships this list to the broker so it knows which
  strategies' recent signals to include in a RETRY_SIGNALS replay for this
  worker."""
  strategies: list[str] = []
  seen: set[str] = set()
  for s in raw.split(","):
    s = s.strip()
    if not s or s in seen:
      continue
    try:
      NatsSubjectEnum(s)
      continue  # control subject, not a strategy
    except ValueError:
      strategies.append(s)
      seen.add(s)
  return strategies


class BaseSignalProcessor(ABC):
  """Template-method base for every ``<Market>SignalProcessor``.

  Subclasses bind a concrete presenter via the ``presenter`` class attribute and
  implement the abstract hooks below. The shared methods (``connect``,
  ``shutdown``, ``run``, ``start_market_jobs``, ``_process_message``,
  startup/shutdown notifications) are final in spirit and should not be
  overridden.
  """

  #: Short label used in log lines, e.g. ``"MT5"`` / ``"CRYPTO"``.
  name: str = "BASE"
  #: Concrete presenter (conforms to :class:`TradePresenterProtocol`).
  presenter: type[TradePresenterProtocol]
  #: Short-TTL account-footer cache (class-level defaults so instances that
  #: bypass ``__init__`` in tests still resolve them).
  _footer_cache: Optional[str] = None
  _footer_cache_at: float = 0.0
  #: Signal-execution gate (class-level default for the same reason). When True,
  #: incoming SIGNALs are skipped — toggled by ADMIN BLOCK_SIGNAL/ALLOW_SIGNAL.
  _signals_blocked: bool = False

  def __init__(self, ctx: WorkerContext, settings_dict: dict) -> None:
    self.ctx = ctx
    self.settings = settings_dict
    self.config = ExecutionConfig.from_dict(settings_dict)

    # Build the broker executor (hook), then the shared strategy/handler stack.
    self.executor = self._build_executor()
    self.strategy = MarketStrategyFactory.create(
      settings_dict.get("market_type", MarketTypeEnum.FOREX),
      executor=self.executor,
      config=self.config,
    )
    self.handler = SignalHandler(self.strategy, ctx.db_service)

    self.subscriber: Optional[NATSSubscriber] = None
    self.publisher: Optional[NATSPublisher] = None
    self._footer: str = ""
    self._market_type: str = self._market_type_value(settings_dict.get("market_type"))
    # When True, incoming SIGNALs are skipped (not executed) — toggled by the
    # private ADMIN actions BLOCK_SIGNAL / ALLOW_SIGNAL. In-memory only: a worker
    # restart resets it to False (unblocked). Read/written on the single NATS
    # listener thread, so no lock is needed.
    self._signals_blocked: bool = False

  # ── Shared lifecycle ──────────────────────────────────────────────────── #

  def connect(self) -> bool:
    if not self._connect_broker():
      log.error("[%s Process] Could not connect to broker. Exiting.", self.name)
      return False

    _tok = self.settings.get("nats_token")
    nats_token = _tok.get_secret_value() if _tok is not None else None

    # Listen on the shared subjects plus this worker's own private ADMIN subject
    # (ADMIN.<market>.<gateway>.<account_id>) so the broker can address a FLAT to
    # exactly this account without fanning it out to every worker.
    subjects = parse_nats_subjects(self.settings.get("nats_subjects", ""))
    private_admin = self._private_admin_subject
    if private_admin is not None and private_admin not in subjects:
      subjects.append(private_admin)

    self.subscriber = NATSSubscriber(
      url=self.settings["nats_url"],
      subjects=subjects,
      publish_subjects=[
        NatsSubjectEnum.TRADE,
        NatsSubjectEnum.SYSTEM,
      ],  # purpose just only show on Notification
      token=nats_token,
      account_id=self.settings.get("account_id"),
      account_footer_fn=self._account_footer,
      enqueue_fn=self.ctx.nats_enqueue,
    )
    self.subscriber.connect()

    self.publisher = NATSPublisher(
      url=self.settings["nats_url"],
      publish_subjects=[NatsSubjectEnum.TRADE, NatsSubjectEnum.SYSTEM],
      token=nats_token,
      account_id=self.settings.get("account_id"),
      # Re-announce on every reconnect so the broker re-pushes init config.
      on_reconnect=self._announce_worker_connected,
    )
    self.publisher.connect()

    self._footer = self._account_footer()

    # Handshake: tell the broker this worker is online so it can push any
    # per-worker init (e.g. CRYPTO_LEVERAGE_INIT). The broker decides from
    # market/gateway whether anything applies — every market announces.
    self._announce_worker_connected()
    return True

  def shutdown(self) -> None:
    if self.subscriber is not None:
      self.subscriber.close()
    if self.publisher is not None:
      self.publisher.close()
    self._disconnect_broker()

  def send_startup_notification(self) -> None:
    self.ctx.direct_notifier.send_message(
      self.presenter.startup(self.settings, self._footer)
    )

  def send_shutdown_notification(self) -> None:
    self.ctx.direct_notifier.send_message(self.presenter.shutdown(self._footer))

  def start_market_jobs(self, stop_event) -> None:
    from worker.jobs.cdc_job import PositionCDC

    # Change-data-capture → NATS TRADE is shared by every market.
    PositionCDC(
      account_id=self._account_id,
      publisher=self.publisher,
      db_service=self.ctx.db_service,
      market_type=self.settings.get("market_type"),
      gateway=self._gateway_value,
      **self._position_cdc_kwargs(),
    ).start(stop_event=stop_event)

    # Broker-specific jobs (health thread / terminal-close / user data stream).
    self._start_broker_jobs(stop_event)

  def run(self, stop_event) -> None:
    for subject, raw in self.subscriber.listen(stop_event=stop_event):
      try:
        self._process_message(subject, raw)
      except Exception:
        log.exception(
          "[%s Process] Unhandled error processing message — skipping. "
          "CRITICAL: manual reconciliation may be required. raw=%r",
          self.name,
          raw,
        )

  # ── Shared message processing ─────────────────────────────────────────── #

  def _process_message(self, subject, raw) -> None:
    if subject == NatsSubjectEnum.ADMIN:
      self._handle_admin_message(raw, private=False)
      return

    if subject == self._private_admin_subject:
      self._handle_admin_message(raw, private=True)
      return

    if subject == NatsSubjectEnum.SYSTEM:
      self._handle_system_message(raw)
      return

    try:
      signal = SignalSchema(**json.loads(raw))
    except json.JSONDecodeError as err:
      log.error("[%s Process] Malformed JSON: %s", self.name, err)
      return
    except ValidationError as err:
      log.error("[%s Process] Signal validation failed: %s", self.name, err)
      # Notify the operator: a malformed signal is otherwise invisible (it never
      # reaches the handler, so no order_failed notification fires). Send only a
      # field-level summary — never the raw payload or pydantic's input dump,
      # which echo the broker token and other secrets into the chat.
      reason = "; ".join(
        f"{'.'.join(str(p) for p in e['loc']) or '(root)'}: {e['msg']}"
        for e in err.errors()
      )
      self.ctx.notifier.send_message(
        self.presenter.signal_rejected(reason, self._current_footer())
      )
      return

    if not self._ensure_connected():
      return

    self._process_signal(signal)

  def _process_signal(self, signal: SignalSchema) -> None:
    """Execute one validated, connection-checked signal end-to-end: apply the
    MAX_OPEN_ORDERS exposure guard, run it through the handler, then persist and
    notify the outcome. Split out of :meth:`_process_message` so each NATS subject
    (ADMIN / SYSTEM / signal) has its own handler."""
    # Signal-execution gate: an ADMIN BLOCK_SIGNAL suspends *all* signal
    # execution for this worker until an ALLOW_SIGNAL clears it. This is the
    # single funnel for both live signals and RETRY_SIGNALS replays, so blocking
    # here covers both. Open positions are untouched — they can still be closed
    # out-of-band via an ADMIN FLAT.
    if self._signals_blocked:
      log.warning(
        "[%s Process] Signal SKIPPED — execution is BLOCKED (BLOCK_SIGNAL) | %s | %s",
        self.name,
        signal.symbol,
        signal.action.value,
      )
      return

    # Unknown-strategy guard: a signal whose strategy has no isolation handle
    # (FOREX: no STRATEGY_MAGIC_MAP entry) can't be routed — every code path
    # downstream calls _magic_for and would raise KeyError deep in the executor,
    # bubbling up as an unhandled "manual reconciliation may be required" error.
    # Skip cleanly with an operator alert so the misconfig (subscribed on NATS
    # but not mapped) is visible and correctable. Crypto's _magic_for returns
    # None, so this is a no-op there.
    try:
      self._magic_for(signal.strategy)
    except (KeyError, ValueError) as exc:
      reason = f"Unknown strategy '{signal.strategy}' — no STRATEGY_MAGIC_MAP entry."
      log.error(
        "[%s Process] Signal SKIPPED — %s (%s) | %s | %s. "
        "Add it to STRATEGY_MAGIC_MAP or remove it from NATS_SUBJECTS.",
        self.name,
        reason,
        exc,
        signal.symbol,
        signal.action.value,
      )
      self.ctx.notifier.send_message(
        self.presenter.signal_rejected(reason, self._current_footer())
      )
      return

    # Scale-in (averaging): the broker has already scaled SL/TP1/TP2/quantity in
    # the payload, so every downstream step (SL/TP placement, persistence,
    # notifications) consumes them verbatim. The only re-derivation happens inside
    # the executor when VOLUME_DECISION sizes the entry from risk — see
    # SignalSchema.scale_quantity_factor. Log the broker-reported multipliers for
    # traceability.
    if signal.is_scale_position:
      log.info(
        "[%s Process] Scale-in position | %s | scaling=%s | sl=%s tp1=%s tp2=%s qty=%s",
        self.name,
        signal.symbol,
        signal.scaling,
        signal.sl,
        signal.tp1,
        signal.tp2,
        signal.quantity,
      )

    log.info(
      "[%s Process] Processing Signal: %s | %s | TV Time: %s",
      self.name,
      signal.symbol,
      signal.action.value,
      signal.timestamp,
    )

    # Entry guards: a rejected entry is never sent to the broker. It is still
    # recorded (status REJECTED), forwarded to the broker via CDC on the TRADE
    # subject, and notified. Exits are never gated here so a position can always
    # be closed. The single-position-per-symbol guard runs first (it is the
    # stricter rule): while any order is open on the symbol, no new entry is
    # placed — unless the market allows multiple strategies per symbol
    # (FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL), in which case only a position
    # already held by *this* strategy still blocks.
    if signal.action in _ENTRY_ACTIONS:
      allow_multi_strategy = bool(
        getattr(
          getattr(self.handler, "strategy", None),
          "allows_multi_strategy_per_symbol",
          False,
        )
      )
      reject_reason = guard.symbol_open_rejection(
        self.ctx.db_service, signal, allow_multi_strategy=allow_multi_strategy
      )
      if reject_reason is None:
        reject_reason = guard.max_open_orders_rejection(
          self.ctx.db_service, self.settings, signal
        )
      if reject_reason is not None:
        self._reject_signal(signal, reject_reason)
        return

    result = self.handler.handle(signal)
    # Some exchanges (notably Binance testnet) return a 0 fill price on a filled
    # MARKET order — for entries *and* closes (FLAT/TP2/SL/...). Fall back to the
    # signal's price so the notification and DB never record a misleading 0.0.
    # NB: result.get("price", signal.price) does NOT help — the key is present
    # but zero, not missing. This is the single funnel for every signal action,
    # so the fallback belongs here rather than in each executor close path (which
    # has no access to the signal anyway).
    if result.get("success") and not result.get("price") and signal.price:
      result["price"] = signal.price
    footer = self._current_footer()

    self.ctx.db_service.log_position(
      strategy=signal.strategy,
      ref_id=result.get("ticket"),
      ref_source_id=result.get("source_ticket") or result.get("ticket"),
      symbol=signal.symbol,
      action=signal.action.value,
      volume=result.get("volume", signal.quantity),
      price=result.get("price", signal.price),
      sl=getattr(signal, "sl", None),
      tp1=getattr(signal, "tp1", None),
      gateway_return_code=result.get("retcode", -1),
      comment=result.get("comment", ""),
      message=signal.model_dump_json(),
      author=LogAuthorEnum.BROKER.value,
      market_type=self._market_type,
      signal_id=signal.signal_id,
    )

    if result.get("success"):
      self._persist_success(signal, result, footer)
      if result.get("sl_failsafe_close") is not None:
        # A breakeven SL could not be placed and the position was force-closed
        # (or could not be) to avoid running unprotected — alert loudly instead
        # of reporting a normal fill.
        msg = self.presenter.position_unprotected_closed(signal, result, footer)
      else:
        risk_info = self._resolve_risk_info(signal)
        msg = self.presenter.order_filled(
          signal,
          result,
          result.get("source_ticket") or result.get("ticket"),
          footer,
          risk_info=risk_info,
          settings_dict=self.settings,
        )
    else:
      msg = self.presenter.order_failed(signal, result, footer)
    self.ctx.channel_notifier.send_message(msg)

  def _persist_success(self, signal: SignalSchema, result: dict, footer: str) -> None:
    action_val = signal.action.value
    pos_ticket = result.get("source_ticket") or result.get("ticket")
    signal_json = signal.model_dump_json()

    for fc in result.get("forced_closed", []):
      self.ctx.channel_notifier.send_message(
        self.presenter.force_closed(signal.symbol, signal.strategy, fc, footer)
      )

    if action_val in ("LONG", "SHORT"):
      self.ctx.db_service.insert_position(
        ref_id=pos_ticket,
        strategy=signal.strategy,
        symbol=signal.symbol,
        action=action_val.lower(),
        volume=result.get("volume", signal.quantity),
        opened_price=result.get("price", signal.price),
        gateway_return_code=result.get("retcode"),
        comment=result.get("comment", ""),
        message=signal_json,
        strategy_code=self._magic_for(signal.strategy),
        market_type=self._market_type,
        signal_id=signal.signal_id,
      )
    else:
      status = _CLOSE_STATUS_MAP.get(action_val)
      closed_price = result.get("price")
      # If a breakeven SL failed and the now-unprotected position was
      # emergency-closed, it is fully flat — persist it as FORCED_CLOSED rather
      # than a still-open TP1 partial. (A failed failsafe close leaves the row at
      # its mapped status; the comment flags it for manual attention.)
      failsafe = result.get("sl_failsafe_close")
      if failsafe is not None and failsafe.get("success"):
        status = PositionStatusEnum.FORCED_CLOSED
        closed_price = failsafe.get("price") or closed_price
      if status:
        self.ctx.db_service.update_position_status(
          ref_source_id=pos_ticket,
          status=status,
          ref_id=result.get("ticket"),
          closed_price=closed_price,
          gateway_return_code=result.get("retcode"),
          comment=result.get("comment", ""),
          message=signal_json,
        )

  def _reject_signal(self, signal: SignalSchema, reason: str) -> None:
    """Handle a policy-rejected entry end-to-end without touching the broker.

    Records the rejection in the append-only log and as a REJECTED position row (so
    :class:`PositionCDC` forwards it to the broker on the TRADE subject with status
    REJECTED), then notifies the community channel. No order is placed.
    """
    log.warning(
      "[%s Process] Entry REJECTED | %s %s | %s",
      self.name,
      signal.symbol,
      signal.action.value,
      reason,
    )
    footer = self._current_footer()
    signal_json = signal.model_dump_json()
    # No broker order exists, so there's no ticket to key on — echo the broker's
    # signal_id (falling back to a deterministic tag) so the rejection is still
    # correlatable end-to-end.
    ref = (
      signal.signal_id
      or f"REJECTED-{signal.strategy}-{signal.symbol}-{int(signal.timestamp.timestamp())}"
    )
    volume = signal.quantity or 0.0
    price = signal.price or 0.0

    self.ctx.db_service.log_position(
      strategy=signal.strategy,
      ref_id=ref,
      ref_source_id=ref,
      symbol=signal.symbol,
      action=signal.action.value,
      volume=volume,
      price=price,
      sl=getattr(signal, "sl", None),
      tp1=getattr(signal, "tp1", None),
      gateway_return_code=-1,
      comment=reason,
      message=signal_json,
      author=LogAuthorEnum.BROKER.value,
      market_type=self._market_type,
      signal_id=signal.signal_id,
    )

    self.ctx.db_service.insert_rejected_position(
      ref_id=ref,
      strategy=signal.strategy,
      symbol=signal.symbol,
      action=signal.action.value.lower(),
      volume=volume,
      opened_price=price,
      gateway_return_code=-1,
      comment=reason,
      message=signal_json,
      strategy_code=self._magic_for(signal.strategy),
      market_type=self._market_type,
      signal_id=signal.signal_id,
    )

    self.ctx.channel_notifier.send_message(
      self.presenter.order_rejected(signal, reason, footer)
    )

  # ── Shared ADMIN FLAT handling ────────────────────────────────────────── #

  def _handle_admin_message(self, raw: str, *, private: bool = False) -> None:
    """Parse a NATS ADMIN message and dispatch by ``action``.

    A message arrives on one of two subjects, selected by ``private``:

    * the **public** ``ADMIN`` subject — fanned out to every worker with optional
      ``market``/``gateway`` filters and **no** ``account_id`` (not
      account-scoped); or
    * this worker's **private** ``ADMIN.<market>.<gateway>.<account_id>`` subject
      — the full composite identity is required and must match this worker.

    Supported actions:

    * ``FLAT`` (both subjects) — close live positions, then reconcile the DB
      (see :meth:`_handle_admin_flat`).
    * ``BLOCK_SIGNAL`` / ``ALLOW_SIGNAL`` (private only) — toggle whether this
      worker executes incoming SIGNALs (see :meth:`_handle_signal_control`).
    """
    try:
      data = json.loads(raw)
      admin = AdminMessageSchema(**data)
    except (json.JSONDecodeError, ValidationError) as err:
      log.error("[ADMIN] Parse error: %s", err)
      return

    if admin.action == AdminActionEnum.FLAT:
      # FLAT touches the broker (closes live positions), so it needs a live
      # connection; the signal-control toggles below do not.
      if not self._ensure_connected():
        return
      self._handle_admin_flat(data, raw, private=private)
      return

    if admin.action in (AdminActionEnum.BLOCK_SIGNAL, AdminActionEnum.ALLOW_SIGNAL):
      self._handle_signal_control(admin.action, data, private=private)
      return

    log.warning("[ADMIN] Unknown action: %s", admin.action)
    return

  def _handle_admin_flat(self, data: dict, raw: str, *, private: bool) -> None:
    """Route + execute a FLAT: validate the subject-specific payload, confirm it
    targets this worker, then close live positions and reconcile the DB.

    The close-and-reconcile algorithm is identical for every market and both
    subjects; only *how a live close maps to a DB row* differs (MT5 matches by
    ticket, a CEX by resolved symbol) — the :meth:`_flat_match_key` /
    :meth:`_flat_db_match_keys` pair. The broker is always the source of truth: a
    row is marked ``FLATTED`` only when its close actually succeeded *or* the
    position was never live on the broker. A row whose close was **attempted but
    failed** is left OPEN (it is still live), so the DB never claims a position
    is flat while it is not."""
    schema_cls = PrivateAdminFlatSchema if private else AdminFlatSchema
    try:
      admin_flat_schema = schema_cls(**data)
    except ValidationError as err:
      log.error(
        "[ADMIN FLAT] Invalid %s payload: %s",
        "private" if private else "public",
        err,
      )
      return
    if not self._flat_targets_this_worker(admin_flat_schema, private=private):
      log.info(
        "[ADMIN FLAT] Skipping (%s): market=%s gateway=%s account_id=%s != "
        "worker market=%s gateway=%s account=%s",
        "private" if private else "public",
        admin_flat_schema.market,
        admin_flat_schema.gateway,
        getattr(admin_flat_schema, "account_id", None),
        self._market_type,
        self._gateway_value,
        self._account_id,
      )
      return
    # Close live broker positions first (source of truth), then reconcile the DB.
    closed, attempted = self._close_live_positions_for_flat(admin_flat_schema)
    self._reconcile_flat_db(admin_flat_schema, closed, attempted, raw)

  def _handle_signal_control(
    self, action: AdminActionEnum, data: dict, *, private: bool
  ) -> None:
    """Handle a private ADMIN ``BLOCK_SIGNAL`` / ``ALLOW_SIGNAL``: toggle whether
    this worker executes incoming SIGNALs.

    Signal control is account-scoped, so it is only accepted on the worker's
    **private** subject (``ADMIN.<market>.<gateway>.<account_id>``); the same
    action arriving on the public ``ADMIN`` subject is ignored. The composite
    identity is required and re-validated against this worker before the toggle
    is applied."""
    if not private:
      log.warning(
        "[ADMIN %s] Ignored on the public ADMIN subject — signal control is "
        "account-scoped and only accepted on the private subject.",
        action.value,
      )
      return
    try:
      ctrl = PrivateAdminSignalControlSchema(**data)
    except ValidationError as err:
      log.error("[ADMIN %s] Invalid private payload: %s", action.value, err)
      return
    if not self._flat_targets_this_worker(ctrl, private=True):
      log.info(
        "[ADMIN %s] Skipping: market=%s gateway=%s account_id=%s != worker "
        "market=%s gateway=%s account=%s",
        action.value,
        ctrl.market,
        ctrl.gateway,
        ctrl.account_id,
        self._market_type,
        self._gateway_value,
        self._account_id,
      )
      return
    self._set_signals_blocked(action == AdminActionEnum.BLOCK_SIGNAL)

  def _set_signals_blocked(self, blocked: bool) -> None:
    """Flip the signal-execution gate and notify the operator on a real change.
    A repeat of the current state is a no-op (logged, no duplicate alert)."""
    state = "BLOCKED" if blocked else "ALLOWED"
    if self._signals_blocked == blocked:
      log.info("[ADMIN] Signal execution already %s — no change.", state)
      return
    self._signals_blocked = blocked
    log.warning("[ADMIN] Signal execution now %s for this worker.", state)
    footer = self._current_footer()
    msg = (
      self.presenter.signals_blocked(footer)
      if blocked
      else self.presenter.signals_allowed(footer)
    )
    self.ctx.channel_notifier.send_message(msg)

  def _flat_targets_this_worker(self, admin, *, private: bool = False) -> bool:
    """Whether this worker should act on an ADMIN FLAT.

    Private subject (``private=True``): ``market``/``gateway``/``account_id`` are
    all required and must match this worker's identity exactly — the private
    subject already addresses one worker, so this re-check is defence-in-depth
    against a misrouted publish.

    Public subject (``private=False``): ``market``/``gateway`` are optional
    filters (unset matches every worker on that dimension; both unset broadcasts
    to everyone matching strategy/symbol). The public FLAT carries no
    ``account_id`` — account-scoped FLATs use the private subject instead."""
    if private:
      return (
        admin.market == self._market_type
        and admin.gateway == self._gateway_value
        and admin.account_id == self._account_id
      )
    if admin.market and admin.market != self._market_type:
      return False
    if admin.gateway and admin.gateway != self._gateway_value:
      return False
    return True

  def _close_live_positions_for_flat(self, admin) -> tuple[Dict[Any, dict], set]:
    """Close every live broker position matching the FLAT filter.

    Returns ``(closed, attempted)``, both keyed by :meth:`_flat_match_key` —
    ``closed`` maps the key to its successful close result; ``attempted`` is every
    key we tried (so the reconcile step can tell a failed close from one that was
    never live).
    """
    if admin.symbol:
      positions = self.executor.get_open_positions(
        admin.symbol, strategy=admin.strategy
      )
    else:
      positions = self.executor.get_all_open_positions(strategy=admin.strategy)

    if positions:
      log.info(
        "[ADMIN FLAT] Closing %d %s position(s) (strategy=%s, symbol=%s)",
        len(positions),
        self.name,
        admin.strategy,
        admin.symbol,
      )
    else:
      log.warning(
        "[ADMIN FLAT] No open %s positions (strategy=%s, symbol=%s)",
        self.name,
        admin.strategy,
        admin.symbol,
      )

    attempted: set = set()
    closed: Dict[Any, dict] = {}
    for pos in positions:
      key = self._flat_match_key(pos)
      attempted.add(key)
      result = self.executor.close_single_position(pos, reason="FLAT")
      if result.get("success"):
        closed[key] = result
        log.info(
          "[ADMIN FLAT] Closed %s key=%s vol=%s", self.name, key, result.get("volume")
        )
      else:
        log.error(
          "[ADMIN FLAT] Failed to close %s key=%s: %s",
          self.name,
          key,
          result.get("comment"),
        )
    return closed, attempted

  def _reconcile_flat_db(
    self, admin, closed: Dict[Any, dict], attempted: set, raw: str
  ) -> None:
    """Reconcile DB rows against what actually closed.

    A row is marked ``FLATTED`` only when its close succeeded, or when it was never
    live on the broker (already closed externally). A row whose close was
    *attempted but failed* is left OPEN — it is still live — and flagged loudly.
    """
    db_positions = self.ctx.db_service.get_open_positions_for_flat(
      strategy=admin.strategy, symbol=admin.symbol
    )
    footer = self._account_footer()
    for db_pos in db_positions:
      db_keys = self._flat_db_match_keys(db_pos)
      matched_key = next((k for k in db_keys if k in closed), None)
      if matched_key is not None:
        result = closed[matched_key]
        self.ctx.db_service.update_position_status(
          ref_source_id=db_pos.get("ref_source_id"),
          status=PositionStatusEnum.FLATTED,
          ref_id=result.get("ticket"),
          closed_price=result.get("price"),
          gateway_return_code=result.get("retcode", 0),
          comment=result.get("comment", ""),
          message=raw,
        )
        self.ctx.channel_notifier.send_message(
          self.presenter.admin_flat_closed(db_pos, result, footer)
        )
      elif db_keys.isdisjoint(attempted):
        # Never seen live on the broker → already closed externally; sync the DB.
        log.warning(
          "[ADMIN FLAT] %s in DB but not found on %s — marking FLATTED",
          db_pos.get("symbol"),
          self.name,
        )
        self.ctx.db_service.update_position_status(
          ref_source_id=db_pos.get("ref_source_id"),
          status=PositionStatusEnum.FLATTED,
          comment=f"Admin FLAT (position not found on {self.name})",
          message=raw,
        )
      else:
        # Attempted but the close FAILED → still live on the broker. Leave the DB
        # row OPEN so it is never falsely reported flat; flag for manual attention.
        log.error(
          "[ADMIN FLAT] %s close FAILED — DB row left OPEN (still live on %s, "
          "manual check required).",
          db_pos.get("symbol"),
          self.name,
        )

  # ── Shared SYSTEM handling ────────────────────────────────────────────── #

  def _handle_system_message(self, raw: str) -> None:
    """Handle a NATS ``SYSTEM`` message: parse the envelope, then dispatch the
    action to the market-specific :meth:`_handle_system_action` hook.

    SYSTEM messages drive operational/maintenance actions (e.g. re-initialising
    per-symbol leverage on a crypto exchange) that are not trade signals. The
    base only validates the common envelope (action + timestamp + account_id); each market
    decides which actions it understands — an unknown action is logged and
    ignored rather than raising, so a SYSTEM action meant for another market type
    is harmless here.
    """
    try:
      data = json.loads(raw)
      system = SystemSchema(**data)
    except (json.JSONDecodeError, ValidationError) as err:
      log.error("[SYSTEM] Parse error: %s", err)
      return

    log.info(
      "[%s SYSTEM] Received action=%s account_id=%s",
      self.name,
      getattr(system.action, "value", system.action),
      system.account_id,
    )

    if not self._ensure_connected():
      return

    # Route by worker identity: every SYSTEM message carries a required
    # account_id (NATS-name format "<market>-<gateway>-<account_id>") and is
    # executed only by the matching worker. A blank value would match nobody.
    if system.account_id and system.account_id != self._system_account_id:
      log.info(
        "[%s SYSTEM] Skipping action=%s: account_id=%s != worker=%s",
        self.name,
        getattr(system.action, "value", system.action),
        system.account_id,
        self._system_account_id,
      )
      return

    self._handle_system_action(system.action, data)

  @property
  def _system_account_id(self) -> Optional[str]:
    """This worker's identity for SYSTEM routing, in NATS-name format
    ``<market>-<gateway>-<account_id>`` (matches the NATS connection name; see
    Settings._validate_market_requirements)."""
    return self.settings.get("account_id") or None

  @property
  def _private_admin_subject(self) -> Optional[str]:
    """This worker's private ADMIN subject: ``ADMIN.<market>.<gateway>.<account_id>``.

    The broker publishes an account-scoped FLAT here (instead of fanning it out
    on the public ``ADMIN`` subject) so only this worker receives it. Returns
    None when the identity is incomplete — there is nothing unique to subscribe
    to, so the worker relies on the public subject alone."""
    if not (self._market_type and self._gateway_value and self._account_id):
      return None
    return (
      f"{NatsSubjectEnum.ADMIN.value}."
      f"{self._market_type}.{self._gateway_value}.{self._account_id}"
    )

  # Settings key holding this market's gateway enum (exchange / platform).
  # Concrete processors set it so the WORKER_CONNECTED handshake can report the
  # gateway name without the base knowing the per-market field.
  _gateway_setting_key: str = ""

  @property
  def _gateway_value(self) -> str:
    """Gateway name for this worker (e.g. ``BINANCE``, ``MT5``), read from the
    market-specific setting named by ``_gateway_setting_key``."""
    g = self.settings.get(self._gateway_setting_key)
    return getattr(g, "value", None) or str(g or "")

  def _subscribed_strategies(self) -> list[str]:
    """Strategy names this worker subscribes to (from ``NATS_SUBJECTS`` minus
    the control subjects). Shipped in WORKER_CONNECTED so the broker knows
    which strategies' recent signals belong in a RETRY_SIGNALS replay."""
    return parse_strategy_subjects(self.settings.get("nats_subjects", "") or "")

  def _worker_connected_payload(self) -> Optional[str]:
    """Build the WORKER_CONNECTED handshake JSON, or None if this worker has no
    identity yet (no account_id → nothing for the broker to target)."""
    account_id = self._system_account_id
    if not account_id:
      return None
    return SystemWorkerConnectedSchema(
      account_id=account_id,
      market=self._market_type,
      gateway=self._gateway_value,
      strategies=self._subscribed_strategies(),
    ).model_dump_json()

  def _announce_worker_connected(self) -> None:
    """Request/reply a SYSTEM ``WORKER_CONNECTED`` so the broker can push any
    initial config targeted at this worker. Waits for the SYSTEM subscription to
    be live first — NATS core does not replay, so a reply arriving before we are
    subscribed would be lost.

    The broker always replies now (``CRYPTO_LEVERAGE_INIT`` /
    ``WORKER_CONNECTED_ACK`` / ``WORKER_CONNECTED_ERROR``), so a timeout means the
    broker genuinely didn't get it. The handshake is idempotent and mandatory
    before trading (crypto needs ``default_leverage``), so this blocks and
    retries with backoff until it succeeds rather than giving up.
    """
    payload = self._worker_connected_payload()
    if payload is None:
      log.warning(
        "[%s Process] No account_id — skipping WORKER_CONNECTED handshake.", self.name
      )
      return
    if self.publisher is None:
      return
    if self.subscriber is not None and not self.subscriber.wait_subscribed(timeout=10):
      log.warning(
        "[%s Process] SYSTEM subscription not confirmed within timeout — "
        "announcing WORKER_CONNECTED anyway (reply may be missed).",
        self.name,
      )

    # Desynchronise a reconnect storm (see _HANDSHAKE_JITTER_MAX) before the
    # very first attempt only — retries are already spaced out by the backoff.
    time.sleep(random.uniform(0, _HANDSHAKE_JITTER_MAX))

    attempt = 0
    while True:
      try:
        raw_response = self.publisher.request(
          NatsSubjectEnum.SYSTEM, payload, timeout=_HANDSHAKE_TIMEOUT
        )
      except Exception as exc:
        delay = _HANDSHAKE_BACKOFF[min(attempt, len(_HANDSHAKE_BACKOFF) - 1)]
        attempt += 1
        log_fn = log.error if attempt >= _HANDSHAKE_ALERT_THRESHOLD else log.warning
        log_fn(
          "[%s Process] WORKER_CONNECTED handshake failed (%s) — attempt %d, retrying in %ds.",
          self.name,
          exc,
          attempt,
          delay,
        )
        time.sleep(delay)
        continue

      log.info(
        "[%s Process] Announced WORKER_CONNECTED account_id=%s",
        self.name,
        self._system_account_id,
      )
      self._handle_worker_connected_response(raw_response)
      return

  def _handle_worker_connected_response(self, raw: str) -> None:
    """Dispatch the broker's reply to WORKER_CONNECTED. ``CRYPTO_LEVERAGE_INIT``
    is routed through the normal :meth:`_handle_system_action` hook (identical to
    receiving it via the SYSTEM subscription); ``WORKER_CONNECTED_ACK`` needs no
    further action; ``WORKER_CONNECTED_ERROR`` is logged for operator attention —
    it signals a broker-side config problem (e.g. missing settings), which a
    retry cannot fix."""
    try:
      data = json.loads(raw)
      action = SystemActionEnum(data.get("action"))
    except (json.JSONDecodeError, ValueError) as err:
      log.error("[%s Process] WORKER_CONNECTED reply parse error: %s", self.name, err)
      return

    if action == SystemActionEnum.WORKER_CONNECTED_ERROR:
      error = SystemWorkerConnectedErrorSchema(**data)
      log.error(
        "[%s Process] WORKER_CONNECTED_ERROR: %s",
        self.name,
        error.reason or "(no reason given)",
      )
      return

    if action == SystemActionEnum.WORKER_CONNECTED_ACK:
      log.info(
        "[%s Process] WORKER_CONNECTED_ACK — handshake complete, no init config needed.",
        self.name,
      )
      return

    self._handle_system_action(action, data)

  def _handle_system_action(self, action: SystemActionEnum, data: dict) -> None:
    """Dispatch a parsed SYSTEM ``action``. Default: log and ignore.

    ``RETRY_SIGNALS`` and ``STRATEGY_MAGIC_MAP`` are handled here for every
    market — the base owns the dedup+timeout gate for replays and the magic-map
    store so both FOREX and CRYPTO behave identically. Markets that add their own
    SYSTEM actions override this hook and delegate to ``super()`` so the shared
    actions still fire.
    """
    if action == SystemActionEnum.RETRY_SIGNALS:
      self._handle_retry_signals(data)
      return
    if action == SystemActionEnum.STRATEGY_MAGIC_MAP:
      self._handle_strategy_magic_map(data)
      return
    log.info(
      "[%s SYSTEM] No handler for action=%s — ignoring.",
      self.name,
      getattr(action, "value", action),
    )

  def _handle_strategy_magic_map(self, data: dict) -> None:
    """Apply a ``STRATEGY_MAGIC_MAP`` push: store the broker's per-strategy magic
    map into the live settings (replacing the legacy ``STRATEGY_MAGIC_MAP`` .env
    value) so the executor and :class:`PositionCDC` resolve magics from it exactly
    as before.

    Only entries for strategies this worker actually subscribes to (its
    ``NATS_SUBJECTS`` minus the control subjects) are kept — the map is scoped to
    this worker so it never claims a magic for a strategy it does not trade. A bad
    envelope is dropped rather than crashing the SYSTEM listener.

    Normally delivered as the ``WORKER_CONNECTED`` reply, i.e. during ``connect()``
    before ``start_market_jobs()`` builds the CDC / close-detection jobs, so those
    jobs read the freshly-stored map. The already-built executor is updated in
    place via :meth:`_set_executor_magic_map` so magic resolution and
    ``owned_magics()`` reflect the push immediately, on connect or at runtime.
    """
    try:
      envelope = SystemStrategyMagicMapSchema(**data)
    except ValidationError as err:
      log.error("[%s SYSTEM] STRATEGY_MAGIC_MAP envelope invalid: %s", self.name, err)
      return

    subscribed = set(self._subscribed_strategies())
    mapping = {
      strategy: magic
      for strategy, magic in envelope.strategy_magic_map.items()
      if strategy in subscribed
    }
    ignored = sorted(set(envelope.strategy_magic_map) - subscribed)
    if ignored:
      log.warning(
        "[%s SYSTEM] STRATEGY_MAGIC_MAP: ignoring %d entr%s for strategies this "
        "worker does not subscribe to: %s",
        self.name, len(ignored), "y" if len(ignored) == 1 else "ies", ", ".join(ignored),
      )

    self.settings["strategy_magic_map"] = mapping
    self._set_executor_magic_map(mapping)
    log.info(
      "[%s SYSTEM] STRATEGY_MAGIC_MAP applied | strategies=%d", self.name, len(mapping)
    )

  def _set_executor_magic_map(self, mapping: dict) -> None:  # noqa: B027 - optional hook
    """Propagate an updated strategy→magic map to the already-built executor.

    Default no-op: a market with no magic concept (e.g. CRYPTO) resolves nothing
    on the executor and its :class:`PositionCDC` reads the map straight from
    settings, so storing it there is enough. FOREX overrides this to refresh the
    executor's in-memory map (magic resolution + ``owned_magics()``)."""

  def _handle_retry_signals(self, data: dict) -> None:
    """Execute a RETRY_SIGNALS replay: for each signal, drop it if already
    processed (dedup by ``signal_id`` against ``position_logs``) or older than
    :data:`~worker.settings.MAX_RETRY_TIMEOUT` (against the signal's own
    ``timestamp``); otherwise run it through the normal signal pipeline.

    A bad envelope is dropped (never crashes the SYSTEM listener) and a single
    bad signal in an otherwise valid batch does not abort the rest — the goal
    of a replay is to fill gaps, so each entry stands on its own.
    """
    try:
      envelope = SystemRetrySignalsSchema(**data)
    except ValidationError as err:
      log.error("[%s SYSTEM] RETRY_SIGNALS envelope invalid: %s", self.name, err)
      return

    signals = envelope.signals
    if not signals:
      log.info("[%s SYSTEM] RETRY_SIGNALS: empty batch — nothing to do.", self.name)
      return

    now = datetime.now(timezone.utc)
    executed = skipped_dedup = skipped_stale = failed = 0

    for signal in signals:
      # Dedup first (cheap DB lookup) so a replay of a signal we already
      # processed never re-hits the broker even if it's still within the window.
      if signal.signal_id and self.ctx.db_service.signal_exists(signal.signal_id):
        skipped_dedup += 1
        log.info(
          "[%s SYSTEM] RETRY_SIGNALS: skip signal_id=%s (already processed).",
          self.name,
          signal.signal_id,
        )
        continue

      age = _seconds_since(signal.timestamp, now)
      if age is None or age > MAX_RETRY_TIMEOUT:
        skipped_stale += 1
        log.info(
          "[%s SYSTEM] RETRY_SIGNALS: skip signal_id=%s symbol=%s action=%s — "
          "age=%.1fs > MAX_RETRY_TIMEOUT=%ds.",
          self.name,
          signal.signal_id,
          signal.symbol,
          signal.action.value,
          age if age is not None else float("nan"),
          MAX_RETRY_TIMEOUT,
        )
        continue

      try:
        self._process_signal(signal)
        executed += 1
      except Exception:
        failed += 1
        log.exception(
          "[%s SYSTEM] RETRY_SIGNALS: signal_id=%s failed — continuing batch.",
          self.name,
          signal.signal_id,
        )

    log.info(
      "[%s SYSTEM] RETRY_SIGNALS done | executed=%d dedup=%d stale=%d failed=%d total=%d",
      self.name,
      executed,
      skipped_dedup,
      skipped_stale,
      failed,
      len(signals),
    )

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _market_type_value(mt) -> str:
    return mt.value if isinstance(mt, MarketTypeEnum) else str(mt or "")

  def _resolve_risk_info(self, signal: SignalSchema):
    """Return ``(risk_percent, is_custom)`` for entry signals when VDE is on, else None.

    ``is_custom`` is True when USE_CUSTOM_RISK_PERCENTAGE overrides the signal's
    own risk_percent (so the gear icon is shown in notifications).
    """
    if (
      signal.action.value not in ("LONG", "SHORT")
      or not self.config.volume_decision_enabled
    ):
      return None
    if self.config.use_custom_risk_percentage:
      return (self.config.risk_percentage, True)
    use_signal_risk = signal.risk_percent is not None and signal.risk_percent > 0
    risk = signal.risk_percent if use_signal_risk else self.config.risk_percentage
    return (risk, False)

  def _current_footer(self) -> str:
    """Account footer for notifications, cached for ``_FOOTER_TTL`` seconds so a
    burst of signals doesn't trigger a live account fetch per message."""
    now = time.monotonic()
    if self._footer_cache is None or (now - self._footer_cache_at) > _FOOTER_TTL:
      self._footer_cache = self._account_footer()
      self._footer_cache_at = now
    return self._footer_cache

  def _ensure_connected(self) -> bool:
    """Verify/restore the broker connection before acting. Default: always ready.

    Overridden by brokers that hold a persistent connection (MT5) to reconnect.
    """
    return True

  # ── Abstract hooks (each concrete market must define these) ───────────── #

  @abstractmethod
  def _build_executor(self) -> Any:
    """Build and return the broker executor (and any client/bridge it needs)."""

  @abstractmethod
  def _connect_broker(self) -> bool:
    """Establish the broker connection. Return True on success."""

  @abstractmethod
  def _disconnect_broker(self) -> None:
    """Tear down the broker connection."""

  @abstractmethod
  def _account_footer(self) -> str:
    """Return the live account footer appended to notifications."""

  @property
  @abstractmethod
  def _account_id(self) -> str:
    """Stable identifier for this worker's account (used for ADMIN routing)."""

  @abstractmethod
  def _magic_for(self, strategy: str) -> Optional[int]:
    """Resolve the broker isolation handle for *strategy* (None where N/A)."""

  @abstractmethod
  def _position_cdc_kwargs(self) -> Dict[str, Any]:
    """Return ``account_info_fn`` / ``account_name`` / ``strategy_magic_map``."""

  @abstractmethod
  def _start_broker_jobs(self, stop_event) -> None:
    """Start broker-specific background jobs (health, close detection, …)."""

  @abstractmethod
  def _flat_match_key(self, pos: Any) -> Any:
    """Identity used to correlate a *live* broker position with a DB row during a
    FLAT (MT5 → ticket; a CEX → resolved exchange symbol)."""

  @abstractmethod
  def _flat_db_match_keys(self, db_pos: dict) -> set:
    """The set of keys a DB row can match a closed live position by — compared
    against the keys produced by :meth:`_flat_match_key` (MT5 → ``{ref_id,
    ref_source_id}``; a CEX → ``{resolved symbol}``)."""
