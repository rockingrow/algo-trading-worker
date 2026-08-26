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
from worker.schemas.cycle_schema import (
  CycleEventSchema,
  CycleOutcomeEnum,
  CycleStatusEnum,
)
from worker.schemas.inbox_schema import (
  WorkerConnectedAckSchema,
  WorkerConnectedErrorSchema,
  WorkerConnectedSchema,
  WorkerSettingsSchema,
)
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalActionEnum, SignalSchema
from worker.schemas.system_schema import SystemActionEnum, SystemSchema
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

# Actions that close (part of) a position, and are therefore the only ones that
# can book a realized PnL onto the cycle.
_CYCLE_EXIT_ACTIONS = frozenset(
  {
    SignalActionEnum.TP1,
    SignalActionEnum.TP2,
    SignalActionEnum.SL,
    SignalActionEnum.R_SL,
    SignalActionEnum.FLAT,
  }
)

# Exit action → DB status, shared by every market.
_CLOSE_STATUS_MAP: Dict[str, PositionStatusEnum] = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
  "FLAT": PositionStatusEnum.FLATTED,
}


def _as_text(value: Any) -> Optional[str]:
  """Broker reference as a string, or None when absent.

  Ticket ids arrive as ints from some gateways and strings from others; the
  cycle stores them as text so the two render identically."""
  return None if value is None else str(value)


def _seconds_since(ts: Optional[datetime], now: datetime) -> Optional[float]:
  """Seconds between *ts* and *now*, treating a naive *ts* as UTC. Returns
  None when *ts* is missing so the signal-replay handler can drop a payload
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
  strategies to scope the ACK's ``strategy_magic_map`` and ``retry_signals``
  replay to for this worker."""
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
    # The account footer needs a connected broker, so the cycle notifier (built
    # in the market-agnostic context) only gets its source now.
    self.ctx.cycle_notifier.bind_footer(self._current_footer)

    # Handshake: tell the broker this worker is online so it can push any
    # per-worker init (magics, leverage, replay). The broker decides from
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
    entry guards (:meth:`_entry_rejection`), run it through the handler, then
    persist and notify the outcome. Split out of :meth:`_process_message` so each
    NATS subject (ADMIN / SYSTEM / signal) has its own handler."""
    # Signal-execution gate: an ADMIN BLOCK_SIGNAL suspends *all* signal
    # execution for this worker until an ALLOW_SIGNAL clears it. This is the
    # single funnel for both live signals and the ACK's replay, so blocking
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
    # (FOREX: no strategy_magic_map entry) can't be routed — every code path
    # downstream calls _magic_for and would raise KeyError deep in the executor,
    # bubbling up as an unhandled "manual reconciliation may be required" error.
    # Skip cleanly with an operator alert so the misconfig (subscribed on NATS
    # but not mapped) is visible and correctable. Crypto's _magic_for returns
    # None, so this is a no-op there.
    try:
      self._magic_for(signal.strategy)
    except (KeyError, ValueError) as exc:
      reason = f"Unknown strategy '{signal.strategy}' — no strategy_magic_map entry."
      log.error(
        "[%s Process] Signal SKIPPED — %s (%s) | %s | %s. "
        "Map it on the broker (WORKER_CONNECTED_ACK.strategy_magic_map) or "
        "remove it from NATS_SUBJECTS.",
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

    # Exits are never gated so a position can always be closed.
    if signal.action in _ENTRY_ACTIONS:
      reject_reason = self._entry_rejection(signal)
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
      # The stop the position really carries, not the one the signal asked for:
      # an entry reports back the level it actually registered with the broker
      # (FOREX widens it to the broker's minimum stop distance), and that is the
      # number the audit trail must hold. Exits carry no stop of their own, so
      # they fall back to the signal's.
      sl=result.get("sl") or getattr(signal, "sl", None),
      # tp1 is the signal's *partial-close* target and is deliberately not
      # overwritten by result["tp"] — that is the full-exit level (tp2) resting
      # on the broker, a different concept that this column does not track.
      tp1=getattr(signal, "tp1", None),
      gateway_return_code=result.get("retcode", -1),
      comment=result.get("comment", ""),
      message=signal.model_dump_json(),
      author=LogAuthorEnum.BROKER.value,
      market_type=self._market_type,
      signal_id=signal.signal_id,
      signal_uxid=signal.signal_uxid,
    )

    if result.get("success"):
      self._persist_success(signal, result, footer)
      if result.get("sl_failsafe_close") is not None:
        # A breakeven SL could not be placed and the position was force-closed
        # (or could not be) to avoid running unprotected — alert loudly instead
        # of reporting a normal fill.
        msg = self.presenter.position_unprotected_closed(signal, result, footer)
        outcome = CycleOutcomeEnum.UNPROTECTED_CLOSED
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
        outcome = CycleOutcomeEnum.FILLED
    else:
      msg = self.presenter.order_failed(signal, result, footer)
      outcome = CycleOutcomeEnum.FAILED

    self._notify_trade(
      signal,
      msg,
      event=self._cycle_event(signal, result, outcome),
      status=self._cycle_status(signal, result),
    )

  def _persist_success(self, signal: SignalSchema, result: dict, footer: str) -> None:
    action_val = signal.action.value
    pos_ticket = result.get("source_ticket") or result.get("ticket")
    signal_json = signal.model_dump_json()

    for fc in result.get("forced_closed", []):
      # Part of this entry's story, so it joins the cycle as its own action
      # rather than arriving as an unrelated message.
      self._notify_trade(
        signal,
        self.presenter.force_closed(signal.symbol, signal.strategy, fc, footer),
        event=CycleEventSchema(
          action=action_val,
          outcome=CycleOutcomeEnum.FORCE_CLOSED,
          timestamp=signal.timestamp,
          price=fc.get("price"),
          volume=fc.get("volume"),
          profit=fc.get("profit"),
          ref_id=_as_text(fc.get("ref_id")),
          ref_source_id=_as_text(fc.get("ref_source_id")),
          reason="Closed to make room for a new entry",
        ),
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
        signal_uxid=signal.signal_uxid,
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

  def _entry_rejection(self, signal: SignalSchema) -> Optional[str]:
    """Run the entry guards against *signal*, returning the first rejection
    reason or ``None`` when it may be sent to the broker.

    A rejected entry is never sent: it is recorded (status REJECTED), forwarded
    to the broker via CDC on the TRADE subject, and notified.

    Ordered cheapest-first, and by how strict the rule is:

    1. **One open order per symbol** — the strictest rule, so it answers first.
       While any order is live on the symbol no new entry is placed, unless the
       market allows several strategies per symbol
       (FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL), in which case only a position
       already held by *this* strategy blocks.
    2. **MAX_OPEN_ORDERS** — the worker's exposure cap.
    3. **Staleness** — needs a live quote from the broker (a tick read / REST
       round-trip), so it runs last: no reason to pay for one on an entry the
       two DB-only guards above already rejected.
    """
    allow_multi_strategy = bool(
      getattr(
        getattr(self.handler, "strategy", None),
        "allows_multi_strategy_per_symbol",
        False,
      )
    )
    reason = guard.symbol_open_rejection(
      self.ctx.db_service, signal, allow_multi_strategy=allow_multi_strategy
    )
    if reason is not None:
      return reason

    reason = guard.max_open_orders_rejection(self.ctx.db_service, self.settings, signal)
    if reason is not None:
      return reason

    return guard.stale_signal_rejection(
      signal, self._entry_quote(signal), self.settings
    )

  def _entry_quote(self, signal: SignalSchema) -> Optional[float]:
    """Live price the entry would fill at, or ``None`` when it can't be read.

    ``getattr`` chain so a market strategy (or a test double) without the
    capability is treated as "no quote": the staleness guard then skips rather
    than blocking the entry. A broker error is swallowed for the same reason —
    failing to fetch a quote must not stop trading, it just means this guard has
    nothing to judge on.
    """
    strategy = getattr(self.handler, "strategy", None)
    getter = getattr(strategy, "entry_price", None)
    if getter is None:
      return None
    try:
      return getter(signal)
    except Exception:
      log.exception(
        "[%s Process] Could not read a live quote for %s — staleness guard skipped.",
        self.name,
        signal.symbol,
      )
      return None

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
      signal_uxid=signal.signal_uxid,
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
      signal_uxid=signal.signal_uxid,
    )

    self._notify_trade(
      signal,
      self.presenter.order_rejected(signal, reason, footer),
      event=CycleEventSchema(
        action=signal.action.value,
        outcome=CycleOutcomeEnum.REJECTED,
        timestamp=signal.timestamp,
        price=signal.price,
        volume=signal.quantity,
        sl=signal.sl,
        tp1=signal.tp1,
        tp2=signal.tp2,
        gateway_return_code=-1,
        reason=reason,
      ),
      status=CycleStatusEnum.REJECTED,
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

    ref_id = getattr(admin, "ref_id", None)
    if ref_id:
      db_positions = self.ctx.db_service.get_open_positions_for_flat(
        strategy=admin.strategy, symbol=admin.symbol, ref_id=ref_id
      )
      allowed_keys = set()
      for db_pos in db_positions:
        allowed_keys.update(self._flat_db_match_keys(db_pos))
      positions = [p for p in positions if self._flat_match_key(p) in allowed_keys]

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

    The ADMIN payload is **not** written over the row's ``gateway_message``. That
    column holds the original *entry signal* JSON, and :class:`PositionCDC` parses
    it for the ``signal_id`` / ``sl`` / ``tp1`` / ``tp2`` the broker needs to match
    the TRADE event back to its own order. Overwriting it with the FLAT payload —
    which carries none of those fields, on the public and private subject alike —
    published an update with ``signal_id=null``, so the broker could not correlate
    it and the order was never updated there. The payload is preserved instead as
    an append-only ``position_logs`` row, which is where per-event audit belongs.
    """
    db_positions = self.ctx.db_service.get_open_positions_for_flat(
      strategy=admin.strategy,
      symbol=admin.symbol,
      ref_id=getattr(admin, "ref_id", None),
    )
    footer = self._account_footer()
    for db_pos in db_positions:
      db_keys = self._flat_db_match_keys(db_pos)
      matched_key = next((k for k in db_keys if k in closed), None)
      if matched_key is not None:
        result = closed[matched_key]
        self._log_flat_event(db_pos, result, raw)
        self.ctx.db_service.update_position_status(
          ref_source_id=db_pos.get("ref_source_id"),
          status=PositionStatusEnum.FLATTED,
          ref_id=result.get("ticket"),
          closed_price=result.get("price"),
          gateway_return_code=result.get("retcode", 0),
          comment=result.get("comment", ""),
        )
        # An admin FLAT is the last action of the position's own cycle, so it
        # closes out that message rather than opening a new one. The uxid comes
        # off the position row — no signal is involved in an admin directive.
        self._notify_cycle_or_send(
          self.presenter.admin_flat_closed(db_pos, result, footer),
          signal_uxid=db_pos.get("signal_uxid"),
          strategy=db_pos.get("strategy") or "",
          symbol=db_pos.get("symbol") or "",
          event=CycleEventSchema(
            action=AdminActionEnum.FLAT.value,
            outcome=CycleOutcomeEnum.ADMIN_FLAT,
            price=result.get("price"),
            volume=result.get("volume"),
            profit=result.get("profit"),
            ref_id=_as_text(result.get("ticket")),
            ref_source_id=_as_text(db_pos.get("ref_source_id")),
            gateway_return_code=result.get("retcode"),
            reason=result.get("comment") or None,
          ),
          status=CycleStatusEnum.FLATTED,
        )
      elif db_keys.isdisjoint(attempted):
        # Never seen live on the broker → already closed externally; sync the DB.
        log.warning(
          "[ADMIN FLAT] %s in DB but not found on %s — marking FLATTED",
          db_pos.get("symbol"),
          self.name,
        )
        self._log_flat_event(db_pos, None, raw)
        self.ctx.db_service.update_position_status(
          ref_source_id=db_pos.get("ref_source_id"),
          status=PositionStatusEnum.FLATTED,
          comment=f"Admin FLAT (position not found on {self.name})",
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

  def _log_flat_event(self, db_pos: dict, result: Optional[dict], raw: str) -> None:
    """Record one ADMIN FLAT outcome in the append-only ``position_logs`` audit
    trail, carrying the raw ADMIN payload.

    This is where the payload lives now that it no longer overwrites the position
    row's stored entry signal. *result* is the successful close, or ``None`` when
    the row was already flat on the broker and is only being synced."""
    self.ctx.db_service.log_position(
      strategy=db_pos.get("strategy"),
      ref_id=(result or {}).get("ticket") or db_pos.get("ref_id"),
      ref_source_id=db_pos.get("ref_source_id"),
      symbol=db_pos.get("symbol"),
      action=PositionStatusEnum.FLATTED.value,
      volume=(result or {}).get("volume") or db_pos.get("volume"),
      price=(result or {}).get("price"),
      sl=None,
      tp1=None,
      gateway_return_code=(result or {}).get("retcode", 0),
      comment=(result or {}).get(
        "comment", f"Admin FLAT (position not found on {self.name})"
      ),
      message=raw,
      author=LogAuthorEnum.BROKER.value,
      market_type=self._market_type,
      signal_id=db_pos.get("signal_id"),
    )

  # ── Shared SYSTEM handling ────────────────────────────────────────────── #

  def _handle_system_message(self, raw: str) -> None:
    """Handle a message received on the NATS ``SYSTEM`` subscription: validate the
    common envelope (action + timestamp + account_id), route it by worker
    identity, then dispatch to the market's :meth:`_handle_system_action` hook.

    This is the **runtime** half of the broker→worker config flow: the worker
    stays subscribed to ``SYSTEM`` for its whole lifetime, so a broadcast here
    always lands — which is what a setting changed *after* the handshake needs
    (e.g. ``CRYPTO_LEVERAGE_INIT`` when an admin edits an account's leverage cap).
    Connect-time config takes the other half, the WORKER_CONNECTED_ACK, because a
    request reply inbox delivers exactly one message.

    An action a market does not understand is logged and ignored rather than
    raising, so a SYSTEM message meant for another market type is harmless here.
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

  def _handle_system_action(self, action: SystemActionEnum, data: dict) -> None:
    """Dispatch a parsed SYSTEM ``action``. Default: log and ignore.

    The base owns no runtime SYSTEM action of its own — the config it manages
    (magic map, signal replay) is connect-time only and arrives in the
    WORKER_CONNECTED_ACK. Markets that do have one override this hook and
    delegate to ``super()`` for anything they don't recognise; CRYPTO handles
    ``CRYPTO_LEVERAGE_INIT``. The base still sees the worker's own
    ``WORKER_CONNECTED`` fanned back out on the shared subject, which lands here
    and is ignored.
    """
    log.info(
      "[%s SYSTEM] No handler for action=%s — ignoring.",
      self.name,
      getattr(action, "value", action),
    )

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
    the control subjects). Shipped in WORKER_CONNECTED so the broker knows which
    strategies to scope the ACK's magic map and signal replay to."""
    return parse_strategy_subjects(self.settings.get("nats_subjects", "") or "")

  def _worker_connected_payload(self) -> Optional[str]:
    """Build the WORKER_CONNECTED handshake JSON, or None if this worker has no
    identity yet (no account_id → nothing for the broker to target)."""
    account_id = self._system_account_id
    if not account_id:
      return None
    return WorkerConnectedSchema(
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

    The broker always replies now (``WORKER_CONNECTED_ACK`` /
    ``WORKER_CONNECTED_ERROR``), so a timeout means the
    broker genuinely didn't get it. The handshake is idempotent and mandatory
    before trading (crypto needs ``default_leverage``), so this blocks and
    retries with backoff until it succeeds rather than giving up.

    The outbound payload is logged at DEBUG before the request is sent, mirroring
    the reply-side logging in :meth:`_handle_worker_connected_response` — the INFO
    line below only prints ``account_id``, so seeing ``market``/``gateway``/
    ``strategies`` the worker actually announced requires ``LOG_LEVEL=DEBUG``.
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

    log.debug("[%s Process] WORKER_CONNECTED request payload: %s", self.name, payload)

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
    """Dispatch the broker's reply to WORKER_CONNECTED.

    ``WORKER_CONNECTED_ACK`` is the normal reply and carries this worker's whole
    connect-time config in one payload (see
    :class:`~worker.schemas.inbox_schema.WorkerConnectedAckSchema`) — a NATS
    request inbox delivers only the first message, so the broker cannot split it
    across several sends.
    ``WORKER_CONNECTED_ERROR`` is logged for operator attention — it signals a
    broker-side config problem (e.g. missing settings), which a retry cannot fix.
    Those two are the only valid replies; anything else means the broker answered
    with something this worker has no contract for.

    The raw reply is logged at DEBUG first, before anything is parsed. A reply
    arrives on the request's private inbox, so it never passes through
    ``NATSSubscriber.listen``'s "Received NATS message" DEBUG line the way a
    subscribed message does — without this, the exact bytes the broker sent are
    invisible, and that is precisely what you need when a section silently fails
    to apply (or the payload doesn't parse at all)."""
    log.debug("[%s Process] WORKER_CONNECTED reply payload: %s", self.name, raw)
    try:
      data = json.loads(raw)
      action = SystemActionEnum(data.get("action"))
    except (json.JSONDecodeError, ValueError) as err:
      log.error("[%s Process] WORKER_CONNECTED reply parse error: %s", self.name, err)
      return

    if action == SystemActionEnum.WORKER_CONNECTED_ERROR:
      error = WorkerConnectedErrorSchema(**data)
      log.error(
        "[%s Process] WORKER_CONNECTED_ERROR: %s",
        self.name,
        error.reason or "(no reason given)",
      )
      return

    if action == SystemActionEnum.WORKER_CONNECTED_ACK:
      self._apply_worker_connected_ack(data)
      return

    log.error(
      "[%s Process] Unexpected WORKER_CONNECTED reply action=%s — ignoring.",
      self.name,
      getattr(action, "value", action),
    )

  def _apply_worker_connected_ack(self, data: dict) -> None:
    """Apply the config carried by a ``WORKER_CONNECTED_ACK``.

    ``settings`` is restored first, before anything can act on it: it carries the
    signal-execution gate, and the replay further down this same method runs
    through ``_process_signal`` like a live signal does — so a block restored
    here suppresses the replay exactly as it suppresses live traffic, while one
    applied afterwards would arrive a batch of orders too late.

    Config sections land **before** the signal replay, which always runs last: a
    replayed FOREX entry is routed by its strategy's magic and a replayed CRYPTO
    entry is sized against the exchange's leverage, so replaying first would fire
    orders against config that hadn't been applied yet. Every section is optional
    — an ACK with none of them is just "handshake complete".

    Ordering alone is not enough, because a section that never arrived is also
    "applied" in order: an ACK that omits ``strategy_magic_map`` (or whose map is
    wholly unparseable) leaves the worker's map untouched, which on a **first**
    connect means empty — ``STRATEGY_MAGIC_MAP`` no longer ships a default. The
    replay would then run against no magics at all and every entry would be
    rejected one-by-one as an unknown strategy. So the replay is additionally
    *gated* on the config it depends on actually being present — see
    :meth:`_replay_blocked_reason`.

    Each section (and each entry within it) is salvaged independently by
    :meth:`~worker.schemas.inbox_schema.WorkerConnectedAckSchema._salvage_sections`,
    so a broker-side payload fault costs only the part that is actually broken.
    Whatever was discarded is logged at ERROR here, one line per entry, so the
    operator sees exactly what the worker is missing. Only a broken *envelope*
    (bad ``account_id``/``timestamp``) drops the ACK wholesale — that leaves
    nothing addressable to apply.
    """
    try:
      ack = WorkerConnectedAckSchema(**data)
    except ValidationError as err:
      log.error(
        "[%s Process] WORKER_CONNECTED_ACK envelope invalid: %s", self.name, err
      )
      return

    for reason in ack.dropped:
      log.error("[%s Process] WORKER_CONNECTED_ACK dropped %s", self.name, reason)

    has_config = (
      ack.settings is not None
      or ack.strategy_magic_map is not None
      or ack.crypto_leverage_init is not None
      or bool(ack.retry_signals)
    )
    if not has_config:
      if not ack.dropped:
        log.info(
          "[%s Process] WORKER_CONNECTED_ACK — handshake complete, no init config needed.",
          self.name,
        )
      return

    log.info(
      "[%s Process] WORKER_CONNECTED_ACK — handshake complete | settings=%s magics=%s "
      "leverage=%s replay=%d",
      self.name,
      "yes" if ack.settings is not None else "-",
      "-" if ack.strategy_magic_map is None else len(ack.strategy_magic_map),
      "yes" if ack.crypto_leverage_init is not None else "-",
      len(ack.retry_signals),
    )
    if ack.settings is not None:
      self._apply_worker_settings(ack.settings)
    if ack.strategy_magic_map is not None:
      self._apply_strategy_magic_map(ack.strategy_magic_map)
    self._apply_market_init(ack)
    if ack.retry_signals:
      blocked = self._replay_blocked_reason()
      if blocked is not None:
        log.error(
          "[%s Process] retry_signals: SKIPPING replay of %d signal(s) — %s. "
          "Replaying now would reject every entry one-by-one; fix the broker-side "
          "config and reconnect the worker to replay them.",
          self.name,
          len(ack.retry_signals),
          blocked,
        )
        return
      self._apply_retry_signals(ack.retry_signals)

  def _apply_worker_settings(self, settings: WorkerSettingsSchema) -> None:
    """Restore the runtime toggles the broker pushed in the ACK's ``settings``
    section, reading each attribute in turn.

    These toggles live in memory only (a restart resets them to their defaults),
    so the broker — which owns the state — re-pushes whatever an operator set in
    an earlier session and the worker replays it here. Each attribute is applied
    through the very same helper the runtime ADMIN action calls, so restoring a
    value is indistinguishable from receiving the directive live: the same
    already-in-that-state no-op, the same log line, the same operator alert.

    A field left ``None`` was not pushed and is skipped — only an explicit value
    changes anything, so the broker can restore one toggle without disturbing the
    rest. Adding a new toggle means adding its field to
    :class:`~worker.schemas.inbox_schema.WorkerSettingsSchema` and one branch
    here that delegates to the existing ADMIN-side setter.
    """
    if settings.signal_blocked is not None:
      # Same setter as ADMIN BLOCK_SIGNAL / ALLOW_SIGNAL: notifies on a real
      # change, no-ops when the worker is already in that state (which a fresh
      # start is for signal_blocked=false).
      self._set_signals_blocked(settings.signal_blocked)

  def _replay_blocked_reason(self) -> Optional[str]:
    """Why this worker must not run the ACK's signal replay, or None to proceed.

    Checked *after* every config section has been applied, so it asks the only
    question that matters at that point: did the config the replay depends on
    actually land? Ordering the sections correctly is not sufficient on its own —
    a section the broker omitted, or one that failed to parse, is skipped in
    order and leaves the worker running on whatever it had before (on a first
    connect: nothing).

    Default: nothing blocks the replay. A market whose execution path *requires*
    broker-pushed config overrides this and returns a reason (FOREX cannot route
    an order without a magic map). Blocking the whole batch rather than letting
    each signal fail individually is deliberate: the outcome is identical (none
    of them can execute) but the operator gets one actionable line naming the
    root cause instead of N per-signal rejections that each name a symptom.
    """
    return None

  def _apply_market_init(  # noqa: B027 - optional hook
    self, ack: WorkerConnectedAckSchema
  ) -> None:
    """Apply the ACK sections only one market understands. Default: no-op.

    The base cannot run these itself — it imports no broker SDK — so a market
    that owns a section overrides this hook and reads its own field off *ack*
    (CRYPTO handles ``crypto_leverage_init``). Called after the shared config and
    before the signal replay, so a replayed order is placed against fully
    initialised market state.
    """

  def _apply_strategy_magic_map(self, pushed: dict[str, int]) -> None:
    """Store the broker's per-strategy magic map (from the WORKER_CONNECTED_ACK)
    into the live settings, replacing the legacy ``STRATEGY_MAGIC_MAP`` .env
    value, so the executor and :class:`PositionCDC` resolve magics from it exactly
    as before. An empty map clears the worker's magics.

    Only entries for strategies this worker actually subscribes to (its
    ``NATS_SUBJECTS`` minus the control subjects) are kept — the map is scoped to
    this worker so it never claims a magic for a strategy it does not trade.

    The ACK is answered during ``connect()``, before ``start_market_jobs()``
    builds the CDC / close-detection jobs, so those jobs read the freshly-stored
    map. The already-built executor is updated in place via
    :meth:`_set_executor_magic_map` so magic resolution and ``owned_magics()``
    reflect the push immediately.
    """
    subscribed = set(self._subscribed_strategies())
    mapping = {
      strategy: magic for strategy, magic in pushed.items() if strategy in subscribed
    }
    ignored = sorted(set(pushed) - subscribed)
    if ignored:
      log.warning(
        "[%s SYSTEM] strategy_magic_map: ignoring %d entr%s for strategies this "
        "worker does not subscribe to: %s",
        self.name,
        len(ignored),
        "y" if len(ignored) == 1 else "ies",
        ", ".join(ignored),
      )

    self.settings["strategy_magic_map"] = mapping
    self._set_executor_magic_map(mapping)
    log.info(
      "[%s SYSTEM] strategy_magic_map applied | strategies=%d", self.name, len(mapping)
    )

  def _set_executor_magic_map(self, mapping: dict) -> None:  # noqa: B027 - optional hook
    """Propagate an updated strategy→magic map to the already-built executor.

    Default no-op: a market with no magic concept (e.g. CRYPTO) resolves nothing
    on the executor and its :class:`PositionCDC` reads the map straight from
    settings, so storing it there is enough. FOREX overrides this to refresh the
    executor's in-memory map (magic resolution + ``owned_magics()``)."""

  def _apply_retry_signals(self, signals: list[SignalSchema]) -> None:
    """Execute the ACK's signal replay: for each signal, drop it if already
    processed (dedup by ``signal_id`` against ``position_logs``) or older than
    :data:`~worker.settings.MAX_RETRY_TIMEOUT` (against the signal's own
    ``timestamp``); otherwise run it through the normal signal pipeline.

    A single failing signal in an otherwise valid batch does not abort the rest —
    the goal of a replay is to fill gaps, so each entry stands on its own.
    """
    if not signals:
      log.info("[%s SYSTEM] retry_signals: empty batch — nothing to do.", self.name)
      return

    now = datetime.now(timezone.utc)
    executed = skipped_dedup = skipped_stale = failed = 0

    for signal in signals:
      # Dedup first (cheap DB lookup) so a replay of a signal we already
      # processed never re-hits the broker even if it's still within the window.
      if signal.signal_id and self.ctx.db_service.signal_exists(signal.signal_id):
        skipped_dedup += 1
        log.info(
          "[%s SYSTEM] retry_signals: skip signal_id=%s (already processed).",
          self.name,
          signal.signal_id,
        )
        continue

      age = _seconds_since(signal.timestamp, now)
      if age is None or age > MAX_RETRY_TIMEOUT:
        skipped_stale += 1
        log.info(
          "[%s SYSTEM] retry_signals: skip signal_id=%s symbol=%s action=%s — "
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
          "[%s SYSTEM] retry_signals: signal_id=%s failed — continuing batch.",
          self.name,
          signal.signal_id,
        )

    log.info(
      "[%s SYSTEM] retry_signals done | executed=%d dedup=%d stale=%d failed=%d total=%d",
      self.name,
      executed,
      skipped_dedup,
      skipped_stale,
      failed,
      len(signals),
    )

  # ── Signal-cycle notifications ────────────────────────────────────────── #
  #
  # Every action of one trade folds into a single channel message that is
  # rewritten in place (see
  # :mod:`worker.services.cycle_notification_service`). The per-action message
  # each call site already builds stays the fallback: a broker that sends no
  # ``signal_uxid``, a cycle write that failed, or cycles turned off all mean
  # there is no message to edit, and the action must still be reported.

  def _notify_trade(
    self,
    signal: SignalSchema,
    message: str,
    *,
    event: "CycleEventSchema",
    status: Optional[CycleStatusEnum] = None,
  ) -> None:
    """Report one executed signal — through its cycle, or on its own."""
    self._notify_cycle_or_send(
      message,
      signal_uxid=signal.signal_uxid,
      strategy=signal.strategy,
      symbol=signal.symbol,
      event=event,
      status=status,
    )

  def _notify_cycle_or_send(
    self,
    message: str,
    *,
    signal_uxid: Optional[str],
    strategy: str,
    symbol: str,
    event: "CycleEventSchema",
    status: Optional[CycleStatusEnum] = None,
  ) -> None:
    """Fold *event* into its cycle, falling back to sending *message* as-is.

    Split from :meth:`_notify_trade` because not every cycle action comes from a
    signal — an admin FLAT closes a position the operator named, and takes its
    cycle key off the position row instead.
    """
    if self.ctx.cycle_notifier.record(
      signal_uxid=signal_uxid,
      strategy=strategy,
      symbol=symbol,
      event=event,
      status=status,
    ):
      return
    self.ctx.channel_notifier.send_message(message)

  def _cycle_event(
    self, signal: SignalSchema, result: dict, outcome: CycleOutcomeEnum
  ) -> "CycleEventSchema":
    """Build the timeline entry for one executed signal.

    Levels are taken from the signal (what was asked for) while price/volume come
    from the result (what actually happened), so a fill that slipped shows both
    sides of the story.
    """
    risk_info = self._resolve_risk_info(signal)
    return CycleEventSchema(
      action=signal.action.value,
      outcome=outcome,
      timestamp=signal.timestamp,
      price=result.get("price", signal.price),
      volume=result.get("volume", signal.quantity),
      sl=signal.sl,
      tp1=signal.tp1,
      tp2=signal.tp2,
      risk_percent=risk_info[0] if risk_info else None,
      risk_custom=bool(risk_info[1]) if risk_info else False,
      auto_volume=not self.config.uses_payload_quantity(
        getattr(signal, "use_equity_sizing", None)
      ),
      tp1_percent=self._cycle_tp1_percent(signal),
      is_scale_position=bool(getattr(signal, "is_scale_position", False)),
      # Only a close books money. An entry carries no PnL at all, so it stays
      # None and the timeline omits the line rather than claiming a 0.00 result
      # — the same rule BaseMessagePresenter._exit_pnl_line applies.
      profit=result.get("profit") if signal.action in _CYCLE_EXIT_ACTIONS else None,
      ref_id=_as_text(result.get("ticket")),
      ref_source_id=_as_text(result.get("source_ticket") or result.get("ticket")),
      gateway_return_code=result.get("retcode"),
      reason=result.get("comment") or None,
    )

  def _cycle_tp1_percent(self, signal: SignalSchema) -> Optional[float]:
    """Percent of the position a TP1 closed, mirroring the executor's own choice.

    Only TP1 sizes itself by percent, and only under VOLUME_DECISION_ENABLED —
    otherwise the close is sized from ``signal.quantity`` and no percent applies.
    """
    if signal.action != SignalActionEnum.TP1:
      return None
    if not self.settings.get("volume_decision_enabled", False):
      return None
    if self.settings.get("use_custom_position_tp1_percent", False):
      return self.settings.get("position_tp1_percent")
    if signal.tp1_percent is not None:
      return signal.tp1_percent
    return self.settings.get("position_tp1_percent")

  def _cycle_status(
    self, signal: SignalSchema, result: dict
  ) -> Optional[CycleStatusEnum]:
    """The position's status after this action — the cycle's headline.

    A branch-for-branch mirror of what :meth:`_persist_success` writes to the
    positions table, so the message and the row can never disagree. A failed
    action returns ``FAILED``, which
    :func:`~worker.schemas.cycle_schema.merge_status` keeps from overwriting a
    position that is still open.
    """
    if not result.get("success"):
      return CycleStatusEnum.FAILED
    if signal.action.value in ("LONG", "SHORT"):
      return CycleStatusEnum.OPENED
    # A breakeven SL that could not be placed leaves the remaining volume
    # unprotected, so it is emergency-closed — fully flat, not a still-open
    # partial. If that close itself failed the position is still live, and the
    # status its action implies is the honest one (the message shouts about it).
    failsafe = result.get("sl_failsafe_close")
    if failsafe is not None and failsafe.get("success"):
      return CycleStatusEnum.FORCED_CLOSED
    return CycleStatusEnum.from_position_status(
      _CLOSE_STATUS_MAP.get(signal.action.value)
    )

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _market_type_value(mt) -> str:
    return mt.value if isinstance(mt, MarketTypeEnum) else str(mt or "")

  def _resolve_risk_info(self, signal: SignalSchema):
    """Return ``(risk_percent, is_custom)`` for entry signals the worker sizes
    itself (see ``ExecutionConfig.uses_payload_quantity``), else None.

    ``is_custom`` is True when USE_CUSTOM_RISK_PERCENTAGE overrides the signal's
    own risk_percent (so the gear icon is shown in notifications).
    """
    if signal.action.value not in (
      "LONG",
      "SHORT",
    ) or self.config.uses_payload_quantity(getattr(signal, "use_equity_sizing", None)):
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
