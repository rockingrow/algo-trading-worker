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

The per-subject rules live in mixins, one module each, so this file keeps only
the lifecycle, the dispatch and the SIGNAL path:

* :mod:`worker.gateways.admin_commands` — ADMIN ``FLAT`` / ``BLOCK_SIGNAL`` /
  ``ALLOW_SIGNAL``;
* :mod:`worker.gateways.system_messages` — SYSTEM, the ``WORKER_CONNECTED``
  handshake and its ACK;
* :mod:`worker.gateways.trade_notifications` — the per-trade cycle messages.

Every message, and the ACK applied from the reconnect thread, runs under
``_pipeline_lock``, so the entry guards and the signal dedup never race.

The base imports no broker SDK (no MetaTrader5, no exchange client), so it is
safe to import on any market's path.
"""

from __future__ import annotations

import json
import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any, Dict, Optional

from pydantic import ValidationError

from worker.context import WorkerContext
from worker.gateways import guard
from worker.gateways.admin_commands import AdminCommandsMixin
from worker.gateways.config import ExecutionConfig
from worker.gateways.errors import PositionsUnavailableError
from worker.gateways.market_strategy import MarketStrategyFactory
from worker.gateways.signal_handler import SignalHandler
from worker.gateways.system_messages import SystemMessagesMixin
from worker.gateways.trade_notifications import (
  _CLOSE_STATUS_MAP,
  TradeNotificationsMixin,
  _as_text,
)
from worker.interfaces.trade_presenter_protocol import TradePresenterProtocol
from worker.logger import get_logger
from worker.schemas.cycle_schema import (
  CycleEventSchema,
  CycleOutcomeEnum,
  CycleStatusEnum,
)
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalActionEnum, SignalSchema
from worker.schemas.trade_result import TradeResult
from worker.services.nats_service import NATSPublisher, NATSSubscriber
from worker.settings import (
  NATS_REQUIRED_LISTENING_SUBJECTS,
  MarketTypeEnum,
)

log = get_logger("worker.gateways.processor")

# How long an account footer is reused before refetching. Signals can arrive in
# bursts; fetching the live account (a REST round-trip for a CEX) once per signal
# adds avoidable latency/rate-limit pressure, and a few-seconds-stale balance in a
# notification is harmless.
_FOOTER_TTL = 30  # seconds

# How many successfully executed signal_ids the live dedup remembers. A replay
# only reaches back MAX_RETRY_TIMEOUT, so this comfortably covers any burst.
_EXECUTED_IDS_MAX = 5000

# Entry actions that open a fresh position — the only ones the MAX_OPEN_ORDERS
# cap applies to (exits must always be allowed so positions can be closed).
_ENTRY_ACTIONS = (SignalActionEnum.LONG, SignalActionEnum.SHORT)


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


class BaseSignalProcessor(
  AdminCommandsMixin, SystemMessagesMixin, TradeNotificationsMixin, ABC
):
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
  #: Serialises the NATS pipeline: each NATS message, the WORKER_CONNECTED_ACK
  #: (whose replay executes signals), and the markets' background handlers that
  #: write the positions table (wrapped with :meth:`_serialized`).
  #: The ACK can land on the reconnect thread while the listener thread is mid
  #: signal; without one lock the dedup and entry guards are check-then-act
  #: races and a signal can be placed twice. Re-entrant because the connect-time
  #: ACK is applied from the thread that later runs the loop. (Class-level
  #: default for instances that bypass ``__init__`` in tests.)
  _pipeline_lock = threading.RLock()
  _announce_state = threading.Lock()
  _announce_running: bool = False
  _announce_pending: bool = False
  #: Whether ``executor.get_*open_positions(strategy=…)`` really filters by
  #: strategy. FOREX does (one magic per strategy); a CEX does not, and its
  #: processor sets this False so ADMIN FLAT scopes through the DB instead.
  _live_positions_carry_strategy: bool = True

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
    # restart resets it to False (unblocked). Read and written only under
    # _pipeline_lock (the listener loop, or an ACK being applied).
    self._signals_blocked: bool = False
    self._pipeline_lock = threading.RLock()
    self._executed_signal_ids: "OrderedDict[str, None]" = OrderedDict()
    # Single-flight WORKER_CONNECTED: a reconnect storm must not start one
    # forever-retrying handshake per reconnect. A reconnect that arrives while
    # one is in flight sets _announce_pending, and the running one goes again.
    self._announce_state = threading.Lock()
    self._announce_running = False
    self._announce_pending = False

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
        with self._pipeline_lock:
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
    if self._signal_skipped(signal):
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

    result = self._execute(signal)
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
      self._remember_executed(signal.signal_id)
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

  def _signal_skipped(self, signal: SignalSchema) -> bool:
    """Apply the gates that drop a signal before anything else happens, logging
    (and where useful alerting) why. True when the signal must not run."""
    # signal_id is unique per action. One this process already executed
    # successfully — live or from the ACK replay — must not place a second order
    # when it is delivered again. Only successes are remembered: a failed
    # attempt stays retryable, which a check against position_logs (where every
    # attempt is written) would not allow.
    if signal.signal_id and signal.signal_id in self._executed_ids():
      log.warning(
        "[%s Process] Signal SKIPPED — signal_id=%s already executed | %s | %s",
        self.name,
        signal.signal_id,
        signal.symbol,
        signal.action.value,
      )
      return True

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
      return True

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
      return True
    return False

  def _execute(self, signal: SignalSchema) -> TradeResult:
    """Run *signal* through the handler and normalise the result."""
    try:
      result = self.handler.handle(signal)
    except PositionsUnavailableError as exc:
      # The broker could not say what is live, so nothing was decided or sent:
      # report a plain failure instead of letting a guess ("nothing is open")
      # drive the exit or entry logic.
      log.error(
        "[%s Process] Signal not executed — %s | %s | %s",
        self.name,
        exc,
        signal.symbol,
        signal.action.value,
      )
      return TradeResult.fail(f"Broker positions unavailable: {exc}")
    # Some exchanges (notably Binance testnet) return a 0 fill price on a filled
    # MARKET order — for entries *and* closes (FLAT/TP2/SL/...). Fall back to the
    # signal's price so the notification and DB never record a misleading 0.0.
    # NB: result.get("price", signal.price) does NOT help — the key is present
    # but zero, not missing. This is the single funnel for every signal action,
    # so the fallback belongs here rather than in each executor close path (which
    # has no access to the signal anyway).
    if result.get("success") and not result.get("price") and signal.price:
      result["price"] = signal.price
    return result

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

  def _serialized(self, fn):
    """Wrap a background callback that writes the positions table so it runs
    under :attr:`_pipeline_lock`.

    The entry guards read the table and then place the order; a reconciler
    importing a manual position, or a close event, landing in between would let
    an entry through that the one-order-per-symbol or MAX_OPEN_ORDERS rule
    should refuse. Markets hand their reconcile / push-stream handlers through
    this so those writes queue behind the signal being processed.
    """

    def _locked(*args, **kwargs):
      with self._pipeline_lock:
        return fn(*args, **kwargs)

    return _locked

  def _executed_ids(self) -> "OrderedDict[str, None]":
    # Per instance, created on first use for instances that bypass __init__ in
    # tests — a class-level default would be shared between them.
    return self.__dict__.setdefault("_executed_signal_ids", OrderedDict())

  def _remember_executed(self, signal_id: Optional[str]) -> None:
    """Record a successfully executed signal_id, keeping the newest
    :data:`_EXECUTED_IDS_MAX` (a replay only reaches back MAX_RETRY_TIMEOUT)."""
    if not signal_id:
      return
    ids = self._executed_ids()
    ids[signal_id] = None
    ids.move_to_end(signal_id)
    while len(ids) > _EXECUTED_IDS_MAX:
      ids.popitem(last=False)

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
       already held by *this* strategy blocks, and/or several signals per
       symbol (FOREX_ALLOW_MULTI_POSITIONS_PER_SYMBOL), in which case only a
       position already held by *this* ``signal_uxid`` blocks.
    2. **MAX_OPEN_ORDERS** — the worker's exposure cap. Account-wide whatever
       those toggles allow: it counts every active position, so concurrent
       signals on one symbol consume the same slots as signals spread across
       symbols.
    3. **Staleness** — needs a live quote from the broker (a tick read / REST
       round-trip), so it runs last: no reason to pay for one on an entry the
       two DB-only guards above already rejected.
    """
    market = getattr(self.handler, "strategy", None)
    allow_multi_strategy = bool(
      getattr(market, "allows_multi_strategy_per_symbol", False)
    )
    allow_multi_positions = bool(
      getattr(market, "allows_multi_positions_per_symbol", False)
    )
    reason = guard.symbol_open_rejection(
      self.ctx.db_service,
      signal,
      allow_multi_strategy=allow_multi_strategy,
      allow_multi_positions=allow_multi_positions,
    )
    if reason is not None:
      return reason

    reason = guard.max_open_orders_rejection(
      self.ctx.db_service,
      self.settings,
      signal,
      allow_multi_positions=allow_multi_positions,
    )
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

  # ── Worker identity ───────────────────────────────────────────────────── #

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
