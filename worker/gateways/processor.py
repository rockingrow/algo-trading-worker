"""
worker/gateways/processor.py
────────────────────────────
Market-agnostic signal-processor skeleton (Template Method pattern).

Both the FOREX (MT5) and CRYPTO (CEX) gateways run the *same* algorithm:

    connect → subscribe to NATS → for each message:
        ADMIN  → validate + reconcile a FLAT
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
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pydantic import ValidationError

from worker.context import WorkerContext
from worker.gateways.config import ExecutionConfig
from worker.gateways.market_strategy import MarketStrategyFactory
from worker.gateways.signal_handler import SignalHandler
from worker.interfaces.trade_presenter_protocol import TradePresenterProtocol
from worker.logger import get_logger
from worker.schemas.admin_schema import (
  AdminActionEnum,
  AdminFlatSchema,
  AdminMessageSchema,
)
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalSchema
from worker.schemas.system_schema import (
  SystemActionEnum,
  SystemSchema,
  SystemWorkerConnectedSchema,
)
from worker.services.nats_service import NATSPublisher, NATSSubscriber
from worker.settings import NATS_REQUIRED_LISTENING_SUBJECTS, MarketTypeEnum

log = get_logger("worker.gateways.processor")

# How long an account footer is reused before refetching. Signals can arrive in
# bursts; fetching the live account (a REST round-trip for a CEX) once per signal
# adds avoidable latency/rate-limit pressure, and a few-seconds-stale balance in a
# notification is harmless.
_FOOTER_TTL = 30  # seconds

# Exit action → DB status, shared by every market.
_CLOSE_STATUS_MAP: Dict[str, PositionStatusEnum] = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
  "FLAT": PositionStatusEnum.FLATTED,
}


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

  # ── Shared lifecycle ──────────────────────────────────────────────────── #

  def connect(self) -> bool:
    if not self._connect_broker():
      log.error("[%s Process] Could not connect to broker. Exiting.", self.name)
      return False

    _tok = self.settings.get("nats_token")
    nats_token = _tok.get_secret_value() if _tok is not None else None

    self.subscriber = NATSSubscriber(
      url=self.settings["nats_url"],
      subjects=parse_nats_subjects(self.settings.get("nats_subjects", "")),
      publish_subjects=[NatsSubjectEnum.TRADE, NatsSubjectEnum.ACK], # purpose just only show on Notification
      token=nats_token,
      account_id=self.settings.get("account_id"),
      account_footer_fn=self._account_footer,
      enqueue_fn=self.ctx.nats_enqueue,
    )
    self.subscriber.connect()

    self.publisher = NATSPublisher(
      url=self.settings["nats_url"],
      publish_subjects=[NatsSubjectEnum.TRADE, NatsSubjectEnum.ACK, NatsSubjectEnum.SYSTEM],
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
          self.name, raw,
        )

  # ── Shared message processing ─────────────────────────────────────────── #

  def _process_message(self, subject, raw) -> None:
    if subject == NatsSubjectEnum.ADMIN:
      self._handle_admin_message(raw)
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

    # Scale-in (averaging): the broker has already scaled SL/TP1/TP2/quantity in
    # the payload, so every downstream step (SL/TP placement, persistence,
    # notifications) consumes them verbatim. The only re-derivation happens inside
    # the executor when VOLUME_DECISION sizes the entry from risk — see
    # SignalSchema.scale_quantity_factor. Log the broker-reported multipliers for
    # traceability.
    if signal.is_scale_position:
      log.info(
        "[%s Process] Scale-in position | %s | scaling=%s | sl=%s tp1=%s tp2=%s qty=%s",
        self.name, signal.symbol, signal.scaling,
        signal.sl, signal.tp1, signal.tp2, signal.quantity,
      )

    log.info(
      "[%s Process] Processing Signal: %s | %s | TV Time: %s",
      self.name, signal.symbol, signal.action.value, signal.timestamp,
    )

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
          signal, result, result.get("source_ticket") or result.get("ticket"), footer,
          risk_info=risk_info, settings_dict=self.settings,
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

  # ── Shared ADMIN FLAT handling ────────────────────────────────────────── #

  def _handle_admin_message(self, raw: str) -> None:
    """Handle a NATS ADMIN ``FLAT``: parse, route by account, close live broker
    positions, then reconcile the DB.

    The algorithm is identical for every market; only *how a live close maps to a
    DB row* differs (MT5 matches by ticket, a CEX by resolved symbol). That single
    variation point is the :meth:`_flat_match_key` / :meth:`_flat_db_match_keys`
    pair, so the broker is always the source of truth: a row is marked ``FLATTED``
    only when its close actually succeeded *or* the position was never live on the
    broker. A row whose close was **attempted but failed** is left OPEN (it is
    still live), so the DB never claims a position is flat while it is not.
    """
    try:
      data = json.loads(raw)
      admin = AdminMessageSchema(**data)
    except (json.JSONDecodeError, ValidationError) as err:
      log.error("[ADMIN] Parse error: %s", err)
      return

    if not self._ensure_connected():
      return

    if admin.action == AdminActionEnum.FLAT:
      admin_flat_schema = AdminFlatSchema(**data)
      if admin_flat_schema.account_id and admin_flat_schema.account_id != self._account_id:
        log.info(
          "[ADMIN FLAT] Skipping: account_id=%s != worker account=%s",
          admin_flat_schema.account_id, self._account_id,
        )
        return
      # Close live broker positions first (source of truth), then reconcile the DB.
      closed, attempted = self._close_live_positions_for_flat(admin_flat_schema)
      self._reconcile_flat_db(admin_flat_schema, closed, attempted, raw)
      return

    log.warning("[ADMIN] Unknown action: %s", admin.action)
    return

  def _close_live_positions_for_flat(self, admin) -> tuple[Dict[Any, dict], set]:
    """Close every live broker position matching the FLAT filter.

    Returns ``(closed, attempted)``, both keyed by :meth:`_flat_match_key` —
    ``closed`` maps the key to its successful close result; ``attempted`` is every
    key we tried (so the reconcile step can tell a failed close from one that was
    never live).
    """
    if admin.symbol:
      positions = self.executor.get_open_positions(admin.symbol, strategy=admin.strategy)
    else:
      positions = self.executor.get_all_open_positions(strategy=admin.strategy)

    if positions:
      log.info(
        "[ADMIN FLAT] Closing %d %s position(s) (strategy=%s, symbol=%s)",
        len(positions), self.name, admin.strategy, admin.symbol,
      )
    else:
      log.warning(
        "[ADMIN FLAT] No open %s positions (strategy=%s, symbol=%s)",
        self.name, admin.strategy, admin.symbol,
      )

    attempted: set = set()
    closed: Dict[Any, dict] = {}
    for pos in positions:
      key = self._flat_match_key(pos)
      attempted.add(key)
      result = self.executor.close_single_position(pos, reason="FLAT")
      if result.get("success"):
        closed[key] = result
        log.info("[ADMIN FLAT] Closed %s key=%s vol=%s", self.name, key, result.get("volume"))
      else:
        log.error("[ADMIN FLAT] Failed to close %s key=%s: %s", self.name, key, result.get("comment"))
    return closed, attempted

  def _reconcile_flat_db(self, admin, closed: Dict[Any, dict], attempted: set, raw: str) -> None:
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
          db_pos.get("symbol"), self.name,
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
          db_pos.get("symbol"), self.name,
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

    if not self._ensure_connected():
      return

    # Route by worker identity: every SYSTEM message carries a required
    # account_id (NATS-name format "<market>-<gateway>-<account_id>") and is
    # executed only by the matching worker. A blank value would match nobody.
    if system.account_id and system.account_id != self._system_account_id:
      log.info(
        "[%s SYSTEM] Skipping action=%s: account_id=%s != worker=%s",
        self.name, getattr(system.action, "value", system.action),
        system.account_id, self._system_account_id,
      )
      return

    self._handle_system_action(system.action, data)

  @property
  def _system_account_id(self) -> Optional[str]:
    """This worker's identity for SYSTEM routing, in NATS-name format
    ``<market>-<gateway>-<account_id>`` (matches the NATS connection name; see
    Settings._validate_market_requirements)."""
    return self.settings.get("account_id") or None

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
    ).model_dump_json()

  def _announce_worker_connected(self) -> None:
    """Publish a SYSTEM ``WORKER_CONNECTED`` so the broker can push any initial
    config targeted at this worker. Waits for the SYSTEM subscription to be live
    first — NATS core does not replay, so a reply that arrives before we are
    subscribed would be lost."""
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
        "announcing WORKER_CONNECTED anyway (reply may be missed).", self.name
      )
    self.publisher.publish(NatsSubjectEnum.SYSTEM, payload)
    log.info(
      "[%s Process] Announced WORKER_CONNECTED account_id=%s",
      self.name, self._system_account_id,
    )

  def _handle_system_action(self, action: SystemActionEnum, data: dict) -> None:
    """Dispatch a parsed SYSTEM ``action``. Default: log and ignore.

    Markets override this to handle the actions they support; the base ignores
    every action so a market without SYSTEM actions (e.g. FOREX) needs no code.
    """
    log.info(
      "[%s SYSTEM] No handler for action=%s — ignoring.",
      self.name, getattr(action, "value", action),
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
    if signal.action.value not in ("LONG", "SHORT") or not self.config.volume_decision_enabled:
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
