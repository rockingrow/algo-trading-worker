"""
worker/gateways/system_messages.py
──────────────────────────────────
SYSTEM subject handling for :class:`~worker.gateways.processor.BaseSignalProcessor`:
the ``WORKER_CONNECTED`` handshake and its ACK (settings, strategy magics,
market init, signal replay), plus the runtime ``WORKER_SETTINGS_UPDATE`` and
market pushes.

A mixin for the same reason as :mod:`worker.gateways.admin_commands`: the ACK
mutates the processor's settings, executor and signal gate. The handshake is
the one part that runs off the listener thread (a publisher reconnect), so it
applies the ACK under the processor's ``_pipeline_lock``.
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from typing import Optional

from pydantic import ValidationError

from worker.logger import get_logger
from worker.schemas.inbox_schema import (
  WorkerConnectedAckSchema,
  WorkerConnectedErrorSchema,
  WorkerConnectedSchema,
  WorkerSettingsSchema,
)
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.signal_schema import SignalSchema
from worker.schemas.system_schema import (
  SystemActionEnum,
  SystemSchema,
  SystemWorkerSettingsUpdateSchema,
)
from worker.settings import MAX_RETRY_TIMEOUT

# Same logger as the processor: these methods are part of its pipeline,
# and operators filter its log lines by this one name.
log = get_logger("worker.gateways.processor")

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


def _seconds_since(ts: Optional[datetime], now: datetime) -> Optional[float]:
  """Seconds between *ts* and *now*, treating a naive *ts* as UTC. Returns
  None when *ts* is missing so the signal-replay handler can drop a payload
  with no timestamp instead of guessing an age."""
  if ts is None:
    return None
  if ts.tzinfo is None:
    ts = ts.replace(tzinfo=timezone.utc)
  return (now - ts).total_seconds()


class SystemMessagesMixin:
  """SYSTEM subject: the WORKER_CONNECTED handshake and runtime pushes."""

  # ── Shared SYSTEM handling ────────────────────────────────────────────── #

  def _handle_system_message(self, raw: str) -> None:
    """Handle a message received on the NATS ``SYSTEM`` subscription: validate the
    common envelope (action + timestamp + account_id), route it by worker
    identity, then dispatch to the market's :meth:`_handle_system_action` hook.
    Whether the platform has to be connected is each action's own call, not this
    subject's.

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

    # Route by worker identity: every SYSTEM message carries a required
    # account_id (NATS-name format "<market>-<gateway>-<account_id>") and is
    # executed only by the matching worker. A blank value would match nobody.
    # Checked before anything with a side effect, so a broadcast addressed to
    # another account costs this worker nothing.
    if system.account_id and system.account_id != self._system_account_id:
      log.info(
        "[%s SYSTEM] Skipping action=%s: account_id=%s != worker=%s",
        self.name,
        getattr(system.action, "value", system.action),
        system.account_id,
        self._system_account_id,
      )
      return

    # No platform check for the subject as a whole: an action that needs a live
    # broker asks for one itself (CRYPTO_LEVERAGE_INIT does), the way the ADMIN
    # path gates FLAT alone and lets BLOCK_SIGNAL through. The settings this
    # subject carries are in-memory state that does not touch the platform, and
    # the broker does not re-send until the next handshake — so dropping a push
    # over a momentary disconnect would leave the worker running on the old cap,
    # or trading under a block an operator had set, until it reconnects.
    self._handle_system_action(system.action, data)

  def _handle_system_action(self, action: SystemActionEnum, data: dict) -> None:
    """Dispatch a parsed SYSTEM ``action``. Default: log and ignore.

    The base owns one runtime action, ``WORKER_SETTINGS_UPDATE``, because the
    settings it carries are market-neutral — the same block both markets already
    receive in their WORKER_CONNECTED_ACK. Everything else it manages (magic
    map, signal replay) is connect-time only and arrives in that ACK. Markets
    with a runtime action of their own override this hook and delegate to
    ``super()`` for anything they don't recognise; CRYPTO handles
    ``CRYPTO_LEVERAGE_INIT``. The base still sees the worker's own
    ``WORKER_CONNECTED`` fanned back out on the shared subject, which lands here
    and is ignored.
    """
    if action == SystemActionEnum.WORKER_SETTINGS_UPDATE:
      self._apply_system_settings_update(data)
      return
    log.info(
      "[%s SYSTEM] No handler for action=%s — ignoring.",
      self.name,
      getattr(action, "value", action),
    )

  def _apply_system_settings_update(self, data: dict) -> None:
    """Apply a runtime ``WORKER_SETTINGS_UPDATE``.

    The runtime half of the ACK's ``settings`` section: an owner changed a value
    from the Telegram bot (or an admin changed a broker-wide default) while this
    worker was connected, so the new block arrives here instead of waiting for
    the next handshake. It is handed to the very same
    :meth:`_apply_worker_settings` the ACK goes through, so a value behaves
    identically whichever path it arrives on — and the broker is still the
    source of truth either way, re-sending it on every connect.

    Fields are salvaged one by one, so a malformed value costs only its own
    setting; each discard is logged. A push whose whole block was unusable
    leaves every setting as it is, which is the same outcome as no push at all.
    """
    try:
      update = SystemWorkerSettingsUpdateSchema(**data)
    except ValidationError as err:
      log.error(
        "[%s SYSTEM] WORKER_SETTINGS_UPDATE envelope invalid: %s", self.name, err
      )
      return

    for reason in update.dropped:
      log.error("[%s SYSTEM] WORKER_SETTINGS_UPDATE dropped %s", self.name, reason)

    if update.settings is None:
      log.warning(
        "[%s SYSTEM] WORKER_SETTINGS_UPDATE carried no usable setting — "
        "nothing changed.",
        self.name,
      )
      return

    self._apply_worker_settings(update.settings)

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
    """Run the WORKER_CONNECTED handshake, at most one at a time.

    Called at connect and from every publisher reconnect (on a thread of its
    own). A call that arrives while a handshake is in flight does not start a
    second one; it marks the running one to go again once it finishes, so a
    broker that restarted mid-handshake is still re-announced to.
    """
    with self._announce_state:
      if self._announce_running:
        self._announce_pending = True
        log.info(
          "[%s Process] WORKER_CONNECTED already in flight — will re-announce "
          "once it completes.",
          self.name,
        )
        return
      self._announce_running = True
    while True:
      try:
        self._announce_once()
      except Exception:
        with self._announce_state:
          rerun = self._announce_pending
          self._announce_pending = False
          if not rerun:
            self._announce_running = False
        if not rerun:
          raise
        # A reconnect asked for a fresh announce while this one failed: honour
        # it rather than drop it with the error.
        log.exception(
          "[%s Process] WORKER_CONNECTED failed — re-announcing for the reconnect "
          "that arrived meanwhile.",
          self.name,
        )
        continue
      with self._announce_state:
        if not self._announce_pending:
          self._announce_running = False
          return
        self._announce_pending = False

  def _announce_once(self) -> None:
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
      # Applied under the pipeline lock: the ACK may replay signals, and this can
      # be the reconnect thread while the listener is executing one.
      with self._pipeline_lock:
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
    if settings.max_open_orders is not None:
      self._set_max_open_orders(settings.max_open_orders)

  def _set_max_open_orders(self, max_open_orders: int) -> None:
    """Store the broker's exposure cap, which the entry guard reads per signal.

    It goes into the live settings dict rather than onto an attribute because
    that dict is what ``guard.max_open_orders_rejection`` is handed on every
    entry — the same place the setting used to arrive from ``.env``, so nothing
    downstream has to change or learn where the value came from.

    Logged on a real change only. No operator alert: unlike a signal block this
    does not stop the worker trading, and the cap is already printed in the
    startup message and every rejection line.
    """
    current = self.settings.get("max_open_orders")
    if current == max_open_orders:
      log.info("[SYSTEM] MAX_OPEN_ORDERS already %s — no change.", max_open_orders)
      return
    self.settings["max_open_orders"] = max_open_orders
    log.warning(
      "[SYSTEM] MAX_OPEN_ORDERS now %s for this worker (was %s).",
      max_open_orders,
      current,
    )

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
