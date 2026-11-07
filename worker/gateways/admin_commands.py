"""
worker/gateways/admin_commands.py
─────────────────────────────────
ADMIN subject handling for :class:`~worker.gateways.processor.BaseSignalProcessor`:
``FLAT`` on the public and private subjects, and the private
``BLOCK_SIGNAL`` / ``ALLOW_SIGNAL`` toggles.

A mixin rather than a collaborator object because every step reads the
processor's own state and hooks — its identity (market / gateway / account),
the executor, the DB service, the presenter and the market's
``_flat_match_key`` / ``_flat_db_match_keys`` pair. Passing all of that to a
separate object would only rebuild the processor under another name. What the
split buys is a module per NATS subject: the processor keeps the lifecycle and
the signal path, and each subject's rules can be read on their own.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from pydantic import ValidationError

from worker.gateways.errors import PositionsUnavailableError
from worker.gateways.trade_notifications import _as_text
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
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.position_schema import PositionStatusEnum

# Same logger as the processor: these methods are part of its pipeline,
# and operators filter its log lines by this one name.
log = get_logger("worker.gateways.processor")


class AdminCommandsMixin:
  """ADMIN ``FLAT`` / ``BLOCK_SIGNAL`` / ``ALLOW_SIGNAL`` for the signal processor."""

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
    try:
      closed, attempted = self._close_live_positions_for_flat(admin_flat_schema)
    except PositionsUnavailableError as exc:
      # Without the live book every DB row would look "never live" and be marked
      # FLATTED. Leave the DB untouched; the broker can resend the FLAT.
      log.error("[ADMIN FLAT] Aborted, DB left unchanged — %s", exc)
      return
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
    # A market whose live positions carry no strategy (a CEX: no magic number)
    # returns every position on the account whatever ``strategy`` asked for, so
    # a strategy-filtered FLAT is scoped through the DB rows that strategy owns
    # — the same way a ref_id-targeted FLAT is. Without it, FLAT {strategy: A}
    # also closed strategy B's and the manually opened positions.
    scope_by_db = bool(ref_id) or (
      bool(admin.strategy) and not self._live_positions_carry_strategy
    )
    if scope_by_db:
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
