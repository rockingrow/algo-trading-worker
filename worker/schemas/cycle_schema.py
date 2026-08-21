"""
worker/schemas/cycle_schema.py
──────────────────────────────
Vocabulary of a *signal cycle* notification: the status a cycle advertises and
the shape of one entry in its timeline.

A cycle is every action of a single trade — the LONG/SHORT entry and the
TP1/TP2/SL/R_SL/FLAT that follow it — tied together by the broker's
``signal_uxid`` (see :class:`~worker.schemas.signal_schema.SignalSchema`). The
worker folds all of them into one Telegram message that is rewritten in place
as the trade progresses, so these types describe what that message stores
rather than what the executor does.

Kept as pure schema (no I/O, no SQL) so the repository, the presenter and the
recorder all read the same definitions.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from worker.schemas.position_schema import PositionStatusEnum


class CycleStatusEnum(str, Enum):
  """Latest state of the position a cycle tracks — the headline of the message.

  Every :class:`~worker.schemas.position_schema.PositionStatusEnum` value has a
  member here with the same name, so a persisted position status converts
  straight across (:meth:`from_position_status`). The two extra members cover
  the cases where no position row exists at all:

  * ``FAILED``   — the broker/exchange refused the order, so nothing opened.
  * ``REJECTED`` — a worker-side policy (MAX_OPEN_ORDERS, one-position-per-symbol)
    blocked the entry before it ever reached the broker.

  A separate enum rather than a reuse of ``PositionStatusEnum`` because a cycle
  outlives — and can exist without — a position row.
  """

  OPENED = "OPENED"
  TP1 = "TP1"
  TP2 = "TP2"
  SL = "SL"
  R_SL = "R_SL"
  TERMINAL_CLOSED = "TERMINAL_CLOSED"
  FORCED_CLOSED = "FORCED_CLOSED"
  FLATTED = "FLATTED"
  REJECTED = "REJECTED"
  FAILED = "FAILED"

  @classmethod
  def from_position_status(
    cls, status: PositionStatusEnum | str | None
  ) -> Optional["CycleStatusEnum"]:
    """Convert a persisted position status, or ``None`` when it is unknown."""
    if status is None:
      return None
    value = getattr(status, "value", str(status))
    try:
      return cls(value)
    except ValueError:
      return None


class CycleOutcomeEnum(str, Enum):
  """What the worker did with one action of the cycle.

  Drives the icon in front of a timeline entry: the *action* says what the
  broker asked for, the outcome says how it went.
  """

  #: The order reached the broker and filled.
  FILLED = "FILLED"
  #: The broker/exchange refused the order.
  FAILED = "FAILED"
  #: A worker-side policy blocked the entry; no order was placed.
  REJECTED = "REJECTED"
  #: An opposite-side position was closed to make room for this entry.
  FORCE_CLOSED = "FORCE_CLOSED"
  #: A breakeven SL could not be placed, so the unprotected position was closed.
  UNPROTECTED_CLOSED = "UNPROTECTED_CLOSED"
  #: An admin FLAT directive closed the position.
  ADMIN_FLAT = "ADMIN_FLAT"
  #: The reconciler found the position already gone on the broker and synced it.
  RECONCILED = "RECONCILED"
  #: The platform reported the close itself (terminal close, exchange fill).
  PLATFORM_CLOSED = "PLATFORM_CLOSED"


#: Statuses that mean the position is finished. Terminal is sticky: a late or
#: replayed action must never advertise a closed position as live again.
TERMINAL_STATUSES: frozenset[CycleStatusEnum] = frozenset(
  {
    CycleStatusEnum.TP2,
    CycleStatusEnum.SL,
    CycleStatusEnum.R_SL,
    CycleStatusEnum.FLATTED,
    CycleStatusEnum.FORCED_CLOSED,
    CycleStatusEnum.TERMINAL_CLOSED,
  }
)

#: Statuses that describe an action, not a position — nothing was ever opened.
#: They only stand as a cycle's status while no position status is known.
NO_POSITION_STATUSES: frozenset[CycleStatusEnum] = frozenset(
  {CycleStatusEnum.FAILED, CycleStatusEnum.REJECTED}
)


def merge_status(
  current: Optional[CycleStatusEnum], incoming: Optional[CycleStatusEnum]
) -> Optional[CycleStatusEnum]:
  """Fold *incoming* into a cycle's *current* status.

  Three rules, each protecting the headline from a misleading update:

  1. An action that says nothing about the position (``incoming is None``)
     leaves the status alone.
  2. A terminal status is final — a TP1 arriving after the SL, or a JetStream
     redelivery of the entry, must not reopen the trade.
  3. ``FAILED`` / ``REJECTED`` describe the *action*, so they never overwrite a
     real position status: a TP1 that the broker refused leaves an OPENED
     position OPENED.
  """
  if incoming is None:
    return current
  if current is None:
    return incoming
  if current in TERMINAL_STATUSES:
    return current
  if incoming in NO_POSITION_STATUSES and current not in NO_POSITION_STATUSES:
    return current
  return incoming


class CycleEventSchema(BaseModel):
  """One entry in a cycle's timeline.

  Everything the message may need is captured on the event itself, because the
  body is re-rendered from the stored events alone on every later edit — going
  back to the ``positions`` table for it would tie the message to rows that a
  close has already rewritten.

  Serialised into the cycle's ``events`` JSON column via ``model_dump``; the
  presenter reads it back as a plain dict, so every field is optional and the
  renderer skips whatever a given action did not carry.
  """

  model_config = ConfigDict(use_enum_values=True)

  action: str
  outcome: CycleOutcomeEnum
  timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

  price: Optional[float] = None
  volume: Optional[float] = None
  sl: Optional[float] = None
  tp1: Optional[float] = None
  tp2: Optional[float] = None

  risk_percent: Optional[float] = None
  #: True when RISK_PERCENTAGE overrode the signal's own risk (marks it ⚙️).
  risk_custom: bool = False
  #: True when the worker sized the volume itself instead of using the payload.
  auto_volume: bool = False
  #: Percent of the position a TP1 closed, when one applied.
  tp1_percent: Optional[float] = None
  is_scale_position: bool = False

  #: Realized PnL this action booked, in account currency. Only a close ever
  #: carries one — an entry has booked nothing yet, and a 0.00 there would read
  #: as a break-even trade. ``None`` on a close means the broker did not tell
  #: us, which the message shows as ``n/a`` rather than hiding.
  profit: Optional[float] = None
  #: Qualifies what *profit* covers when it is not this close alone (the
  #: reconciler reports a position total, a crypto estimate says so).
  profit_label: str = "PnL"

  ref_id: Optional[str] = None
  ref_source_id: Optional[str] = None
  gateway_return_code: Optional[int] = None
  #: Free text from the broker or the worker policy — HTML-escaped at render.
  reason: Optional[str] = None

  def to_event(self) -> dict:
    """JSON-ready dict for the cycle's ``events`` column."""
    data = self.model_dump()
    data["timestamp"] = self.timestamp.isoformat()
    return data
