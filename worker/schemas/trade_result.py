"""
worker/schemas/trade_result.py
──────────────────────────────
``TradeResult`` value object.

Every broker operation (open / partial-close / SL-update / full-close) returns a
``TradeResult``. Previously this was a bare ``dict`` built from scattered
``{"success": False, "retcode": -1, "comment": ...}`` literals duplicated across
both executors and the strategy/handler — easy to mistype a key, no type safety.

This value object centralizes the shape and offers two factory constructors
(:meth:`ok` / :meth:`fail`) so the success/failure literals live in one place.

To keep the large body of existing call sites (and tests) working, it also
supports read/write *mapping* access (``r["ticket"]``, ``r.get("price")``,
``r["sl_update"] = ...``), so consumers can migrate to attribute access
incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Optional


class TradeFailureEnum(str, Enum):
  """Why a broker operation failed, for callers whose next step depends on it.

  The free-text ``comment`` is for humans; branching on it is how "the broker
  refused the close" once got read as "there was nothing to close", and a live
  position was recorded as flat. A caller that must tell those apart reads
  :attr:`TradeResult.failure` instead. ``None`` on a failed result means the
  cause was not classified — treat it as a broker-side failure, never as "no
  position".
  """

  #: Nothing was live to act on. The only failure that proves the broker holds
  #: no matching position, so the only one that may sync a DB row to closed.
  NO_POSITION = "NO_POSITION"
  #: A specific ticket was requested and is not among the live positions.
  TICKET_NOT_FOUND = "TICKET_NOT_FOUND"
  #: Some of the positions closed, at least one is still live.
  PARTIAL_CLOSE = "PARTIAL_CLOSE"


# Declared fields exposed via mapping access (everything except the `extra` bag).
_FIELDS = frozenset(
  {
    "success",
    "retcode",
    "comment",
    "ticket",
    "price",
    "volume",
    "source_ticket",
    "sl",
    "tp",
    "new_sl",
    "sl_update",
    "forced_closed",
    "profit",
    "failure",
  }
)


@dataclass
class TradeResult:
  """Outcome of a broker trade operation (broker-neutral)."""

  success: bool
  retcode: int = -1
  comment: str = ""
  ticket: Optional[str] = None
  price: Optional[float] = None
  volume: Optional[float] = None
  source_ticket: Optional[str] = None
  # Stops actually registered with the broker by an entry — the signal's levels
  # after any broker-driven adjustment (FOREX: StopValidator widening them to the
  # minimum stop distance). They are what the position really carries, so the DB
  # and the operator notification report these rather than the signal's own
  # numbers. ``None`` means no stop of that kind is resting on the position.
  sl: Optional[float] = None
  tp: Optional[float] = None
  new_sl: Optional[float] = None
  sl_update: Any = None  # nested TradeResult from the SL-update step (TP1)
  forced_closed: list = field(default_factory=list)
  # Realized PnL booked by a *closing* operation, in account currency. Set on
  # every close (partial or full) the worker performs, so the notification for it
  # can report the money the trade actually made or lost. ``None`` means either
  # "not a close" (an entry books nothing) or "the broker did not tell us" — the
  # two are distinguished by context, and a close that renders ``None`` shows
  # ``n/a`` rather than a fabricated 0.0.
  profit: Optional[float] = None
  # Machine-readable cause of a failure (see TradeFailureEnum); ``None`` on a
  # success and on a failure nobody classified.
  failure: Optional[TradeFailureEnum] = None
  # Catch-all for any ad-hoc keys a caller may still set via mapping access.
  extra: dict = field(default_factory=dict)

  # ── Factories ─────────────────────────────────────────────────────────── #

  @classmethod
  def ok(cls, *, retcode: int = 0, **kwargs: Any) -> "TradeResult":
    """Build a successful result. Unknown keys are kept in ``extra``."""
    known, extra = cls._split(kwargs)
    return cls(success=True, retcode=retcode, extra=extra, **known)

  @classmethod
  def fail(
    cls, comment: str = "", *, retcode: int = -1, **kwargs: Any
  ) -> "TradeResult":
    """Build a failed result. Unknown keys are kept in ``extra``."""
    known, extra = cls._split(kwargs)
    return cls(success=False, retcode=retcode, comment=comment, extra=extra, **known)

  @classmethod
  def no_position(cls, comment: str = "No Positions Found") -> "TradeResult":
    """A failure that proves nothing matching was live on the broker."""
    return cls.fail(comment, failure=TradeFailureEnum.NO_POSITION)

  @property
  def is_no_position(self) -> bool:
    return is_no_position(self)

  @staticmethod
  def _split(kwargs: dict) -> tuple[dict, dict]:
    known = {k: v for k, v in kwargs.items() if k in _FIELDS}
    extra = {k: v for k, v in kwargs.items() if k not in _FIELDS}
    return known, extra

  # ── Mapping-compatibility shim (eases migration from the old dict) ────── #

  def get(self, key: str, default: Any = None) -> Any:
    if key in _FIELDS:
      return getattr(self, key)
    return self.extra.get(key, default)

  def __getitem__(self, key: str) -> Any:
    if key in _FIELDS:
      return getattr(self, key)
    return self.extra[key]

  def __setitem__(self, key: str, value: Any) -> None:
    if key in _FIELDS:
      setattr(self, key, value)
    else:
      self.extra[key] = value

  def __contains__(self, key: str) -> bool:
    return key in _FIELDS or key in self.extra

  def setdefault(self, key: str, default: Any) -> Any:
    if key in self and self[key] is not None:
      return self[key]
    self[key] = default
    return default


def is_no_position(result: Any) -> bool:
  """True when *result* failed because nothing matching was live.

  Accepts a :class:`TradeResult` or a legacy result mapping, so a caller can
  branch on it whatever shape its collaborator returns.
  """
  if result.get("success"):
    return False
  failure = result.get("failure")
  return failure is TradeFailureEnum.NO_POSITION or failure == "NO_POSITION"


def close_batch_result(
  closed: list,
  failed: list,
  *,
  source_ticket: Optional[str],
  reason: str,
  nothing_closed_comment: str,
  fill_retcode: bool = True,
) -> TradeResult:
  """The outcome of closing several positions as one exit.

  *closed* / *failed* are the per-position results. Every position closed is a
  success. Some closed is :attr:`TradeFailureEnum.PARTIAL_CLOSE`: the exit did
  not happen, so the caller must not mark the tracked row closed. It carries the
  *refusal's* retcode (not the last fill's, which would read as a success) and
  still reports the PnL the closed ones booked. None closed is a plain failure.
  Shared by every executor so the rule cannot drift between markets.

  *fill_retcode* reports the last fill's retcode on success (MT5's ``DONE``);
  a gateway whose fills carry no meaningful one keeps the default ``0``.
  """
  if not closed:
    last_refusal = failed[-1] if failed else {}
    return TradeResult.fail(
      nothing_closed_comment, retcode=last_refusal.get("retcode", -1)
    )
  last = closed[-1]
  summary = {
    "ticket": last.get("ticket"),
    "source_ticket": source_ticket,
    "price": last.get("price"),
    "volume": last.get("volume"),
    # Summed, not last-write-wins like price/volume: the notification reports
    # one exit, so it needs what the whole exit booked.
    "profit": total_profit(closed),
  }
  total = len(closed) + len(failed)
  if failed:
    refusal = failed[-1]
    return TradeResult.fail(
      f"Closed {len(closed)} of {total} position(s) [{reason}] — the rest are "
      f"still open: {refusal.get('comment') or 'close refused'}",
      retcode=refusal.get("retcode", -1),
      failure=TradeFailureEnum.PARTIAL_CLOSE,
      **summary,
    )
  retcode = last.get("retcode") if fill_retcode else 0
  return TradeResult.ok(
    retcode=retcode if retcode is not None else 0,
    comment=f"Closed {len(closed)} position(s) [{reason}]",
    **summary,
  )


def total_profit(results: Iterable[Any]) -> Optional[float]:
  """Sum the realized PnL of several closes, or ``None`` when none reported one.

  Used where one logical exit fans out into several broker closes (a symbol held
  as multiple tickets, an ADMIN FLAT over a netted position): the notification
  reports a single close, so it needs a single figure.

  A result that reports no PnL contributes nothing rather than zero, and an exit
  where *nothing* was reported stays ``None`` — so a partially-known total is
  still shown (it is closer to the truth than nothing) while a wholly unknown one
  never masquerades as a break-even close.
  """
  known = [r.get("profit") for r in results if r.get("profit") is not None]
  return sum(known) if known else None
