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
from typing import Any, Optional

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
    "new_sl",
    "sl_update",
    "forced_closed",
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
  new_sl: Optional[float] = None
  sl_update: Any = None  # nested TradeResult from the SL-update step (TP1)
  forced_closed: list = field(default_factory=list)
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
