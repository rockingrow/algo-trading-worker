from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class SignalActionEnum(str, Enum):
  """Trading signal actions that the worker can receive and execute."""

  LONG = "LONG"
  SHORT = "SHORT"
  TP1 = "TP1"
  TP2 = "TP2"
  R_SL = "R_SL"
  SL = "SL"
  FLAT = "FLAT"


class SignalStatusEnum(str, Enum):
  """Execution status of a signal as reported back to the broker."""

  OPENED = "OPENED"
  REJECTED = "REJECTED"
  CLOSED = "CLOSED"
  PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
  FLAT = "FLAT"


class ScalingSchema(BaseModel):
  """Multipliers used to scale a *scale-in* (averaging) position.

  Each field is an optional multiplier applied to the original signal value:
  ``tp`` scales TP1/TP2, ``sl`` scales the stop loss, and ``quantity`` scales
  the order size. A ``None`` (or absent) field leaves the corresponding value
  untouched. The broker only sets these when ``is_scale_position`` is True.
  """

  tp: Optional[float] = None
  sl: Optional[float] = None
  quantity: Optional[float] = None


class SignalSchema(BaseModel):
  """
  Flattened SignalSchema matching the TradingSignal produced by the broker.
  Trading signals (LONG/SHORT/TP1/TP2/SL/R_SL) carry all fields.
  FLAT signals only need strategy, timestamp, action, and symbol.
  """

  strategy: str
  timestamp: datetime
  action: SignalActionEnum
  symbol: str
  signal_id: Optional[str] = None
  price: Optional[float] = None
  quantity: Optional[float] = None
  sl: Optional[float] = None
  tp1: Optional[float] = None
  tp2: Optional[float] = None
  is_running: Optional[bool] = None
  risk_percent: Optional[float] = None
  # Scale-in (averaging) support. When ``is_scale_position`` is True the broker
  # also sends a ``scaling`` object whose multipliers adjust TP/SL/quantity off
  # the original signal values — see :meth:`apply_scaling`.
  is_scale_position: Optional[bool] = None
  scaling: Optional[ScalingSchema] = None

  def apply_scaling(self) -> "SignalSchema":
    """Return a copy with TP/SL/quantity scaled for a scale-in position.

    When this is not a scale position (``is_scale_position`` is not True) or no
    ``scaling`` object is present, the signal is returned unchanged. Otherwise
    each present multiplier rescales the matching original value:

    * ``scaling.tp``        → multiplies ``tp1`` and ``tp2``
    * ``scaling.sl``        → multiplies ``sl``
    * ``scaling.quantity``  → multiplies ``quantity`` (payload-quantity mode)
                              and ``risk_percent`` (self-determined / risk mode),
                              so the resulting position size scales the same way
                              regardless of which sizing mode is active.

    A ``None`` multiplier (field absent) leaves its target untouched; a value
    that has nothing to scale (e.g. ``scaling.tp`` set but ``tp1``/``tp2`` are
    None) is a no-op.
    """
    if not self.is_scale_position or self.scaling is None:
      return self

    updates: dict = {}
    scaling = self.scaling

    if scaling.tp is not None:
      if self.tp1 is not None:
        updates["tp1"] = self.tp1 * scaling.tp
      if self.tp2 is not None:
        updates["tp2"] = self.tp2 * scaling.tp

    if scaling.sl is not None and self.sl is not None:
      updates["sl"] = self.sl * scaling.sl

    if scaling.quantity is not None:
      # Scale whichever sizing input is present so both modes are supported:
      # payload-quantity mode reads ``quantity``; self-determined (risk) mode
      # derives the size from ``risk_percent``.
      if self.quantity is not None:
        updates["quantity"] = self.quantity * scaling.quantity
      if self.risk_percent is not None:
        updates["risk_percent"] = self.risk_percent * scaling.quantity

    if not updates:
      return self
    return self.model_copy(update=updates)
