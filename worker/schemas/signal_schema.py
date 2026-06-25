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
  """Scale-in (averaging) multipliers reported by the broker for context.

  The broker has **already applied** these multipliers to the signal it sends:
  ``sl``, ``tp1``, ``tp2`` and ``quantity`` arrive as their final, scaled values.
  The multipliers are passed through purely as metadata — ``tp`` (the factor
  applied to TP1/TP2), ``sl`` (applied to the stop loss) and ``quantity``
  (applied to the order size).

  The worker re-applies **only** ``quantity``, and only when it sizes the entry
  itself (``VOLUME_DECISION_ENABLED`` risk mode ignores ``signal.quantity``) — see
  :meth:`SignalSchema.scale_quantity_factor`. ``tp``/``sl`` are never re-applied;
  the scaled SL/TP already in the payload are used verbatim.

  A ``None`` (or absent) field means no scaling on that dimension. The broker only
  sets these when ``is_scale_position`` is True.
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
  # has already scaled SL/TP1/TP2/quantity in this payload and sends the
  # ``scaling`` object as metadata. The worker uses those values as-is and only
  # re-derives the multiplier for self-sized entries — see
  # :meth:`scale_quantity_factor`.
  is_scale_position: Optional[bool] = None
  scaling: Optional[ScalingSchema] = None

  def scale_quantity_factor(self) -> float:
    """Quantity multiplier to apply to a *self-computed* entry volume.

    The broker already bakes the scale-in factor into the SL/TP/quantity it
    sends, so those are consumed verbatim — the worker re-applies the factor in
    exactly one case: when ``VOLUME_DECISION_ENABLED`` sizes the entry from risk
    and therefore ignores ``signal.quantity``. Multiplying the risk-derived
    volume by this factor keeps a scale-in entry sized by the same proportion the
    broker intended.

    Returns the ``scaling.quantity`` multiplier for a scale-in signal that
    carries one, else ``1.0`` (leave the computed volume untouched).
    """
    if (
      self.is_scale_position
      and self.scaling is not None
      and self.scaling.quantity is not None
    ):
      return self.scaling.quantity
    return 1.0
