from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class SignalActionEnum(str, Enum):
  LONG = "LONG"
  SHORT = "SHORT"
  TP1 = "TP1"
  TP2 = "TP2"
  R_SL = "R_SL"
  SL = "SL"

class SignalStatusEnum(str, Enum):
  OPENED = "OPENED"
  REJECTED = "REJECTED"
  CLOSED = "CLOSED"
  PARTIALLY_CLOSED = "PARTIALLY_CLOSED"

class SignalSchema(BaseModel):
  """
  Flattened SignalSchema matching the TradingSignal produced by the broker.
  """

  signal_id: str
  timestamp: datetime
  action: SignalActionEnum
  symbol: str
  price: float
  quantity: float
  sl: Optional[float] = None
  tp1: Optional[float] = None
  tp2: Optional[float] = None
  is_running: Optional[bool] = None
  risk_percent: float
