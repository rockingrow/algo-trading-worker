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
  FLAT = "FLAT"


class SignalStatusEnum(str, Enum):
  OPENED = "OPENED"
  REJECTED = "REJECTED"
  CLOSED = "CLOSED"
  PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
  FLAT = "FLAT"


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
