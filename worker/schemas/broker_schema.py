from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class PositionSchema(BaseModel):
  action: str = Field(..., description="Action: LONG, SHORT, SL, TP1, etc.")
  price: float
  quantity: float
  sl: Optional[float] = None
  tp1: Optional[float] = None
  tp2: Optional[float] = None
  is_running: bool = False


class SignalSchema(BaseModel):
  token: Optional[str] = None
  symbol: str = Field(..., description="Trading pair like XAUUSD")
  timeframe: str
  timestamp: datetime
  position: PositionSchema
