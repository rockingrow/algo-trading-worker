from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict


class PositionEventType(str, Enum):
  CREATED = "CREATED"
  UPDATED = "UPDATED"


class PositionEvent(BaseModel):
  """Event payload published to the NATS TRADE subject whenever a row in the
  worker's SQLite `positions` table is inserted or updated."""

  model_config = ConfigDict(use_enum_values=True)

  event: PositionEventType
  account_id: str

  id: int
  source_ticket: int
  ticket: int
  strategy: str
  symbol: str
  action: str
  volume: float
  opened_price: float
  closed_price: Optional[float] = None
  status: str
  mt5_retcode: Optional[int] = None
  comment: Optional[str] = None
  message: Optional[str] = None
  created_at: Optional[str] = None
  updated_at: Optional[str] = None
  sync_status: Optional[str] = None
  sync_time: Optional[str] = None

  # Signal-derived fields (parsed from `message` JSON) needed for broker upsert
  # the first time a position is seen.
  signal_id: Optional[str] = None
  magic: Optional[str] = None
  sl: Optional[float] = None
  tp1: Optional[float] = None
  tp2: Optional[float] = None
  risk_percent: float = 0.0

  # MT5 account snapshot — required to create a Trade record on the broker.
  account_leverage: Optional[int] = None
  account_balance: Optional[float] = None
