from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict

from worker.settings import MarketTypeEnum


class PositionStatusEnum(str, Enum):
  """Lifecycle status values for a tracked position row in SQLite."""

  OPENED = "OPENED"
  TP1 = "TP1"
  TP2 = "TP2"
  SL = "SL"
  R_SL = "R_SL"
  TERMINAL_CLOSED = "TERMINAL_CLOSED"
  FORCED_CLOSED = "FORCED_CLOSED"
  FLATTED = "FLATTED"


class PositionEventType(str, Enum):
  """Whether the NATS TRADE event represents a new position or an update to an existing one."""

  CREATED = "CREATED"
  UPDATED = "UPDATED"


class PositionEvent(BaseModel):
  """Event payload published to the NATS TRADE subject whenever a row in the
  worker's SQLite `positions` table is inserted or updated."""

  model_config = ConfigDict(use_enum_values=True)

  event: PositionEventType
  account_id: str
  account_name: Optional[str] = None
  market_type: MarketTypeEnum

  id: int
  ref_source_id: int
  ref_id: int
  strategy: str
  symbol: str
  action: str
  volume: float
  opened_price: float
  closed_price: Optional[float] = None
  status: str
  gateway_return_code: Optional[int] = None
  comment: Optional[str] = None
  message: Optional[str] = None
  created_at: Optional[str] = None
  updated_at: Optional[str] = None
  sync_status: Optional[str] = None
  sync_time: Optional[str] = None

  # Signal-derived fields (parsed from `message` JSON) needed for broker upsert
  # the first time a position is seen.
  signal_id: Optional[str] = None
  strategy_code: Optional[str] = None
  sl: Optional[float] = None
  tp1: Optional[float] = None
  tp2: Optional[float] = None
  risk_percent: float = 0.0

  # MT5 account snapshot — required to create a Trade record on the broker.
  account_leverage: Optional[int] = None
  account_balance: Optional[float] = None
