from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class AdminActionEnum(str, Enum):
  FLAT = "FLAT"


class AdminMessageSchema(BaseModel):
  action: AdminActionEnum
  timestamp: datetime

class AdminFlatSchema(AdminMessageSchema):
  """``account_id`` alone is not a unique worker identity — the same raw id can
  collide across markets/gateways (e.g. an email used as ``account_id`` also
  matching a CRYPTO gateway name). The broker now sends ``market``/``gateway``
  alongside ``account_id`` so a FLAT can be routed by the full composite key,
  mirroring ``SystemWorkerConnectedSchema``. Each of the three fields is an
  optional filter: unset means "don't filter on this dimension" (e.g. no
  account_id → broadcast to every worker matching strategy/symbol)."""

  strategy: Optional[str] = None
  symbol: Optional[str] = None
  account_id: Optional[str] = None
  market: Optional[str] = None
  gateway: Optional[str] = None
