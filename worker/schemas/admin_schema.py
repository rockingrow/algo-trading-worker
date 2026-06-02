from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class AdminActionEnum(str, Enum):
  FLAT = "FLAT"


class AdminSignalSchema(BaseModel):
  action: AdminActionEnum
  timestamp: datetime
  strategy: Optional[str] = None
  symbol: Optional[str] = None
  account_id: Optional[str] = None
