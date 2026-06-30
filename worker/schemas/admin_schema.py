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
  strategy: Optional[str] = None
  symbol: Optional[str] = None
  account_id: Optional[str] = None
