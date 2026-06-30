from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class SystemActionEnum(str, Enum):
  CRYPTO_LEVERAGE_INIT = "CRYPTO_LEVERAGE_INIT"


class SystemSchemaSchema(BaseModel):
  action: SystemActionEnum
  timestamp: datetime


class SystemCryptoLeverageInitSchema(SystemSchemaSchema):
  symbols: Optional[list[str]] = None
  default_leverage: Optional[int] = None
