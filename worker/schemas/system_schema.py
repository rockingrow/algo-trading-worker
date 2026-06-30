from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class SystemActionEnum(str, Enum):
  CRYPTO_LEVERAGE_INIT = "CRYPTO_LEVERAGE_INIT"


class SystemSchemaSchema(BaseModel):
  action: SystemActionEnum
  timestamp: datetime
  # Target worker identity in NATS-name format "<market_type>-<account_id>".
  # When omitted the action applies to every worker; when set, only the worker
  # whose identity matches executes it.
  account_id: Optional[str] = None


class SystemCryptoLeverageInitSchema(SystemSchemaSchema):
  symbols: Optional[list[str]] = None
  default_leverage: Optional[int] = None
