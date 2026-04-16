from enum import Enum
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class MarketTypeEnum(str, Enum):
  FOREX = "FOREX"
  CRYPTO = "CRYPTO"


class Settings(BaseSettings):
  # ZeroMQ
  zmq_sub_host: str

  market_type: MarketTypeEnum = MarketTypeEnum.FOREX

  # MT5 Credentials
  mt5_server: str
  mt5_login: int
  mt5_password: str
  mt5_path: Optional[str] = None
  mt5_name: Optional[str] = None

  # Strategy configuration
  magic_number: int = 123456
  slippage_deviation: int = 20

  # Optional Notifications
  telegram_enabled: bool = False
  telegram_bot_token: Optional[str] = None
  telegram_chat_id: Optional[str] = None

  # Logging
  log_level: str = "INFO"

  # FastAPI/Web configuration
  app_host: str = "0.0.0.0"
  app_port: int = 8000

  model_config = SettingsConfigDict(
    env_file=".env", env_file_encoding="utf-8", extra="ignore"
  )


settings = Settings()
