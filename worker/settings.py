from enum import Enum
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class MarketTypeEnum(str, Enum):
  FOREX = "FOREX"
  CRYPTO = "CRYPTO"


class Settings(BaseSettings):
  # ZeroMQ
  zmq_sub_host: str

  # ZMQ CURVE security (optional — leave blank to disable encryption)
  zmq_curve_server_public_key: Optional[str] = None  # broker's public key (Z85)
  zmq_curve_client_public_key: Optional[str] = None  # this client's public key (Z85)
  zmq_curve_client_secret_key: Optional[str] = None  # this client's secret key (Z85)

  market_type: MarketTypeEnum = MarketTypeEnum.FOREX

  # MT5 Credentials
  mt5_server: str
  mt5_login: int
  mt5_password: str
  mt5_path: Optional[str] = None
  mt5_name: Optional[str] = None

  # Strategy configuration
  magic_number: int = 20260409
  slippage_deviation: int = 20

  # Telegram
  telegram_enabled: bool
  telegram_bot_token: str
  telegram_chat_id: str           # management: ZMQ events, service start/stop, MT5 health
  telegram_chat_channel_id: str = ""  # signals: order fills/failures, terminal closes

  # Logging
  log_level: str = "INFO"

  # FastAPI/Web configuration
  app_host: str = "0.0.0.0"
  app_port: int = 8000

  # Broker
  broker_api_url: str
  broker_api_key: str

  model_config = SettingsConfigDict(
    env_file=".env", env_file_encoding="utf-8", extra="ignore"
  )


settings = Settings()
