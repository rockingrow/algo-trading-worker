from enum import Enum
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class MarketTypeEnum(str, Enum):
  FOREX = "FOREX"
  CRYPTO = "CRYPTO"


class Settings(BaseSettings):
  # NATS
  nats_url: str
  nats_token: Optional[str] = None

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

  # Init capital and risk management
  capital: float = 1000
  capital_currency: str = "USC"
  volume_decision_enabled: bool = True
  risk_percentage: float = 3.0  # Risk 1% of capital per trade
  use_account_equity: bool = False  # If true, use account equity instead of initial capital for entry volume calculation
  position_tp1_percent: float = 30.0

  # Telegram
  telegram_enabled: bool
  telegram_bot_token: str
  telegram_chat_id: str  # management: NATS events, service start/stop, MT5 health
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
