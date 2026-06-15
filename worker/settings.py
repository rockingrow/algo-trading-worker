from enum import Enum
from typing import Optional

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from worker.schemas.nats_schema import NatsSubjectEnum


class MarketTypeEnum(str, Enum):
  """Supported trading market types."""

  FOREX = "FOREX"
  CRYPTO = "CRYPTO"


class CryptoExchangeEnum(str, Enum):
  """Supported crypto centralized exchanges (CEX).

  The factory in :mod:`worker.gateways.crypto.factory` maps each member to a concrete
  gateway implementation, so adding an exchange means adding a member here and a
  gateway — no call site changes.
  """

  BINANCE = "BINANCE"


class ForexPlatformEnum(str, Enum):
  """Supported FOREX trading platforms.

  The factory in :mod:`worker.gateways.forex.factory` maps each member to a concrete
  :class:`~worker.gateways.forex.base.BasePlatformGateway`, so adding a platform
  (e.g. MT6) means adding a member here and a gateway — no call site changes.
  """

  MT5 = "MT5"


NATS_REQUIRED_LISTENING_SUBJECTS: set[NatsSubjectEnum] = {NatsSubjectEnum.ADMIN}
WATCHDOG_INTERVAL = 10  # seconds
MT5_HEALTH_INTERVAL = 15  # seconds between MT5 connection health checks


class Settings(BaseSettings):
  """Application configuration loaded from environment variables and .env file."""

  # Logging
  notification_mode: str = "VERBOSE" # VERBOSE, SILENT, or ERROR

  # NATS
  nats_url: str
  nats_token: Optional[str] = None
  nats_subjects: str

  market_type: MarketTypeEnum = MarketTypeEnum.FOREX

  # MT5 Credentials — required only when MARKET_TYPE == FOREX. They are optional
  # at the field level so a pure CRYPTO deployment never has to set them (and the
  # MT5 / MetaTrader5 stack is never initialized). See the model validator below.
  forex_platform: ForexPlatformEnum = ForexPlatformEnum.MT5
  mt5_server: Optional[str] = None
  mt5_login: Optional[int] = None
  mt5_password: Optional[str] = None
  mt5_path: Optional[str] = None
  mt5_name: Optional[str] = None

  # Crypto CEX — required only when MARKET_TYPE == CRYPTO.
  crypto_exchange: CryptoExchangeEnum = CryptoExchangeEnum.BINANCE
  crypto_quote_asset: str = "USDT"  # quote currency appended to bare symbols
  binance_api_key: Optional[str] = None
  binance_api_secret: Optional[str] = None
  binance_account_id: Optional[str] = None
  binance_testnet: bool = False
  # Binance USDⓈ-M futures use netting mode: all strategies on the same symbol
  # share one net position, so a second strategy opening on that symbol will
  # merge positions at the exchange level.  Set to True only if you deliberately
  # run multiple strategies on the same symbol and understand the implications.
  crypto_allow_multi_strategy_per_symbol: bool = False

  # Strategy configuration
  strategy_magic_map: dict[str, int] = {}
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
  telegram_chat_channel_id: list[str] = []  # signals: order fills/failures, terminal closes

  @field_validator("telegram_chat_channel_id", mode="before")
  @classmethod
  def parse_channel_ids(cls, v):
    if isinstance(v, list):
      return [i for i in v if i]
    if isinstance(v, str):
      return [i.strip() for i in v.split(",") if i.strip()]
    if isinstance(v, int):
      return [str(v)]
    return v

  # Database
  db_file: str = "worker_data.sqlite"

  # Logging
  log_level: str = "INFO"

  # FastAPI/Web configuration
  app_host: str = "0.0.0.0"
  app_port: int = 8000

  # Broker
  broker_api_url: str
  broker_api_key: str

  @model_validator(mode="after")
  def _validate_market_requirements(self):
    """Require only the credentials the selected market actually needs.

    FOREX must not boot without MT5 credentials; CRYPTO must not boot without
    the selected exchange's API keys. This keeps each deployment from carrying
    (or initializing) the other market's dependencies.
    """
    if self.market_type == MarketTypeEnum.FOREX:
      missing = [
        name
        for name, value in (
          ("MT5_SERVER", self.mt5_server),
          ("MT5_LOGIN", self.mt5_login),
          ("MT5_PASSWORD", self.mt5_password),
        )
        if not value
      ]
      if missing:
        raise ValueError(
          f"MARKET_TYPE=FOREX requires: {', '.join(missing)}"
        )
    elif self.market_type == MarketTypeEnum.CRYPTO:
      if self.crypto_exchange == CryptoExchangeEnum.BINANCE:
        missing = [
          name
          for name, value in (
            ("CRYPTO_QUOTE_ASSET", self.crypto_quote_asset),
            ("BINANCE_API_KEY", self.binance_api_key),
            ("BINANCE_API_SECRET", self.binance_api_secret),
            ("BINANCE_ACCOUNT_ID", self.binance_account_id),
            ("BINANCE_TESTNET", self.binance_testnet),
          )
          if not value
        ]
        if missing:
          raise ValueError(
            f"MARKET_TYPE=CRYPTO (BINANCE) requires: {', '.join(missing)}"
          )
    return self

  model_config = SettingsConfigDict(
    env_file=".env", env_file_encoding="utf-8", extra="ignore"
  )


settings = Settings()
