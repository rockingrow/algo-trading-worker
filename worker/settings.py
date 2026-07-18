from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Optional

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

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


NATS_REQUIRED_LISTENING_SUBJECTS: set[NatsSubjectEnum] = {NatsSubjectEnum.ADMIN, NatsSubjectEnum.SYSTEM}
WATCHDOG_INTERVAL = 10  # seconds
# Maximum age (seconds, against the signal's own timestamp) a replayed signal
# from a SYSTEM RETRY_SIGNALS may still be executed at. Older replays are
# dropped so the worker never fires a stale entry/exit fill hours after the
# market moved. See BaseSignalProcessor._handle_retry_signals.
MAX_RETRY_TIMEOUT = 60  # seconds
MT5_HEALTH_INTERVAL = 15  # seconds between MT5 connection health checks
# The FOREX market is closed from Friday 22:00 UTC to Sunday 22:00 UTC, when the
# broker's trade server is offline for its weekly maintenance. A disconnect
# during this window is expected — not a crash — so hammering the server with the
# weekday relaunch/reconnect storm only floods the logs and Telegram. While the
# market is closed the health loop backs off to this slower cadence.
MT5_HEALTH_INTERVAL_WEEKEND = 15 * 60  # seconds between health checks while closed
# Boundaries of the weekend-closed window, in UTC. These are the New-York-close
# times (17:00 ET) in winter; during northern-hemisphere summer (DST) the market
# opens/closes an hour earlier, so set both to 21 if your broker follows it.
MARKET_CLOSE_HOUR_UTC = 22  # Friday: market closed at/after this UTC hour
MARKET_OPEN_HOUR_UTC = 22   # Sunday: market open at/after this UTC hour


def is_market_closed(now: Optional[datetime] = None) -> bool:
  """True if `now` falls in the FOREX weekend-closed window (UTC).

  The window runs Friday ``MARKET_CLOSE_HOUR_UTC``:00 → Sunday
  ``MARKET_OPEN_HOUR_UTC``:00 UTC — the broker's weekly maintenance, when the
  trade server is offline and MT5 legitimately reports "disconnected". Callers
  use it to close/park the MT5 connection instead of hammering an unreachable
  server. A naive ``now`` is treated as UTC.
  """
  if now is None:
    moment = datetime.now(timezone.utc)
  elif now.tzinfo is None:
    moment = now.replace(tzinfo=timezone.utc)
  else:
    moment = now.astimezone(timezone.utc)

  weekday = moment.weekday()  # Mon=0 … Fri=4, Sat=5, Sun=6
  if weekday == 4:  # Friday — closed once the close hour passes
    return moment.hour >= MARKET_CLOSE_HOUR_UTC
  if weekday == 5:  # Saturday — closed all day
    return True
  if weekday == 6:  # Sunday — closed until the open hour
    return moment.hour < MARKET_OPEN_HOUR_UTC
  return False


class Settings(BaseSettings):
  """Application configuration loaded from environment variables and .env file."""

  # Logging
  notification_mode: str = "VERBOSE" # VERBOSE, SILENT, or ERROR

  # NATS
  nats_url: str
  nats_token: Optional[SecretStr] = None
  nats_subjects: str

  market_type: MarketTypeEnum = MarketTypeEnum.FOREX

  # Identify — derived in _validate_market_requirements as
  # "<market_type>-<gateway>-<MT5_LOGIN|CRYPTO_ACCOUNT_ID>" once the per-market
  # credentials are validated, so it is never read from .env and can't drift
  # from the credential it identifies.
  account_id: str = ""

  # MT5 Credentials — required only when MARKET_TYPE == FOREX. They are optional
  # at the field level so a pure CRYPTO deployment never has to set them (and the
  # MT5 / MetaTrader5 stack is never initialized). See the model validator below.
  forex_platform: ForexPlatformEnum = ForexPlatformEnum.MT5
  mt5_server: Optional[str] = None
  mt5_login: Optional[int] = None
  mt5_password: Optional[SecretStr] = None
  mt5_path: Optional[str] = None
  mt5_name: Optional[str] = None

  # Crypto CEX — required only when MARKET_TYPE == CRYPTO.
  crypto_exchange: CryptoExchangeEnum = CryptoExchangeEnum.BINANCE
  crypto_quote_asset: str = "USDT"  # quote currency appended to bare symbols
  crypto_api_key: Optional[SecretStr] = None
  crypto_api_secret: Optional[SecretStr] = None
  crypto_account_id: Optional[str] = None
  crypto_testnet: bool = False
  # Desired exchange position mode, enforced at startup: the gateway calls the
  # exchange after connect() to switch the account into this mode (True = Hedge,
  # False = One-way), so the account setting always matches the worker's payload
  # convention. True = Hedge (default): every order must carry an explicit
  # positionSide (LONG/SHORT). False = One-way: the account nets BUY/SELL into a
  # single position per symbol and the exchange infers direction from `side`
  # alone. A stale account setting is what produces Binance error -4061 ("Order's
  # position side does not match user's setting.") — this flag makes the worker
  # reconcile it rather than the operator flipping it by hand.
  crypto_hedge_mode: bool = True
  # Binance USDⓈ-M futures use netting mode: all strategies on the same symbol
  # share one net position, so a second strategy opening on that symbol will
  # merge positions at the exchange level.  Set to True only if you deliberately
  # run multiple strategies on the same symbol and understand the implications.
  # NOTE: this worker only ever tracks one net position per symbol regardless of
  # crypto_hedge_mode — it does not manage simultaneous LONG and SHORT
  # positions on the same symbol even when the account is in Hedge mode.
  crypto_allow_multi_strategy_per_symbol: bool = False
  # Symbols whose leverage must be initialised at worker startup. The
  # LeverageInitJob walks this list once after gateway.connect() succeeds and
  # sets each symbol's leverage to min(exchange_max, MAX_LEVERAGE_CAP). Empty
  # list (default) skips the init entirely. Comma-separated raw signal symbols
  # — they are resolved through the executor's symbol resolver, so the .env
  # form mirrors how upstream signals address the symbol (e.g. "BTCUSD,ETHUSD").
  # NoDecode: read the env var as a raw string and let parse_leverage_init_symbols
  # split it — otherwise pydantic-settings JSON-decodes the dotenv value first and
  # a plain "BTCUSD,ETHUSD" (not valid JSON) raises a SettingsError on startup.
  crypto_leverage_init_symbols: Annotated[list[str], NoDecode] = []
  # Upper bound applied by LeverageInitJob. If the symbol's exchange-side max
  # leverage is below this, the lower value is used (sub-account caps are
  # honoured automatically); if it is at or above, this cap wins.
  max_leverage_cap: int = 10
  # Last-resort floor for LeverageInitJob: if a symbol is rejected with -4421
  # (account-level cap) but the gateway cannot parse the real ceiling out of the
  # error message — e.g. Binance reworded it and the regex no longer matches —
  # it retries once at min(min_leverage_cap, target) instead of leaving the
  # symbol at its dangerous default. Set to the lowest leverage you know your
  # sub-accounts can take (5x on Binance); if an account is restricted below
  # this, the retry still fails and the symbol is logged for manual fixing.
  min_leverage_cap: int = 5
  use_custom_leverage: bool = False  # If true, force use custom leverage instead of broker crypto leverage initialization

  # Strategy configuration
  slippage_deviation: int = 100
  strategy_magic_map: dict[str, int] = {}
  use_custom_position_tp1_percent: bool = False  # If true, use custom TP1 percentage instead of signal's tp1_percent
  position_tp1_percent: Optional[float] = 50  # Only defined if you want custom TP1 percentage, otherwise default is tp1_percent in signal
  # When True, TP1 partial-closes and then moves the stop to breakeven (entry).
  # When False, TP1 only partial-closes and leaves the original entry SL
  # untouched, so the runner keeps its initial protection.
  tp1_move_sl_to_breakeven: Optional[bool] = None  # Only defined if you want to move SL to breakeven when TP1 is hit, otherwise default is move_sl_to_be in signal

  # Init capital and risk management
  capital: float = 1000
  capital_currency: str = "USD"
  volume_decision_enabled: bool = True
  use_custom_risk_percentage: bool = False  # If true, use custom risk percentage instead of signal's risk percentage
  risk_percentage: float = 2.0  # Risk 2% of capital per trade
  use_account_equity: bool = False  # If true, use account equity instead of initial capital for entry volume calculation
  # Maximum number of concurrently open orders (OPENED/TP1 positions) this worker
  # may hold. A new entry (LONG/SHORT) that would exceed the cap is not sent to the
  # broker: it is recorded with status REJECTED, reported to the broker on the TRADE
  # subject (also REJECTED) and notified — but no order is placed. A re-entry/scale-in
  # on a symbol the strategy already holds replaces that position rather than opening
  # a new slot, so it never counts against the cap. Set to 0 to disable the limit.
  max_open_orders: int = 5

  # Telegram
  telegram_enabled: bool
  telegram_bot_token: SecretStr
  telegram_chat_id: str  # management: NATS events, service start/stop, MT5 health
  # NoDecode (see crypto_leverage_init_symbols): the documented unquoted
  # comma-separated form (-100123,-100987) is not valid JSON, so let
  # parse_channel_ids split the raw string instead of pydantic JSON-decoding it.
  telegram_chat_channel_id: Annotated[list[str], NoDecode] = []  # signals: order fills/failures, terminal closes

  # ── Telegram error-log hook ──────────────────────────────────────
  # When enabled (and telegram_enabled is true), log records at ERROR level or
  # above are forwarded to Telegram. The dedicated log bot/chat is kept separate
  # from the main bot so an outage/ban on one never affects the other; both fall
  # back to telegram_bot_token / telegram_chat_id when left empty.
  telegram_log_errors_enabled: bool = False
  telegram_log_dedup_window: int = 60  # seconds — suppress identical messages
  telegram_log_chat_id: str = ""
  telegram_log_bot_token: Optional[SecretStr] = None

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

  @field_validator("crypto_leverage_init_symbols", mode="before")
  @classmethod
  def parse_leverage_init_symbols(cls, v):
    if isinstance(v, list):
      return [s for s in (str(i).strip() for i in v) if s]
    if isinstance(v, str):
      return [s for s in (i.strip() for i in v.split(",")) if s]
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
  broker_api_key: SecretStr

  @model_validator(mode="after")
  def _validate_market_requirements(self):
    """Require only the credentials the selected market actually needs, then
    derive ``account_id`` from them.

    FOREX must not boot without MT5 credentials; CRYPTO must not boot without
    the selected exchange's API keys. This keeps each deployment from carrying
    (or initializing) the other market's dependencies. Once the required
    credential is confirmed present, ``account_id`` is set to
    "<market_type>-<gateway>-<identifying credential>" — MT5_LOGIN behind the
    forex platform for FOREX, CRYPTO_ACCOUNT_ID behind the exchange for CRYPTO
    — so it is always in sync and never configured by hand. The gateway segment
    lets the broker route/identify by exchange without a separate lookup.
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
      self.account_id = f"{self.market_type.value}-{self.forex_platform.value}-{self.mt5_login}"
    elif self.market_type == MarketTypeEnum.CRYPTO:
      if self.crypto_exchange == CryptoExchangeEnum.BINANCE:
        missing = [
          name
          for name, value in (
            ("CRYPTO_QUOTE_ASSET", self.crypto_quote_asset),
            ("CRYPTO_API_KEY", self.crypto_api_key),
            ("CRYPTO_API_SECRET", self.crypto_api_secret),
            ("CRYPTO_ACCOUNT_ID", self.crypto_account_id),
          )
          if not value
        ]
        if missing:
          raise ValueError(
            f"MARKET_TYPE=CRYPTO (BINANCE) requires: {', '.join(missing)}"
          )
        self.account_id = f"{self.market_type.value}-{self.crypto_exchange.value}-{self.crypto_account_id}"
    return self

  model_config = SettingsConfigDict(
    env_file=".env", env_file_encoding="utf-8", extra="ignore"
  )


settings = Settings()
