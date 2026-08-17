from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Optional

from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
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


NATS_REQUIRED_LISTENING_SUBJECTS: set[NatsSubjectEnum] = {
  NatsSubjectEnum.ADMIN,
  NatsSubjectEnum.SYSTEM,
}
WATCHDOG_INTERVAL = 10  # seconds
# Maximum age (seconds, against the signal's own timestamp) a signal replayed in
# the WORKER_CONNECTED_ACK may still be executed at. Older replays are dropped so
# the worker never fires a stale entry/exit fill hours after the market moved.
# See BaseSignalProcessor._apply_retry_signals.
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
MARKET_OPEN_HOUR_UTC = 22  # Sunday: market open at/after this UTC hour


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


# Every settings group below is its own ``BaseSettings`` so it reads the same
# flat environment variables the app has always used (kept via each field's
# ``validation_alias``), while grouping related settings under one nested object
# on the main :class:`Settings`. ``serialization_alias`` keeps ``model_dump``
# emitting the original flat keys, so :meth:`Settings.flat_dump` reproduces the
# exact legacy ``settings_dict`` every downstream consumer already expects.
_GROUP_CONFIG = SettingsConfigDict(
  env_file=".env", env_file_encoding="utf-8", extra="ignore"
)


def _req(env: str, dump: str, **kwargs) -> Any:
  """A required group field: read from env var *env*, dumped as key *dump*."""
  return Field(validation_alias=AliasChoices(env), serialization_alias=dump, **kwargs)


def _opt(default: Any, env: str, dump: str, **kwargs) -> Any:
  """An optional group field with *default*: env var *env*, dump key *dump*."""
  return Field(
    default, validation_alias=AliasChoices(env), serialization_alias=dump, **kwargs
  )


#: Order the settings groups are flattened in by :meth:`Settings.flat_dump`
#: (no key clashes — every ``serialization_alias`` is unique across groups).
_FLAT_GROUPS = (
  "logging",
  "nats",
  "forex",
  "crypto",
  "strategy",
  "risk",
  "telegram",
  "database",
  "web",
  "broker",
)


class LoggingSettings(BaseSettings):
  """Logging / notification verbosity."""

  model_config = _GROUP_CONFIG

  notification_mode: str = _opt(
    "VERBOSE", "NOTIFICATION_MODE", "notification_mode"
  )  # VERBOSE, SILENT, or ERROR
  level: str = _opt("INFO", "LOG_LEVEL", "log_level")


class NatsSettings(BaseSettings):
  """NATS connection + subscriptions."""

  model_config = _GROUP_CONFIG

  url: str = _req("NATS_URL", "nats_url")
  token: Optional[SecretStr] = _opt(None, "NATS_TOKEN", "nats_token")
  subjects: str = _req("NATS_SUBJECTS", "nats_subjects")


class ForexSettings(BaseSettings):
  """MT5 / FOREX platform credentials.

  Required only when ``MARKET_TYPE == FOREX``; every field is optional at the
  group level so a pure CRYPTO deployment never has to set them (and the MT5 /
  MetaTrader5 stack is never initialized). The main-model validator enforces the
  required subset for a FOREX deployment.
  """

  model_config = _GROUP_CONFIG

  platform: ForexPlatformEnum = _opt(
    ForexPlatformEnum.MT5, "FOREX_PLATFORM", "forex_platform"
  )
  server: Optional[str] = _opt(None, "MT5_SERVER", "mt5_server")
  login: Optional[int] = _opt(None, "MT5_LOGIN", "mt5_login")
  password: Optional[SecretStr] = _opt(None, "MT5_PASSWORD", "mt5_password")
  path: Optional[str] = _opt(None, "MT5_PATH", "mt5_path")
  name: Optional[str] = _opt(None, "MT5_NAME", "mt5_name")
  # FOREX is the only market that can genuinely hold several *independent*
  # positions on one symbol: every strategy trades under its own magic number
  # (``strategy_magic_map``), so the platform keeps their tickets, SL/TP and
  # closes fully isolated. Set to True to let a second strategy enter a symbol
  # another strategy already holds. The one-active-position-per-(strategy,
  # symbol) rule still applies — a strategy re-entering its own symbol replaces
  # its existing position, it never stacks a second one.
  # Default False keeps the conservative one-strategy-per-symbol behaviour.
  # Requires a hedging-enabled account: on a netting account the broker merges
  # both strategies into a single net position and the isolation is lost.
  allow_multi_strategy_per_symbol: bool = _opt(
    False,
    "FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL",
    "forex_allow_multi_strategy_per_symbol",
  )


class CryptoSettings(BaseSettings):
  """Crypto CEX credentials + behaviour. Required only when ``MARKET_TYPE == CRYPTO``."""

  model_config = _GROUP_CONFIG

  exchange: CryptoExchangeEnum = _opt(
    CryptoExchangeEnum.BINANCE, "CRYPTO_EXCHANGE", "crypto_exchange"
  )
  quote_asset: str = _opt(
    "USDT", "CRYPTO_QUOTE_ASSET", "crypto_quote_asset"
  )  # quote currency appended to bare symbols
  api_key: Optional[SecretStr] = _opt(None, "CRYPTO_API_KEY", "crypto_api_key")
  api_secret: Optional[SecretStr] = _opt(None, "CRYPTO_API_SECRET", "crypto_api_secret")
  account_id: Optional[str] = _opt(None, "CRYPTO_ACCOUNT_ID", "crypto_account_id")
  testnet: bool = _opt(False, "CRYPTO_TESTNET", "crypto_testnet")
  # Desired exchange position mode, enforced at startup: the gateway calls the
  # exchange after connect() to switch the account into this mode (True = Hedge,
  # False = One-way), so the account setting always matches the worker's payload
  # convention. True = Hedge (default): every order must carry an explicit
  # positionSide (LONG/SHORT). False = One-way: the account nets BUY/SELL into a
  # single position per symbol and the exchange infers direction from `side`
  # alone. A stale account setting is what produces Binance error -4061 ("Order's
  # position side does not match user's setting.") — this flag makes the worker
  # reconcile it rather than the operator flipping it by hand.
  hedge_mode: bool = _opt(True, "CRYPTO_HEDGE_MODE", "crypto_hedge_mode")
  # Binance USDⓈ-M futures use netting mode: all strategies on the same symbol
  # share one net position, so a second strategy opening on that symbol will
  # merge positions at the exchange level.  Set to True only if you deliberately
  # run multiple strategies on the same symbol and understand the implications.
  # NOTE: this worker only ever tracks one net position per symbol regardless of
  # hedge_mode — it does not manage simultaneous LONG and SHORT positions on the
  # same symbol even when the account is in Hedge mode.
  allow_multi_strategy_per_symbol: bool = _opt(
    False,
    "CRYPTO_ALLOW_MULTI_STRATEGY_PER_SYMBOL",
    "crypto_allow_multi_strategy_per_symbol",
  )
  # Symbols whose leverage must be initialised at worker startup. The
  # LeverageInitJob walks this list once after gateway.connect() succeeds and
  # sets each symbol's leverage to min(exchange_max, MAX_LEVERAGE_CAP). Empty
  # list (default) skips the init entirely. Comma-separated raw signal symbols
  # — they are resolved through the executor's symbol resolver, so the .env
  # form mirrors how upstream signals address the symbol (e.g. "BTCUSD,ETHUSD").
  # NoDecode: read the env var as a raw string and let parse_leverage_init_symbols
  # split it — otherwise pydantic-settings JSON-decodes the dotenv value first and
  # a plain "BTCUSD,ETHUSD" (not valid JSON) raises a SettingsError on startup.
  leverage_init_symbols: Annotated[list[str], NoDecode] = _opt(
    [], "CRYPTO_LEVERAGE_INIT_SYMBOLS", "crypto_leverage_init_symbols"
  )
  # Upper bound applied by LeverageInitJob. If the symbol's exchange-side max
  # leverage is below this, the lower value is used (sub-account caps are
  # honoured automatically); if it is at or above, this cap wins.
  max_leverage_cap: int = _opt(10, "MAX_LEVERAGE_CAP", "max_leverage_cap")
  # Last-resort floor for LeverageInitJob: if a symbol is rejected with -4421
  # (account-level cap) but the gateway cannot parse the real ceiling out of the
  # error message — e.g. Binance reworded it and the regex no longer matches —
  # it retries once at min(min_leverage_cap, target) instead of leaving the
  # symbol at its dangerous default. Set to the lowest leverage you know your
  # sub-accounts can take (5x on Binance); if an account is restricted below
  # this, the retry still fails and the symbol is logged for manual fixing.
  min_leverage_cap: int = _opt(5, "MIN_LEVERAGE_CAP", "min_leverage_cap")
  use_custom_leverage: bool = _opt(
    False, "USE_CUSTOM_LEVERAGE", "use_custom_leverage"
  )  # If true, force use custom leverage instead of broker crypto leverage initialization

  @field_validator("leverage_init_symbols", mode="before")
  @classmethod
  def parse_leverage_init_symbols(cls, v):
    if isinstance(v, list):
      return [s for s in (str(i).strip() for i in v) if s]
    if isinstance(v, str):
      return [s for s in (i.strip() for i in v.split(",")) if s]
    return v


class StrategySettings(BaseSettings):
  """Per-strategy execution configuration."""

  model_config = _GROUP_CONFIG

  slippage_deviation: int = _opt(100, "SLIPPAGE_DEVIATION", "slippage_deviation")
  # Staleness guard (worker.gateways.guard.stale_signal_rejection). Entries are
  # market orders, so they fill at the live quote, never at the price the signal
  # was built on. These bound how far the market may have moved before the entry
  # is rejected instead of placed. An entry whose live quote has already passed
  # the signal's own tp2 or sl is always rejected, with no threshold to tune.
  #
  # Drift measured as a percentage of the signal's entry-to-SL distance ("R").
  # Preferred over a raw price percentage because it is self-normalizing: 50%
  # means the same thing on BTCUSDT at 100,000 as on EURUSD at 1.08, and it
  # tightens automatically for a tight-stop signal. 0 disables the check.
  max_entry_drift_r_percent: float = _opt(
    50.0, "MAX_ENTRY_DRIFT_R_PERCENT", "max_entry_drift_r_percent"
  )
  # Fallback for a signal carrying no SL, where there is no risk distance to
  # measure against: drift as a plain percentage of the signal price. 0 disables.
  max_entry_drift_percent: float = _opt(
    0.5, "MAX_ENTRY_DRIFT_PERCENT", "max_entry_drift_percent"
  )
  # Per-strategy MT5 magic numbers. The broker now owns this mapping and pushes it
  # on connect in the WORKER_CONNECTED_ACK (``strategy_magic_map``), scoped to the
  # strategies this worker subscribes to — BaseSignalProcessor.
  # _apply_strategy_magic_map overwrites this key in the live settings dict on
  # receipt. The ``STRATEGY_MAGIC_MAP`` env var is kept only as an offline/legacy
  # fallback default (empty by default); a broker push replaces it.
  magic_map: dict[str, int] = _opt({}, "STRATEGY_MAGIC_MAP", "strategy_magic_map")
  use_custom_position_tp1_percent: bool = _opt(
    False, "USE_CUSTOM_POSITION_TP1_PERCENT", "use_custom_position_tp1_percent"
  )  # If true, use custom TP1 percentage instead of signal's tp1_percent
  position_tp1_percent: Optional[float] = _opt(
    50, "POSITION_TP1_PERCENT", "position_tp1_percent"
  )  # Only defined if you want custom TP1 percentage, otherwise default is tp1_percent in signal
  # When True, TP1 partial-closes and then moves the stop to breakeven (entry).
  # When False, TP1 only partial-closes and leaves the original entry SL
  # untouched, so the runner keeps its initial protection.
  tp1_move_sl_to_breakeven: Optional[bool] = _opt(
    None, "TP1_MOVE_SL_TO_BREAKEVEN", "tp1_move_sl_to_breakeven"
  )  # Only defined if you want to move SL to breakeven when TP1 is hit, otherwise default is move_sl_to_be in signal


class RiskSettings(BaseSettings):
  """Initial capital + risk management."""

  model_config = _GROUP_CONFIG

  capital: float = _opt(1000, "CAPITAL", "capital")
  capital_currency: str = _opt("USD", "CAPITAL_CURRENCY", "capital_currency")
  volume_decision_enabled: bool = _opt(
    True, "VOLUME_DECISION_ENABLED", "volume_decision_enabled"
  )
  use_custom_risk_percentage: bool = _opt(
    False, "USE_CUSTOM_RISK_PERCENTAGE", "use_custom_risk_percentage"
  )  # If true, use custom risk percentage instead of signal's risk percentage
  risk_percentage: float = _opt(
    2.0, "RISK_PERCENTAGE", "risk_percentage"
  )  # Risk 2% of capital per trade
  use_account_equity: bool = _opt(
    False, "USE_ACCOUNT_EQUITY", "use_account_equity"
  )  # If true, use account equity instead of initial capital for entry volume calculation
  # Maximum number of concurrently open orders (OPENED/TP1 positions) this worker
  # may hold. A new entry (LONG/SHORT) that would exceed the cap is not sent to the
  # broker: it is recorded with status REJECTED, reported to the broker on the TRADE
  # subject (also REJECTED) and notified — but no order is placed. A re-entry/scale-in
  # on a symbol the strategy already holds replaces that position rather than opening
  # a new slot, so it never counts against the cap. Set to 0 to disable the limit.
  max_open_orders: int = _opt(5, "MAX_OPEN_ORDERS", "max_open_orders")


class TelegramSettings(BaseSettings):
  """Telegram notification + error-log hook."""

  model_config = _GROUP_CONFIG

  enabled: bool = _req("TELEGRAM_ENABLED", "telegram_enabled")
  bot_token: SecretStr = _req("TELEGRAM_BOT_TOKEN", "telegram_bot_token")
  # Every chat id in this group — chat_id, chat_channel_id and log_chat_id
  # below — is resolved through ``notification_service.parse_chat_targets``, so
  # each accepts a comma-separated list of chats, and any entry may address a
  # topic inside a supergroup that has Topics enabled by suffixing the topic
  # id: ``-1002173777783_924584`` sends with ``message_thread_id`` so the
  # message lands in that topic instead of General.
  chat_id: str = _req(
    "TELEGRAM_WORKER_LOG_CHAT_IDS", "telegram_chat_id"
  )  # management: NATS events, service start/stop, MT5 health
  # NoDecode (see crypto leverage_init_symbols): the documented unquoted
  # comma-separated form (-100123,-100987) is not valid JSON, so let
  # parse_channel_ids split the raw string instead of pydantic JSON-decoding it.
  chat_channel_id: Annotated[list[str], NoDecode] = _opt(
    [], "TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS", "telegram_chat_channel_id"
  )  # signals: order fills/failures, terminal closes

  # ── Telegram error-log hook ──────────────────────────────────────
  # When enabled (and enabled is true), log records at ERROR level or above are
  # forwarded to Telegram. The dedicated log bot/chat is kept separate from the
  # main bot so an outage/ban on one never affects the other; both fall back to
  # bot_token / chat_id when left empty.
  log_errors_enabled: bool = _opt(
    False, "TELEGRAM_LOG_ERRORS_ENABLED", "telegram_log_errors_enabled"
  )
  log_dedup_window: int = _opt(
    60, "TELEGRAM_LOG_DEDUP_WINDOW", "telegram_log_dedup_window"
  )  # seconds — suppress identical messages
  log_chat_id: str = _opt("", "TELEGRAM_LOG_CHAT_ID", "telegram_log_chat_id")
  log_bot_token: Optional[SecretStr] = _opt(
    None, "TELEGRAM_LOG_BOT_TOKEN", "telegram_log_bot_token"
  )

  @field_validator("chat_channel_id", mode="before")
  @classmethod
  def parse_channel_ids(cls, v):
    if isinstance(v, list):
      return [i for i in v if i]
    if isinstance(v, str):
      return [i.strip() for i in v.split(",") if i.strip()]
    if isinstance(v, int):
      return [str(v)]
    return v


class DatabaseSettings(BaseSettings):
  """Local SQLite store."""

  model_config = _GROUP_CONFIG

  file: str = _opt("worker_data.sqlite", "DB_FILE", "db_file")


class WebSettings(BaseSettings):
  """FastAPI / web server."""

  model_config = _GROUP_CONFIG

  host: str = _opt("0.0.0.0", "APP_HOST", "app_host")
  port: int = _opt(8000, "APP_PORT", "app_port")


class BrokerSettings(BaseSettings):
  """Broker HTTP API."""

  model_config = _GROUP_CONFIG

  api_url: str = _req("BROKER_API_URL", "broker_api_url")
  api_key: SecretStr = _req("BROKER_API_KEY", "broker_api_key")


class Settings(BaseSettings):
  """Application configuration loaded from environment variables and .env file.

  Related settings are grouped under nested ``<specific>Settings`` objects
  (``settings.nats.url``, ``settings.telegram.enabled``, …). Each group reads the
  same flat environment variables as before, and :meth:`flat_dump` reproduces the
  legacy flat ``settings_dict`` handed to worker subprocesses.
  """

  model_config = SettingsConfigDict(
    env_file=".env", env_file_encoding="utf-8", extra="ignore"
  )

  market_type: MarketTypeEnum = MarketTypeEnum.FOREX

  # Identify — derived in _validate_market_requirements as
  # "<market_type>-<gateway>-<MT5_LOGIN|CRYPTO_ACCOUNT_ID>" once the per-market
  # credentials are validated, so it is never read from .env and can't drift
  # from the credential it identifies.
  account_id: str = ""

  logging: LoggingSettings = Field(default_factory=LoggingSettings)
  nats: NatsSettings = Field(default_factory=NatsSettings)
  forex: ForexSettings = Field(default_factory=ForexSettings)
  crypto: CryptoSettings = Field(default_factory=CryptoSettings)
  strategy: StrategySettings = Field(default_factory=StrategySettings)
  risk: RiskSettings = Field(default_factory=RiskSettings)
  telegram: TelegramSettings = Field(default_factory=TelegramSettings)
  database: DatabaseSettings = Field(default_factory=DatabaseSettings)
  web: WebSettings = Field(default_factory=WebSettings)
  broker: BrokerSettings = Field(default_factory=BrokerSettings)

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
          ("MT5_SERVER", self.forex.server),
          ("MT5_LOGIN", self.forex.login),
          ("MT5_PASSWORD", self.forex.password),
        )
        if not value
      ]
      if missing:
        raise ValueError(f"MARKET_TYPE=FOREX requires: {', '.join(missing)}")
      self.account_id = (
        f"{self.market_type.value}-{self.forex.platform.value}-{self.forex.login}"
      )
    elif self.market_type == MarketTypeEnum.CRYPTO:
      if self.crypto.exchange == CryptoExchangeEnum.BINANCE:
        missing = [
          name
          for name, value in (
            ("CRYPTO_QUOTE_ASSET", self.crypto.quote_asset),
            ("CRYPTO_API_KEY", self.crypto.api_key),
            ("CRYPTO_API_SECRET", self.crypto.api_secret),
            ("CRYPTO_ACCOUNT_ID", self.crypto.account_id),
          )
          if not value
        ]
        if missing:
          raise ValueError(
            f"MARKET_TYPE=CRYPTO (BINANCE) requires: {', '.join(missing)}"
          )
        self.account_id = f"{self.market_type.value}-{self.crypto.exchange.value}-{self.crypto.account_id}"
    return self

  @model_validator(mode="after")
  def _validate_multi_strategy_magics(self):
    """Reject duplicate magic numbers when multi-strategy-per-symbol is on.

    With the toggle enabled, the magic number is the *only* thing separating two
    strategies that share a symbol: `get_open_positions(symbol, strategy)`
    filters on it, so two strategies mapped to the same magic would see — and
    force-close, on the next entry — each other's positions. Elsewhere a
    duplicate magic is merely a misconfiguration; here it silently defeats the
    isolation the feature is built on, so it fails fast at startup instead.
    """
    if not (
      self.market_type == MarketTypeEnum.FOREX
      and self.forex.allow_multi_strategy_per_symbol
    ):
      return self
    seen: dict[int, str] = {}
    collisions: list[str] = []
    for strategy, magic in self.strategy.magic_map.items():
      if magic in seen:
        collisions.append(f"{seen[magic]} + {strategy} → {magic}")
      else:
        seen[magic] = strategy
    if collisions:
      raise ValueError(
        "FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL=true requires a unique magic "
        f"number per strategy in STRATEGY_MAGIC_MAP; duplicates: {'; '.join(collisions)}"
      )
    return self

  def flat_dump(self) -> dict:
    """Reproduce the legacy flat ``settings_dict`` (original flat keys, with
    ``SecretStr`` / enum members preserved) that worker subprocesses, gateways
    and presenters consume via ``settings_dict.get("<flat_key>")``.

    Grouping the fields into nested models changed ``model_dump`` to a nested
    shape; this flattens it back so no downstream consumer had to change."""
    flat: dict = {"market_type": self.market_type, "account_id": self.account_id}
    for group in _FLAT_GROUPS:
      flat.update(getattr(self, group).model_dump(by_alias=True))
    return flat


settings = Settings()
