"""Per-market credential validation in Settings.

Ensures a CRYPTO deployment does not require MT5 credentials and a FOREX
deployment does not boot without them — the mechanism that keeps each market
free of the other's dependencies.
"""

import pytest
from pydantic import SecretStr, ValidationError

from worker.settings import (
  CryptoExchangeEnum,
  ForexPlatformEnum,
  MarketTypeEnum,
  Settings,
)

# The exact flat keys the legacy (pre-grouping) ``settings.model_dump()`` emitted.
# ``Settings.flat_dump()`` must keep reproducing this set so every downstream
# consumer that reads ``settings_dict.get("<flat_key>")`` stays untouched.
_LEGACY_FLAT_KEYS = {
  "notification_mode",
  "nats_url",
  "nats_token",
  "nats_subjects",
  "market_type",
  "account_id",
  "forex_platform",
  "mt5_server",
  "mt5_login",
  "mt5_password",
  "mt5_path",
  "mt5_name",
  "forex_allow_multi_strategy_per_symbol",
  "crypto_exchange",
  "crypto_quote_asset",
  "crypto_api_key",
  "crypto_api_secret",
  "crypto_account_id",
  "crypto_testnet",
  "crypto_hedge_mode",
  "crypto_allow_multi_strategy_per_symbol",
  "crypto_leverage_init_symbols",
  "max_leverage_cap",
  "min_leverage_cap",
  "use_custom_leverage",
  "slippage_deviation",
  "max_entry_drift_r_percent",
  "max_entry_drift_percent",
  "strategy_magic_map",
  "use_custom_position_tp1_percent",
  "position_tp1_percent",
  "tp1_move_sl_to_breakeven",
  "capital",
  "capital_currency",
  "volume_decision_enabled",
  "use_custom_risk_percentage",
  "risk_percentage",
  "use_account_equity",
  "max_open_orders",
  "telegram_enabled",
  "telegram_bot_token",
  "telegram_chat_id",
  "telegram_chat_channel_id",
  "telegram_cycle_enabled",
  "telegram_log_errors_enabled",
  "telegram_log_dedup_window",
  "telegram_log_chat_id",
  "telegram_log_bot_token",
  "db_file",
  "log_level",
  "app_host",
  "app_port",
  "broker_api_url",
  "broker_api_key",
}


def _forex_env(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "FOREX")
  monkeypatch.setenv("MT5_SERVER", "Exness-MT5Trial6")
  monkeypatch.setenv("MT5_LOGIN", "413652379")
  monkeypatch.setenv("MT5_PASSWORD", "pw")


def test_flat_dump_reproduces_legacy_flat_schema(monkeypatch):
  _forex_env(monkeypatch)
  flat = Settings(_env_file=None).flat_dump()
  assert set(flat) == _LEGACY_FLAT_KEYS


def test_flat_dump_preserves_secret_and_enum_types(monkeypatch):
  _forex_env(monkeypatch)
  flat = Settings(_env_file=None).flat_dump()
  # Secrets stay wrapped (consumers call .get_secret_value()).
  assert isinstance(flat["mt5_password"], SecretStr)
  assert isinstance(flat["telegram_bot_token"], SecretStr)
  # Enums stay as members (factories accept the member or its value).
  assert flat["market_type"] == MarketTypeEnum.FOREX
  assert flat["forex_platform"] == ForexPlatformEnum.MT5
  assert flat["crypto_exchange"] == CryptoExchangeEnum.BINANCE


def test_flat_dump_values_match_nested_access(monkeypatch):
  _forex_env(monkeypatch)
  s = Settings(_env_file=None)
  flat = s.flat_dump()
  assert flat["nats_url"] == s.nats.url
  assert flat["mt5_login"] == s.forex.login
  assert flat["max_open_orders"] == s.risk.max_open_orders
  assert flat["app_port"] == s.web.port
  assert flat["account_id"] == s.account_id


def test_crypto_requires_binance_keys(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "CRYPTO")
  monkeypatch.delenv("CRYPTO_API_KEY", raising=False)
  monkeypatch.delenv("CRYPTO_API_SECRET", raising=False)
  with pytest.raises(ValidationError):
    Settings(_env_file=None)


def test_crypto_ok_with_keys_and_no_mt5(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "CRYPTO")
  monkeypatch.setenv("CRYPTO_API_KEY", "key")
  monkeypatch.setenv("CRYPTO_API_SECRET", "secret")
  monkeypatch.setenv("CRYPTO_ACCOUNT_ID", "acct-1")
  # MT5 creds intentionally absent — must NOT be required for crypto.
  monkeypatch.delenv("MT5_SERVER", raising=False)
  monkeypatch.delenv("MT5_LOGIN", raising=False)
  monkeypatch.delenv("MT5_PASSWORD", raising=False)
  s = Settings(_env_file=None)
  assert s.market_type == MarketTypeEnum.CRYPTO
  assert s.crypto.exchange.value == "BINANCE"
  assert s.forex.server is None


def test_crypto_account_id_derived_from_binance_account_id(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "CRYPTO")
  monkeypatch.setenv("CRYPTO_API_KEY", "key")
  monkeypatch.setenv("CRYPTO_API_SECRET", "secret")
  monkeypatch.setenv("CRYPTO_ACCOUNT_ID", "acct-1")
  # ACCOUNT_ID is never read from .env — set it anyway to prove it's ignored.
  monkeypatch.setenv("ACCOUNT_ID", "should-be-overwritten")
  s = Settings(_env_file=None)
  assert s.account_id == "CRYPTO-BINANCE-acct-1"


def test_forex_requires_mt5(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "FOREX")
  monkeypatch.delenv("MT5_SERVER", raising=False)
  monkeypatch.delenv("MT5_LOGIN", raising=False)
  monkeypatch.delenv("MT5_PASSWORD", raising=False)
  with pytest.raises(ValidationError):
    Settings(_env_file=None)


def test_forex_account_id_derived_from_mt5_login(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "FOREX")
  monkeypatch.setenv("MT5_SERVER", "Exness-MT5Trial6")
  monkeypatch.setenv("MT5_LOGIN", "413652379")
  monkeypatch.setenv("MT5_PASSWORD", "pw")
  s = Settings(_env_file=None)
  assert s.account_id == "FOREX-MT5-413652379"


def test_max_open_orders_defaults_to_five(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "FOREX")
  monkeypatch.setenv("MT5_SERVER", "Exness-MT5Trial6")
  monkeypatch.setenv("MT5_LOGIN", "413652379")
  monkeypatch.setenv("MT5_PASSWORD", "pw")
  monkeypatch.delenv("MAX_OPEN_ORDERS", raising=False)
  assert Settings(_env_file=None).risk.max_open_orders == 5


def test_max_open_orders_overridable(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "FOREX")
  monkeypatch.setenv("MT5_SERVER", "Exness-MT5Trial6")
  monkeypatch.setenv("MT5_LOGIN", "413652379")
  monkeypatch.setenv("MT5_PASSWORD", "pw")
  monkeypatch.setenv("MAX_OPEN_ORDERS", "10")
  assert Settings(_env_file=None).risk.max_open_orders == 10


# ── Multi-strategy-per-symbol requires unique magics ─────────────────────── #


def test_duplicate_magics_rejected_when_multi_strategy_enabled(monkeypatch):
  """The magic number is the only isolation between strategies sharing a symbol,
  so a duplicate must fail fast rather than silently merge two strategies."""
  _forex_env(monkeypatch)
  monkeypatch.setenv("FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL", "true")
  monkeypatch.setenv("STRATEGY_MAGIC_MAP", '{"MT5_GOLD": 111, "MT5_GOLD_SHORT": 111}')
  with pytest.raises(ValidationError, match="unique magic"):
    Settings(_env_file=None)


def test_unique_magics_accepted_when_multi_strategy_enabled(monkeypatch):
  _forex_env(monkeypatch)
  monkeypatch.setenv("FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL", "true")
  monkeypatch.setenv("STRATEGY_MAGIC_MAP", '{"MT5_GOLD": 111, "MT5_GOLD_SHORT": 222}')
  s = Settings(_env_file=None)
  assert s.forex.allow_multi_strategy_per_symbol is True


def test_duplicate_magics_tolerated_when_multi_strategy_disabled(monkeypatch):
  """Unchanged for existing deployments: the check only guards the new mode."""
  _forex_env(monkeypatch)
  monkeypatch.setenv("FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL", "false")
  monkeypatch.setenv("STRATEGY_MAGIC_MAP", '{"MT5_GOLD": 111, "MT5_GOLD_SHORT": 111}')
  assert Settings(_env_file=None).forex.allow_multi_strategy_per_symbol is False
