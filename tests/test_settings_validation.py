"""Per-market credential validation in Settings.

Ensures a CRYPTO deployment does not require MT5 credentials and a FOREX
deployment does not boot without them — the mechanism that keeps each market
free of the other's dependencies.
"""

import pytest
from pydantic import ValidationError

from worker.settings import MarketTypeEnum, Settings


def test_crypto_requires_binance_keys(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "CRYPTO")
  monkeypatch.delenv("BINANCE_API_KEY", raising=False)
  monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
  with pytest.raises(ValidationError):
    Settings(_env_file=None)


def test_crypto_ok_with_keys_and_no_mt5(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "CRYPTO")
  monkeypatch.setenv("BINANCE_API_KEY", "key")
  monkeypatch.setenv("BINANCE_API_SECRET", "secret")
  # MT5 creds intentionally absent — must NOT be required for crypto.
  monkeypatch.delenv("MT5_SERVER", raising=False)
  monkeypatch.delenv("MT5_LOGIN", raising=False)
  monkeypatch.delenv("MT5_PASSWORD", raising=False)
  s = Settings(_env_file=None)
  assert s.market_type == MarketTypeEnum.CRYPTO
  assert s.crypto_exchange.value == "BINANCE"
  assert s.mt5_server is None


def test_forex_requires_mt5(monkeypatch):
  monkeypatch.setenv("MARKET_TYPE", "FOREX")
  monkeypatch.delenv("MT5_SERVER", raising=False)
  monkeypatch.delenv("MT5_LOGIN", raising=False)
  monkeypatch.delenv("MT5_PASSWORD", raising=False)
  with pytest.raises(ValidationError):
    Settings(_env_file=None)
