"""Tests for the CEX factory."""

import pytest
from pydantic import SecretStr

from worker.gateways.crypto.base import BaseExchangeGateway
from worker.gateways.crypto.factory import ExchangeFactory
from worker.settings import CryptoExchangeEnum


def test_factory_builds_binance_gateway():
  # Keys arrive as SecretStr: the factory consumes settings.model_dump(), which
  # preserves SecretStr (not plain str) and calls .get_secret_value() on them.
  gw = ExchangeFactory.create(
    {
      "crypto_exchange": CryptoExchangeEnum.BINANCE,
      "binance_api_key": SecretStr("k"),
      "binance_api_secret": SecretStr("s"),
      "binance_testnet": True,
    }
  )
  assert isinstance(gw, BaseExchangeGateway)
  assert gw.name == "BINANCE"


def test_factory_accepts_string_exchange():
  gw = ExchangeFactory.create(
    {
      "crypto_exchange": "BINANCE",
      "binance_api_key": SecretStr("k"),
      "binance_api_secret": SecretStr("s"),
    }
  )
  assert gw.name == "BINANCE"


def test_factory_rejects_unknown_exchange():
  with pytest.raises(ValueError):
    ExchangeFactory.create({"crypto_exchange": "KRAKEN"})
