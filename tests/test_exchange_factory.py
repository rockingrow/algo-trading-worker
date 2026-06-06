"""Tests for the CEX factory."""

import pytest

from worker.crypto.base import BaseExchangeGateway
from worker.crypto.factory import ExchangeFactory
from worker.settings import CryptoExchangeEnum


def test_factory_builds_binance_gateway():
  gw = ExchangeFactory.create(
    {
      "crypto_exchange": CryptoExchangeEnum.BINANCE,
      "binance_api_key": "k",
      "binance_api_secret": "s",
      "binance_testnet": True,
    }
  )
  assert isinstance(gw, BaseExchangeGateway)
  assert gw.name == "BINANCE"


def test_factory_accepts_string_exchange():
  gw = ExchangeFactory.create(
    {"crypto_exchange": "BINANCE", "binance_api_key": "k", "binance_api_secret": "s"}
  )
  assert gw.name == "BINANCE"


def test_factory_rejects_unknown_exchange():
  with pytest.raises(ValueError):
    ExchangeFactory.create({"crypto_exchange": "KRAKEN"})
