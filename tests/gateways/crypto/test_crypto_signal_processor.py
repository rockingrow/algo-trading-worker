"""Tests for CRYPTO-specific signal-processor hooks."""

from worker.gateways.crypto.signal_processor import CryptoSignalProcessor
from worker.settings import CryptoExchangeEnum


def _proc(settings: dict) -> CryptoSignalProcessor:
  # Bypass __init__ (which builds a live gateway); _account_id only reads .settings.
  proc = object.__new__(CryptoSignalProcessor)
  proc.settings = settings
  return proc


def test_account_id_uses_enum_value_not_repr():
  # str(CryptoExchangeEnum.BINANCE) would leak "CryptoExchangeEnum.BINANCE";
  # the published account_id must be the bare exchange name.
  proc = _proc({"crypto_exchange": CryptoExchangeEnum.BINANCE})
  assert proc._account_id == "BINANCE"


def test_account_id_accepts_plain_string():
  proc = _proc({"crypto_exchange": "BINANCE"})
  assert proc._account_id == "BINANCE"


def test_account_id_defaults_when_missing():
  proc = _proc({})
  assert proc._account_id == "CRYPTO"
