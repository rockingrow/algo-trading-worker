"""
tests/gateways/test_execution_config.py
───────────────────────────────────────
``ExecutionConfig.from_dict`` resolves the multi-strategy-per-symbol toggle from
the *active market's* env var, so a FOREX worker never picks up the crypto flag
(and vice-versa).
"""

import pytest

from worker.gateways.config import ExecutionConfig
from worker.settings import MarketTypeEnum


@pytest.mark.parametrize("market", [MarketTypeEnum.FOREX, "FOREX", None])
def test_forex_reads_the_forex_multi_strategy_flag(market):
  """None → the default market (FOREX), matching the processor's own default."""
  cfg = ExecutionConfig.from_dict(
    {
      "market_type": market,
      "forex_allow_multi_strategy_per_symbol": True,
      "crypto_allow_multi_strategy_per_symbol": False,
    }
  )
  assert cfg.allow_multi_strategy_per_symbol is True


def test_forex_ignores_the_crypto_multi_strategy_flag():
  cfg = ExecutionConfig.from_dict(
    {
      "market_type": MarketTypeEnum.FOREX,
      "forex_allow_multi_strategy_per_symbol": False,
      "crypto_allow_multi_strategy_per_symbol": True,
    }
  )
  assert cfg.allow_multi_strategy_per_symbol is False


@pytest.mark.parametrize("market", [MarketTypeEnum.CRYPTO, "CRYPTO"])
def test_crypto_reads_the_crypto_multi_strategy_flag(market):
  cfg = ExecutionConfig.from_dict(
    {
      "market_type": market,
      "forex_allow_multi_strategy_per_symbol": True,
      "crypto_allow_multi_strategy_per_symbol": True,
    }
  )
  assert cfg.allow_multi_strategy_per_symbol is True


def test_crypto_ignores_the_forex_multi_strategy_flag():
  cfg = ExecutionConfig.from_dict(
    {
      "market_type": MarketTypeEnum.CRYPTO,
      "forex_allow_multi_strategy_per_symbol": True,
      "crypto_allow_multi_strategy_per_symbol": False,
    }
  )
  assert cfg.allow_multi_strategy_per_symbol is False


def test_defaults_to_disabled_when_nothing_is_configured():
  assert ExecutionConfig.from_dict({}).allow_multi_strategy_per_symbol is False
