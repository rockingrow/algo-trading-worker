"""Tests for the CRYPTO branch of the market-strategy factory + shared logic."""

from types import SimpleNamespace

import pytest
from helpers import make_signal

from worker.gateways.market_strategy import (
  CryptoMarket,
  ForexMarket,
  MarketStrategyFactory,
)
from worker.schemas.signal_schema import SignalActionEnum
from worker.settings import MarketTypeEnum


class FakeExecutor:
  def __init__(self, positions=None):
    self._positions = positions if positions is not None else []
    self.calls = []

  def get_open_positions(self, symbol, strategy=None):
    return list(self._positions)

  def normalize_volume(self, symbol, volume):
    return round(volume, 3)

  def convert_quantity_to_lots(self, symbol, quantity):
    return quantity

  def partial_close_position(self, symbol, close_volume, position_ticket=None, strategy=None):
    self.calls.append(("partial", close_volume, position_ticket, strategy))
    return {"success": True, "volume": close_volume}

  def update_position_sl(self, symbol, new_sl, position_ticket=None, strategy=None):
    self.calls.append(("sl", new_sl, position_ticket, strategy))
    return {"success": True, "new_sl": new_sl}

  def open_position(self, signal):
    return {"success": True}

  def close_all_positions(self, symbol, reason="CLOSE", strategy=None):
    return {"success": True, "reason": reason}


def test_factory_builds_crypto_market(config):
  market = MarketStrategyFactory.create(
    MarketTypeEnum.CRYPTO, executor=FakeExecutor(), config=config
  )
  assert isinstance(market, CryptoMarket)
  # CryptoMarket shares the executor-backed base, not a Forex instance.
  assert not isinstance(market, ForexMarket)


def test_factory_crypto_requires_executor(config):
  with pytest.raises(ValueError):
    MarketStrategyFactory.create(MarketTypeEnum.CRYPTO, executor=None, config=config)


def test_crypto_handle_tp1_partial_then_breakeven(config):
  pos = SimpleNamespace(ticket=7, volume=0.02, price_open=30000.0)
  ex = FakeExecutor(positions=[pos])
  market = MarketStrategyFactory.create(
    MarketTypeEnum.CRYPTO, executor=ex, config=config
  )
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, symbol="BTCUSD"))
  assert res["success"] is True
  # 0.02 * 30% = 0.006 partial close, SL moved to entry (breakeven)
  assert ("partial", 0.006, 7, "strat-1") in ex.calls
  assert ("sl", 30000.0, 7, "strat-1") in ex.calls


def test_crypto_full_close_passes_reason(config):
  market = MarketStrategyFactory.create(
    MarketTypeEnum.CRYPTO, executor=FakeExecutor(), config=config
  )
  res = market.handle_full_close(make_signal(SignalActionEnum.SL, symbol="BTCUSD"))
  assert res["reason"] == "SL"
