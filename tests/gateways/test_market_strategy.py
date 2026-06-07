from dataclasses import replace
from types import SimpleNamespace

import pytest
from helpers import make_signal

from worker.gateways.market_strategy import (
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
    self.calls.append(("get_open", strategy))
    return list(self._positions)

  def normalize_volume(self, symbol, volume):
    return round(volume, 2)

  def convert_quantity_to_lots(self, symbol, quantity):
    return quantity / 100.0

  def partial_close_position(self, symbol, close_volume, position_ticket=None, strategy=None):
    self.calls.append(("partial", close_volume, position_ticket, strategy))
    return {"success": True, "volume": close_volume, "ticket": 1}

  def update_position_sl(self, symbol, new_sl, position_ticket=None, strategy=None):
    self.calls.append(("sl", new_sl, position_ticket, strategy))
    return {"success": True, "new_sl": new_sl}

  def open_position(self, signal):
    return {"success": True}

  def close_all_positions(self, symbol, reason="CLOSE", strategy=None):
    return {"success": True, "reason": reason, "strategy": strategy}


def test_handle_tp1_volume_decision_mode(config):
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, config)  # position_tp1_percent=30
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1))
  assert res["success"] is True
  # 1.0 * 30% = 0.3 normalized, scoped to the signal's strategy
  assert ("partial", 0.3, 7, "strat-1") in ex.calls
  # SL moved to breakeven (price_open)
  assert ("sl", 2000.0, 7, "strat-1") in ex.calls


def test_handle_tp1_payload_quantity_mode(config):
  cfg = replace(config, volume_decision_enabled=False)
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, quantity=50))
  # 50 / 100 = 0.5
  assert ("partial", 0.5, 7, "strat-1") in ex.calls
  assert res["success"] is True


def test_handle_tp1_no_positions(config):
  ex = FakeExecutor(positions=[])
  market = ForexMarket(ex, config)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1))
  assert res["success"] is False


def test_handle_full_close_passes_reason(config):
  ex = FakeExecutor()
  market = ForexMarket(ex, config)
  res = market.handle_full_close(make_signal(SignalActionEnum.SL))
  assert res["reason"] == "SL"


def test_factory_requires_executor(config):
  with pytest.raises(ValueError):
    MarketStrategyFactory.create(MarketTypeEnum.FOREX, executor=None, config=config)


def test_factory_requires_config():
  with pytest.raises(ValueError):
    MarketStrategyFactory.create(MarketTypeEnum.FOREX, executor=FakeExecutor(), config=None)


def test_factory_builds_forex_market(config):
  market = MarketStrategyFactory.create(
    MarketTypeEnum.FOREX, executor=FakeExecutor(), config=config
  )
  assert isinstance(market, ForexMarket)
