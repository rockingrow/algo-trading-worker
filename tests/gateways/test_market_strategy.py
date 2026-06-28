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


def test_handle_tp1_keeps_original_sl_when_breakeven_disabled(config):
  cfg = replace(config, tp1_move_sl_to_breakeven=False)
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1))
  assert res["success"] is True
  # Partial close still happens...
  assert ("partial", 0.3, 7, "strat-1") in ex.calls
  # ...but the SL is left untouched (no breakeven move).
  assert not any(call[0] == "sl" for call in ex.calls)
  assert "sl_update" not in res


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


# ── Signal-level tp1_percent / move_sl_to_be fallback ────────────────────── #

def test_handle_tp1_uses_signal_tp1_percent_when_config_is_none(config):
  """signal.tp1_percent is used when config.position_tp1_percent is None."""
  cfg = replace(config, position_tp1_percent=None)
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, tp1_percent=40.0))
  assert res["success"] is True
  # 1.0 * 40% = 0.4
  assert ("partial", 0.4, 7, "strat-1") in ex.calls


def test_handle_tp1_falls_back_to_config_when_signal_has_no_tp1_percent(config):
  """When use_custom=False and signal has no tp1_percent, config.position_tp1_percent is used."""
  cfg = replace(config, use_custom_position_tp1_percent=False, position_tp1_percent=50.0)
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1))
  assert res["success"] is True
  # no signal tp1_percent → fallback to config: 1.0 * 50% = 0.5
  assert ("partial", 0.5, 7, "strat-1") in ex.calls


def test_handle_tp1_signal_tp1_percent_used_when_not_custom(config):
  """use_custom_position_tp1_percent=False: signal.tp1_percent wins over config."""
  cfg = replace(config, use_custom_position_tp1_percent=False)  # config has position_tp1_percent=30
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, tp1_percent=75.0))
  assert res["success"] is True
  # signal wins: 1.0 * 75% = 0.75, not 30%
  assert ("partial", 0.75, 7, "strat-1") in ex.calls
  assert not any(c == ("partial", 0.3, 7, "strat-1") for c in ex.calls)


def test_handle_tp1_custom_tp1_percent_overrides_signal(config):
  """use_custom_position_tp1_percent=True: config.position_tp1_percent wins over signal.tp1_percent."""
  cfg = replace(config, use_custom_position_tp1_percent=True)  # config has position_tp1_percent=30
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, tp1_percent=75.0))
  assert res["success"] is True
  # config wins: 1.0 * 30% = 0.3, not 75%
  assert ("partial", 0.3, 7, "strat-1") in ex.calls
  assert not any(c == ("partial", 0.75, 7, "strat-1") for c in ex.calls)


def test_handle_tp1_uses_signal_move_sl_to_be_when_config_is_none(config):
  """signal.move_sl_to_be=True triggers breakeven move when config is None."""
  cfg = replace(config, tp1_move_sl_to_breakeven=None)
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, move_sl_to_be=True))
  assert res["success"] is True
  assert ("sl", 2000.0, 7, "strat-1") in ex.calls


def test_handle_tp1_defaults_no_breakeven_when_both_none(config):
  """Default move_sl_to_be=False when neither config nor signal provides one."""
  cfg = replace(config, tp1_move_sl_to_breakeven=None)
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1))
  assert res["success"] is True
  assert not any(call[0] == "sl" for call in ex.calls)


def test_handle_tp1_config_move_sl_overrides_signal(config):
  """config.tp1_move_sl_to_breakeven=False wins over signal.move_sl_to_be=True."""
  cfg = replace(config, tp1_move_sl_to_breakeven=False)
  pos = SimpleNamespace(ticket=7, volume=1.0, price_open=2000.0)
  ex = FakeExecutor(positions=[pos])
  market = ForexMarket(ex, cfg)
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, move_sl_to_be=True))
  assert res["success"] is True
  # config.False wins — no SL update despite signal saying True
  assert not any(call[0] == "sl" for call in ex.calls)


def test_factory_builds_forex_market(config):
  market = MarketStrategyFactory.create(
    MarketTypeEnum.FOREX, executor=FakeExecutor(), config=config
  )
  assert isinstance(market, ForexMarket)
