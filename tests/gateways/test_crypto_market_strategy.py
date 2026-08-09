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
  def __init__(self, positions=None, sl_ok=True, failsafe_ok=True):
    self._positions = positions if positions is not None else []
    self._sl_ok = sl_ok
    self._failsafe_ok = failsafe_ok
    self.calls = []

  def get_open_positions(self, symbol, strategy=None):
    return list(self._positions)

  def normalize_volume(self, symbol, volume):
    return round(volume, 3)

  def convert_quantity_to_lots(self, symbol, quantity):
    return quantity

  def partial_close_position(
    self, symbol, close_volume, position_ticket=None, strategy=None
  ):
    self.calls.append(("partial", close_volume, position_ticket, strategy))
    return {"success": True, "volume": close_volume}

  def update_position_sl(self, symbol, new_sl, position_ticket=None, strategy=None):
    self.calls.append(("sl", new_sl, position_ticket, strategy))
    if self._sl_ok:
      return {"success": True, "new_sl": new_sl}
    return {"success": False, "comment": "Precision is over the maximum"}

  def open_position(self, signal):
    return {"success": True}

  def close_all_positions(self, symbol, reason="CLOSE", strategy=None):
    self.calls.append(("close_all", reason, strategy))
    return {
      "success": self._failsafe_ok,
      "reason": reason,
      "volume": 0.014,
      "price": 30000.0,
      "comment": "" if self._failsafe_ok else "exchange error",
    }


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


def test_crypto_tp1_failsafe_closes_when_breakeven_sl_fails(config):
  """If the breakeven SL can't be set, the unprotected position is force-closed."""
  pos = SimpleNamespace(ticket=7, volume=0.02, price_open=30000.0)
  ex = FakeExecutor(positions=[pos], sl_ok=False, failsafe_ok=True)
  market = MarketStrategyFactory.create(
    MarketTypeEnum.CRYPTO, executor=ex, config=config
  )
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, symbol="BTCUSD"))

  # Partial filled, breakeven SL attempted, then emergency close fired.
  assert ("partial", 0.006, 7, "strat-1") in ex.calls
  assert ("sl", 30000.0, 7, "strat-1") in ex.calls
  assert ("close_all", "SL_FAILSAFE", "strat-1") in ex.calls
  assert res["sl_failsafe_close"]["success"] is True
  assert "emergency-closed" in res["comment"]


def test_crypto_tp1_failsafe_flags_when_close_also_fails(config):
  """SL fails AND the emergency close fails → position still naked, flagged loudly."""
  pos = SimpleNamespace(ticket=7, volume=0.02, price_open=30000.0)
  ex = FakeExecutor(positions=[pos], sl_ok=False, failsafe_ok=False)
  market = MarketStrategyFactory.create(
    MarketTypeEnum.CRYPTO, executor=ex, config=config
  )
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, symbol="BTCUSD"))

  assert ("close_all", "SL_FAILSAFE", "strat-1") in ex.calls
  assert res["sl_failsafe_close"]["success"] is False
  assert "UNPROTECTED" in res["comment"]


def test_crypto_tp1_no_failsafe_when_sl_ok(config):
  """Happy path: SL set successfully → no emergency close, no failsafe flag."""
  pos = SimpleNamespace(ticket=7, volume=0.02, price_open=30000.0)
  ex = FakeExecutor(positions=[pos])
  market = MarketStrategyFactory.create(
    MarketTypeEnum.CRYPTO, executor=ex, config=config
  )
  res = market.handle_tp1(make_signal(SignalActionEnum.TP1, symbol="BTCUSD"))
  assert res.get("sl_failsafe_close") is None
  assert not any(c[0] == "close_all" for c in ex.calls)


def test_crypto_full_close_passes_reason(config):
  market = MarketStrategyFactory.create(
    MarketTypeEnum.CRYPTO, executor=FakeExecutor(), config=config
  )
  res = market.handle_full_close(make_signal(SignalActionEnum.SL, symbol="BTCUSD"))
  assert res["reason"] == "SL"
