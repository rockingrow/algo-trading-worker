"""Tests for CryptoExecutor against a fake exchange gateway."""

from dataclasses import replace

from helpers import make_signal

from worker.gateways.crypto.base import SIDE_LONG, ExchangePosition, SymbolFilter
from worker.gateways.crypto.executor import CryptoExecutor
from worker.schemas.signal_schema import SignalActionEnum


class FakeGateway:
  def __init__(self, positions=None, mark_price=30000.0, step=0.001, sl_ok=True):
    self._positions = positions if positions is not None else []
    self._mark = mark_price
    self._filter = SymbolFilter(step_size=step, min_qty=step, tick_size=0.1)
    self._sl_ok = sl_ok
    self.orders = []
    self.stops = []
    self.cancelled = []

  def get_symbol_filter(self, symbol):
    return self._filter

  def get_mark_price(self, symbol):
    return self._mark

  def get_positions(self, symbol=None):
    return list(self._positions)

  def place_market_order(self, symbol, side, quantity, reduce_only=False, client_order_id=None):
    self.orders.append((symbol, side, quantity, reduce_only))
    return {"success": True, "retcode": 0, "ticket": 99, "price": self._mark, "volume": quantity}

  def set_stop_loss(self, symbol, position_side, stop_price, quantity):
    self.stops.append((symbol, position_side, stop_price, quantity))
    if not self._sl_ok:
      return {"success": False, "retcode": -2021, "comment": "SL rejected"}
    return {"success": True, "retcode": 0, "ticket": 100}

  def cancel_all_orders(self, symbol):
    self.cancelled.append(symbol)

  def get_account(self):
    return {"balance": 1000.0}


def _long_pos(qty=0.02, entry=30000.0):
  return ExchangePosition(
    symbol="BTCUSDT", side=SIDE_LONG, quantity=qty, entry_price=entry, ticket=7
  )


def test_get_symbol_appends_quote():
  ex = CryptoExecutor(FakeGateway(), None, quote_asset="USDT")
  assert ex.get_symbol("BTCUSD") == "BTCUSDT"
  assert ex.get_symbol("BTCUSDT") == "BTCUSDT"
  assert ex.get_symbol("eth") == "ETHUSDT"


def test_normalize_volume_floors_to_step():
  ex = CryptoExecutor(FakeGateway(step=0.001), None)
  assert ex.normalize_volume("BTCUSD", 0.0239) == 0.023


def test_normalize_volume_no_float_underfloor():
  # 0.3 / 0.1 == 2.9999… in IEEE-754; must floor to 3, not 2.
  ex = CryptoExecutor(FakeGateway(step=0.1), None)
  assert ex.normalize_volume("BTCUSD", 0.3) == 0.3
  # 0.24 / 0.04 is another classic case.
  ex4 = CryptoExecutor(FakeGateway(step=0.04), None)
  assert ex4.normalize_volume("BTCUSD", 0.24) == 0.24


def test_calculate_quantity_risk_based(config):
  ex = CryptoExecutor(FakeGateway(step=0.001), config)
  # capital=1000, risk=2% → $20 risk; |30000-29000|=1000 → 0.02
  assert ex.calculate_quantity("BTCUSD", 30000, 29000, 2.0, 1000) == 0.02


def test_open_position_risk_mode_places_order_and_stop(config):
  gw = FakeGateway()
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0))
  assert res["success"] is True
  assert gw.orders == [("BTCUSDT", "LONG", 0.02, False)]
  assert gw.stops and gw.stops[0][2] == 29000.0


def test_open_position_rolls_back_when_sl_fails(config):
  """A filled entry whose protective stop fails must be closed, not persisted."""
  gw = FakeGateway(sl_ok=False)
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0))

  assert res["success"] is False
  assert "stop-loss failed" in res["comment"]
  # Entry opened (reduce_only=False) then rolled back (reduce_only=True).
  assert gw.orders == [
    ("BTCUSDT", "LONG", 0.02, False),
    ("BTCUSDT", "LONG", 0.02, True),
  ]
  assert res["sl_update"]["success"] is False


def test_open_position_payload_quantity_mode(config):
  cfg = replace(config, volume_decision_enabled=False)
  gw = FakeGateway()
  ex = CryptoExecutor(gw, cfg)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, symbol="BTCUSD", quantity=0.05, sl=None)
  )
  assert res["success"] is True
  assert gw.orders[0][2] == 0.05  # quantity used directly


def test_partial_close_uses_reduce_only(config):
  gw = FakeGateway(positions=[_long_pos(qty=0.02)])
  ex = CryptoExecutor(gw, config)
  res = ex.partial_close_position("BTCUSD", close_volume=0.006, position_ticket=7)
  assert res["success"] is True
  assert gw.orders == [("BTCUSDT", "LONG", 0.006, True)]
  assert res["source_ticket"] == 7


def test_update_sl_cancels_then_sets(config):
  gw = FakeGateway(positions=[_long_pos()])
  ex = CryptoExecutor(gw, config)
  res = ex.update_position_sl("BTCUSD", new_sl=30000.0, position_ticket=7)
  assert res["success"] is True
  assert gw.cancelled == ["BTCUSDT"]
  assert gw.stops[0][2] == 30000.0


def test_close_all_positions_reduce_only(config):
  gw = FakeGateway(positions=[_long_pos(qty=0.02)])
  ex = CryptoExecutor(gw, config)
  res = ex.close_all_positions("BTCUSD", reason="SL")
  assert res["success"] is True
  assert gw.orders == [("BTCUSDT", "LONG", 0.02, True)]
  assert "SL" in res["comment"]


def test_close_all_no_positions(config):
  ex = CryptoExecutor(FakeGateway(positions=[]), config)
  res = ex.close_all_positions("BTCUSD")
  assert res["success"] is False


def test_owned_magics_empty(config):
  assert CryptoExecutor(FakeGateway(), config).owned_magics() == set()
