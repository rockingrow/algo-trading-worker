from helpers import FakeMt5, make_symbol_info, make_tick

from worker.gateways.forex.mt5.stop_validator import StopValidator


def _validator(**si):
  mt5 = FakeMt5(symbol_info=make_symbol_info(**si))
  return StopValidator(mt5), mt5


def test_no_symbol_info_returns_unchanged():
  mt5 = FakeMt5()
  mt5._symbol_info = None
  v = StopValidator(mt5)
  assert v.validate_stops("X", mt5.ORDER_TYPE_BUY, make_tick(), 1.0, 2.0) == (1.0, 2.0)


def test_buy_sl_too_close_is_pushed_down():
  v, mt5 = _validator(trade_stops_level=10, point=0.01, digits=2)
  tick = make_tick(ask=2000.0, bid=1999.5)
  # stop_dist = 10 * 0.01 = 0.1; max_sl = bid - 0.1 = 1999.4
  # sl=1999.45 >= 1999.4 -> adjusted to 1999.4 - 0.01 = 1999.39
  sl, tp = v.validate_stops("X", mt5.ORDER_TYPE_BUY, tick, 1999.45, None)
  assert sl == 1999.39


def test_buy_sl_ok_unchanged():
  v, mt5 = _validator(trade_stops_level=10, point=0.01, digits=2)
  tick = make_tick(ask=2000.0, bid=1999.5)
  sl, tp = v.validate_stops("X", mt5.ORDER_TYPE_BUY, tick, 1990.0, None)
  assert sl == 1990.0


def test_sell_sl_too_close_is_pushed_up():
  v, mt5 = _validator(trade_stops_level=10, point=0.01, digits=2)
  tick = make_tick(ask=2000.0, bid=1999.5)
  # min_sl = ask + 0.1 = 2000.1; sl=2000.05 <= 2000.1 -> 2000.1 + 0.01 = 2000.11
  sl, tp = v.validate_stops("X", mt5.ORDER_TYPE_SELL, tick, 2000.05, None)
  assert sl == 2000.11


def test_sell_tp_too_close_is_pushed_down():
  v, mt5 = _validator(trade_stops_level=10, point=0.01, digits=2)
  tick = make_tick(ask=2000.0, bid=1999.5)
  # max_tp = bid - 0.1 = 1999.4; tp=1999.45 >= 1999.4 -> 1999.4 - 0.01 = 1999.39
  sl, tp = v.validate_stops("X", mt5.ORDER_TYPE_SELL, tick, None, 1999.45)
  assert tp == 1999.39
