from helpers import make_symbol_spec

from worker.gateways.forex.base import SIDE_LONG, SIDE_SHORT, Tick
from worker.gateways.forex.stop_validator import StopValidator


def _tick(ask=2000.0, bid=1999.5):
  return Tick(bid=bid, ask=ask)


def test_no_spec_returns_unchanged():
  v = StopValidator()
  assert v.validate_stops(None, SIDE_LONG, _tick(), 1.0, 2.0) == (1.0, 2.0)


def test_buy_sl_too_close_is_pushed_down():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=10, point=0.01, digits=2)
  # stop_dist = 10 * 0.01 = 0.1; max_sl = bid - 0.1 = 1999.4
  # sl=1999.45 >= 1999.4 -> adjusted to 1999.4 - 0.01 = 1999.39
  sl, tp = v.validate_stops(spec, SIDE_LONG, _tick(2000.0, 1999.5), 1999.45, None)
  assert sl == 1999.39


def test_buy_sl_ok_unchanged():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=10, point=0.01, digits=2)
  sl, tp = v.validate_stops(spec, SIDE_LONG, _tick(2000.0, 1999.5), 1990.0, None)
  assert sl == 1990.0


def test_sell_sl_too_close_is_pushed_up():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=10, point=0.01, digits=2)
  # min_sl = ask + 0.1 = 2000.1; sl=2000.05 <= 2000.1 -> 2000.1 + 0.01 = 2000.11
  sl, tp = v.validate_stops(spec, SIDE_SHORT, _tick(2000.0, 1999.5), 2000.05, None)
  assert sl == 2000.11


def test_sell_tp_too_close_is_pushed_down():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=10, point=0.01, digits=2)
  # max_tp = bid - 0.1 = 1999.4; tp=1999.45 >= 1999.4 -> 1999.4 - 0.01 = 1999.39
  sl, tp = v.validate_stops(spec, SIDE_SHORT, _tick(2000.0, 1999.5), None, 1999.45)
  assert tp == 1999.39
