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


# ── Unusable quotes ────────────────────────────────────────────────────────── #
#
# Every adjustment is anchored on the tick, so a zeroed quote (what the terminal
# reports for a symbol it has no price for) has to be refused outright: anchoring
# on 0.0 makes *any* stop look "too close" and pushes it below zero.


def test_zero_tick_leaves_long_stops_untouched():
  """Regression (XPTUSD, retcode 10016): with bid=0.0 and stops_level=0 the LONG
  branch computed max_sl = 0.0, judged a 1726.86 stop "too close", and sent
  sl=-0.01 while leaving tp=1745.39 on the far side of the market."""
  v = StopValidator()
  spec = make_symbol_spec(stops_level=0, point=0.01, digits=2)
  sl, tp = v.validate_stops(
    spec, SIDE_LONG, _tick(ask=0.0, bid=0.0), 1726.86528, 1745.38981
  )
  assert (sl, tp) == (1726.86528, 1745.38981)


def test_zero_tick_leaves_short_stops_untouched():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=0, point=0.01, digits=2)
  sl, tp = v.validate_stops(
    spec, SIDE_SHORT, _tick(ask=0.0, bid=0.0), 1745.38981, 1726.86528
  )
  assert (sl, tp) == (1745.38981, 1726.86528)


def test_half_populated_tick_leaves_stops_untouched():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=10, point=0.01, digits=2)
  sl, tp = v.validate_stops(spec, SIDE_LONG, _tick(ask=0.0, bid=1999.5), 1990.0, 2050.0)
  assert (sl, tp) == (1990.0, 2050.0)


def test_missing_tick_leaves_stops_untouched():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=10, point=0.01, digits=2)
  assert v.validate_stops(spec, SIDE_LONG, None, 1990.0, 2050.0) == (1990.0, 2050.0)


def test_adjustment_never_yields_a_non_positive_price():
  """A stops_level wider than the price itself would push the stop below zero;
  keep the signal's own level and let the broker reject it on its own terms."""
  v = StopValidator()
  # stop_dist = 300000 * 0.01 = 3000.0 -> max_sl = 1999.5 - 3000.0 = -1000.5
  spec = make_symbol_spec(stops_level=300000, point=0.01, digits=2)
  sl, tp = v.validate_stops(spec, SIDE_LONG, _tick(2000.0, 1999.5), 1990.0, None)
  assert sl == 1990.0


def test_short_tp_adjustment_never_yields_a_non_positive_price():
  v = StopValidator()
  spec = make_symbol_spec(stops_level=300000, point=0.01, digits=2)
  sl, tp = v.validate_stops(spec, SIDE_SHORT, _tick(2000.0, 1999.5), None, 1990.0)
  assert tp == 1990.0
