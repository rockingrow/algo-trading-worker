"""Tests for SignalSchema scale-in (averaging) via scale_quantity_factor().

The broker pre-scales SL/TP1/TP2/quantity in the payload, so the worker uses
those verbatim. The only re-derivation is the quantity multiplier the executor
re-applies when VOLUME_DECISION sizes the entry from risk.
"""

from worker.schemas.signal_schema import SignalActionEnum, SignalSchema


def _signal(**overrides):
  defaults = {
    "strategy": "strat-1",
    "timestamp": "2026-05-29T10:00:00",
    "action": SignalActionEnum.LONG,
    "symbol": "BTCUSDT",
    "price": 66000.0,
    "quantity": 0.5,
    "sl": 64000.0,
    "tp1": 65000.0,
    "tp2": 67000.0,
  }
  defaults.update(overrides)
  return SignalSchema(**defaults)


def test_not_scale_position_factor_is_one():
  sig = _signal(is_scale_position=False, scaling={"quantity": 0.5})
  assert sig.scale_quantity_factor() == 1.0


def test_missing_scaling_object_factor_is_one():
  sig = _signal(is_scale_position=True)
  assert sig.scale_quantity_factor() == 1.0


def test_absent_quantity_multiplier_factor_is_one():
  # Only tp/sl multipliers reported → nothing to re-apply to self-sized volume.
  sig = _signal(is_scale_position=True, scaling={"tp": 1.1, "sl": 0.9})
  assert sig.scale_quantity_factor() == 1.0


def test_quantity_multiplier_returned_for_scale_position():
  sig = _signal(is_scale_position=True, scaling={"quantity": 0.5})
  assert sig.scale_quantity_factor() == 0.5


def test_quantity_multiplier_ignored_without_scale_flag():
  # scaling present but is_scale_position not set → factor stays 1.0.
  sig = _signal(scaling={"quantity": 2.0})
  assert sig.scale_quantity_factor() == 1.0


def test_broker_scaled_values_are_left_untouched():
  # The schema never mutates the broker's pre-scaled SL/TP/quantity.
  sig = _signal(
    is_scale_position=True,
    sl=63000.0,
    tp1=65500.0,
    tp2=68000.0,
    quantity=0.25,
    scaling={"tp": 1.1, "sl": 0.9, "quantity": 0.5},
  )
  assert sig.sl == 63000.0
  assert sig.tp1 == 65500.0
  assert sig.tp2 == 68000.0
  assert sig.quantity == 0.25
  assert sig.scale_quantity_factor() == 0.5
