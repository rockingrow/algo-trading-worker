"""Tests for SignalSchema scale-in (averaging) adjustment via apply_scaling()."""

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


def test_not_scale_position_returns_self_unchanged():
  sig = _signal(is_scale_position=False, scaling={"tp": 1.1})
  assert sig.apply_scaling() is sig


def test_missing_scaling_object_returns_self_unchanged():
  sig = _signal(is_scale_position=True)
  assert sig.apply_scaling() is sig


def test_tp_multiplier_scales_both_tp1_and_tp2():
  sig = _signal(is_scale_position=True, scaling={"tp": 1.1})
  scaled = sig.apply_scaling()
  assert scaled.tp1 == 65000.0 * 1.1
  assert scaled.tp2 == 67000.0 * 1.1
  # untouched fields preserved
  assert scaled.sl == 64000.0
  assert scaled.quantity == 0.5
  # original signal is not mutated (copy semantics)
  assert sig.tp1 == 65000.0


def test_sl_multiplier_scales_sl_only():
  sig = _signal(is_scale_position=True, scaling={"sl": 0.95})
  scaled = sig.apply_scaling()
  assert scaled.sl == 64000.0 * 0.95
  assert scaled.tp1 == 65000.0
  assert scaled.tp2 == 67000.0


def test_quantity_multiplier_scales_payload_quantity():
  sig = _signal(is_scale_position=True, quantity=0.5, scaling={"quantity": 2.0})
  scaled = sig.apply_scaling()
  assert scaled.quantity == 1.0


def test_quantity_multiplier_scales_risk_percent_for_self_determined_mode():
  # No payload quantity → self-determined (risk) sizing; the multiplier must
  # scale risk_percent so the resulting position size scales the same way.
  sig = _signal(
    is_scale_position=True, quantity=None, risk_percent=2.0, scaling={"quantity": 1.5}
  )
  scaled = sig.apply_scaling()
  assert scaled.risk_percent == 3.0
  assert scaled.quantity is None


def test_quantity_multiplier_scales_both_when_both_present():
  sig = _signal(
    is_scale_position=True, quantity=0.5, risk_percent=2.0, scaling={"quantity": 2.0}
  )
  scaled = sig.apply_scaling()
  assert scaled.quantity == 1.0
  assert scaled.risk_percent == 4.0


def test_all_multipliers_together():
  sig = _signal(
    is_scale_position=True,
    scaling={"tp": 1.1, "sl": 0.9, "quantity": 0.5},
  )
  scaled = sig.apply_scaling()
  assert scaled.tp1 == 65000.0 * 1.1
  assert scaled.tp2 == 67000.0 * 1.1
  assert scaled.sl == 64000.0 * 0.9
  assert scaled.quantity == 0.25


def test_absent_multiplier_leaves_target_untouched():
  # Only sl is provided; tp/quantity absent → those values stay as-is.
  sig = _signal(is_scale_position=True, scaling={"sl": 0.8})
  scaled = sig.apply_scaling()
  assert scaled.sl == 64000.0 * 0.8
  assert scaled.tp1 == 65000.0
  assert scaled.quantity == 0.5


def test_no_target_values_is_noop_returns_self():
  # scaling.tp set but the signal carries no TP fields → nothing to scale.
  sig = _signal(is_scale_position=True, tp1=None, tp2=None, scaling={"tp": 1.2})
  assert sig.apply_scaling() is sig
