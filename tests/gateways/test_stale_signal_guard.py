"""Tests for guard.stale_signal_rejection — the entry staleness guard.

Entries are market orders, so they fill at the live quote rather than at the
price the signal was built on. These cover the three ways a signal can be too
stale to execute: the market has already passed its take-profit, already passed
its stop-loss, or drifted too far from the price it was planned at.
"""

from helpers import make_signal

from worker.gateways import guard
from worker.schemas.signal_schema import SignalActionEnum

# make_signal defaults (LONG): price=2000, sl=1990, tp1=2020, tp2=2050. The
# entry-to-SL distance ("R") is therefore 10.0, so the default 50%
# MAX_ENTRY_DRIFT_R_PERCENT allows a drift of up to 5.0.


def _short(**overrides):
  """A SHORT signal that mirrors the LONG defaults: R = 10, TP 50 below entry."""
  defaults = {"price": 2000.0, "sl": 2010.0, "tp1": 1980.0, "tp2": 1950.0}
  defaults.update(overrides)
  return make_signal(SignalActionEnum.SHORT, **defaults)


# ── Market already through the take-profit ────────────────────────────────── #


def test_long_rejected_when_ask_already_past_tp2():
  reason = guard.stale_signal_rejection(make_signal(), 2055.0, {})
  assert reason is not None
  assert "already reached its take-profit" in reason
  assert "ask is 2055.0" in reason
  assert "tp2 is 2050.0" in reason


def test_long_rejected_when_ask_exactly_at_tp2():
  # At the target there is nothing left to capture, so the boundary rejects too.
  assert guard.stale_signal_rejection(make_signal(), 2050.0, {}) is not None


def test_short_rejected_when_bid_already_past_tp2():
  reason = guard.stale_signal_rejection(_short(), 1945.0, {})
  assert reason is not None
  assert "already reached its take-profit" in reason
  assert "bid is 1945.0" in reason


def test_xpdusd_regression_entry_above_its_own_tp_is_rejected():
  """The reported failure: XPDUSD LONG signalled at 1375 with TP 1377, but the
  ask had run to 1388 — the fill opened above its own target and was instantly
  in loss. The guard must stop this before any order is sent."""
  signal = make_signal(symbol="XPDUSD", price=1375.0, sl=1370.0, tp1=1376.0, tp2=1377.0)
  reason = guard.stale_signal_rejection(signal, 1388.0, {})
  assert reason is not None
  assert "XPDUSD LONG rejected" in reason
  assert "already reached its take-profit" in reason


# ── Market already through the stop-loss ──────────────────────────────────── #


def test_long_rejected_when_ask_already_past_sl():
  reason = guard.stale_signal_rejection(make_signal(), 1985.0, {})
  assert reason is not None
  assert "already reached its stop-loss" in reason
  assert "sl is 1990.0" in reason


def test_short_rejected_when_bid_already_past_sl():
  reason = guard.stale_signal_rejection(_short(), 2015.0, {})
  assert reason is not None
  assert "already reached its stop-loss" in reason


def test_tp_check_takes_precedence_over_drift():
  # A signal can breach both rules at once; the TP message is the more specific
  # diagnosis, so it is the one reported.
  reason = guard.stale_signal_rejection(make_signal(), 2060.0, {})
  assert "already reached its take-profit" in reason
  assert "drift" not in reason


# ── Drift as a percentage of the entry-to-SL distance ─────────────────────── #


def test_long_rejected_when_drift_exceeds_r_percent():
  # drift = 6.0 = 60% of R (10.0), over the 50% default. Still short of tp2
  # (2050) and clear of sl (1990), so only the drift rule can catch it.
  reason = guard.stale_signal_rejection(make_signal(), 2006.0, {})
  assert reason is not None
  assert "price moved too far" in reason
  assert "60.0% of the 10 entry-to-SL risk" in reason
  assert "MAX_ENTRY_DRIFT_R_PERCENT" in reason


def test_long_allowed_when_drift_within_r_percent():
  # drift = 4.0 = 40% of R — a normal amount of movement, entry proceeds.
  assert guard.stale_signal_rejection(make_signal(), 2004.0, {}) is None


def test_drift_at_exactly_the_limit_is_allowed():
  # drift = 5.0 = exactly 50% of R; the rule rejects only *beyond* the limit.
  assert guard.stale_signal_rejection(make_signal(), 2005.0, {}) is None


def test_drift_is_direction_agnostic():
  # A move *in the signal's favour* is still a different trade than the one
  # planned, so the drift rule measures absolute distance.
  reason = guard.stale_signal_rejection(make_signal(), 1994.0, {})
  assert reason is not None
  assert "price moved too far" in reason


def test_short_drift_uses_the_same_r_normalization():
  # SHORT R is also 10.0 (2010 - 2000), so 2006 is the same 60% drift.
  reason = guard.stale_signal_rejection(_short(), 2006.0, {})
  assert reason is not None
  assert "60.0% of the 10 entry-to-SL risk" in reason


def test_r_percent_threshold_is_read_from_settings():
  settings = {"max_entry_drift_r_percent": 80.0}
  # drift = 6.0 = 60% of R, now inside a more permissive limit.
  assert guard.stale_signal_rejection(make_signal(), 2006.0, settings) is None
  # 90% of R is still over it.
  assert guard.stale_signal_rejection(make_signal(), 2009.0, settings) is not None


def test_r_percent_zero_disables_the_drift_check_only():
  settings = {"max_entry_drift_r_percent": 0}
  # A large drift now passes...
  assert guard.stale_signal_rejection(make_signal(), 2030.0, settings) is None
  # ...but a market already through TP/SL is still rejected: those rules are
  # correctness, not a tunable tolerance.
  assert guard.stale_signal_rejection(make_signal(), 2050.0, settings) is not None
  assert guard.stale_signal_rejection(make_signal(), 1990.0, settings) is not None


def test_r_normalization_holds_across_price_scales():
  """The point of measuring drift in R: one threshold fits every symbol."""
  # BTCUSDT-scale: R = 1000, drift 120 = 12% → allowed.
  btc = make_signal(symbol="BTCUSDT", price=95000.0, sl=94000.0, tp2=99000.0)
  assert guard.stale_signal_rejection(btc, 95120.0, {}) is None
  # EURUSD-scale: R = 0.0030, drift 0.0002 ≈ 6.7% → allowed by the same number.
  eur = make_signal(symbol="EURUSD", price=1.0835, sl=1.0805, tp2=1.0900)
  assert guard.stale_signal_rejection(eur, 1.0837, {}) is None
  # And both reject once the drift is large relative to their own risk.
  assert guard.stale_signal_rejection(btc, 95600.0, {}) is not None
  assert guard.stale_signal_rejection(eur, 1.0855, {}) is not None


# ── Drift fallback for signals with no SL ─────────────────────────────────── #


def test_no_sl_falls_back_to_percent_of_price():
  signal = make_signal(sl=None)
  # drift = 20.0 on a 2000 price = 1.0%, over the 0.5% default.
  reason = guard.stale_signal_rejection(signal, 2020.0, {})
  assert reason is not None
  assert "1.00%" in reason
  assert "MAX_ENTRY_DRIFT_PERCENT" in reason
  assert "signal carries no SL" in reason


def test_no_sl_allows_drift_within_percent_of_price():
  signal = make_signal(sl=None)
  # drift = 5.0 = 0.25%, inside the 0.5% default.
  assert guard.stale_signal_rejection(signal, 2005.0, {}) is None


def test_zero_width_risk_falls_back_to_percent_of_price():
  # An SL equal to the entry price gives R = 0; dividing by it would raise, so
  # the percent-of-price rule takes over.
  signal = make_signal(sl=2000.0)
  reason = guard.stale_signal_rejection(signal, 2020.0, {})
  assert reason is not None
  assert "MAX_ENTRY_DRIFT_PERCENT" in reason


def test_percent_threshold_is_read_from_settings():
  signal = make_signal(sl=None)
  assert (
    guard.stale_signal_rejection(signal, 2020.0, {"max_entry_drift_percent": 2}) is None
  )


def test_percent_zero_disables_the_fallback():
  signal = make_signal(sl=None)
  settings = {"max_entry_drift_percent": 0}
  # drift = 40.0 = 2% of price, well over the default, but still short of tp2.
  assert guard.stale_signal_rejection(signal, 2040.0, settings) is None
  # The TP rule is unaffected.
  assert guard.stale_signal_rejection(signal, 2050.0, settings) is not None


# ── Missing data never blocks an entry ────────────────────────────────────── #


def test_no_live_quote_skips_every_check():
  # A quote the broker could not supply is a data-availability problem; blocking
  # entries on it would be a worse failure than letting the broker decide.
  assert guard.stale_signal_rejection(make_signal(), None, {}) is None


def test_non_positive_live_quote_skips_every_check():
  assert guard.stale_signal_rejection(make_signal(), 0.0, {}) is None
  assert guard.stale_signal_rejection(make_signal(), -1.0, {}) is None


def test_signal_without_price_still_gets_the_tp_and_sl_checks():
  signal = make_signal(price=None)
  # No signal price means no drift to measure...
  assert guard.stale_signal_rejection(signal, 2004.0, {}) is None
  # ...but tp2/sl are absolute levels and are still enforced.
  assert guard.stale_signal_rejection(signal, 2055.0, {}) is not None
  assert guard.stale_signal_rejection(signal, 1985.0, {}) is not None


def test_signal_without_tp_or_sl_only_gets_the_drift_check():
  signal = make_signal(sl=None, tp2=None)
  assert guard.stale_signal_rejection(signal, 2005.0, {}) is None
  assert guard.stale_signal_rejection(signal, 2020.0, {}) is not None


def test_unparseable_threshold_falls_back_to_the_default():
  settings = {"max_entry_drift_r_percent": "not-a-number"}
  # Falls back to 50%, so a 60% drift is still rejected.
  assert guard.stale_signal_rejection(make_signal(), 2006.0, settings) is not None
