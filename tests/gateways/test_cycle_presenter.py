"""
Tests for worker/gateways/cycle_presenter.py — the body of the one message that
reports a whole trade.

The message is edited in place, so what matters here is that the *whole* story
is re-rendered every time: the headline reflects the latest status, the timeline
holds every action, and the boxes stay separate ``<pre>`` blocks (Telegram
rejects a nested one).
"""

from worker.gateways.cycle_presenter import format_cycle_message
from worker.schemas.cycle_schema import CycleOutcomeEnum, CycleStatusEnum
from worker.settings import MarketTypeEnum

FOREX_SETTINGS = {
  "market_type": MarketTypeEnum.FOREX,
  "volume_decision_enabled": True,
  "use_custom_risk_percentage": True,
  "risk_percentage": 3.0,
  "use_account_equity": False,
  "use_custom_position_tp1_percent": False,
  "position_tp1_percent": 50.0,
  "tp1_move_sl_to_breakeven": None,
  "max_open_orders": 5,
}


def _event(action="LONG", outcome=CycleOutcomeEnum.FILLED, **overrides):
  event = {
    "action": action,
    "outcome": outcome.value,
    "timestamp": "2026-06-02T08:00:00+00:00",
  }
  event.update(overrides)
  return event


def _cycle(events, status=CycleStatusEnum.OPENED, **overrides):
  cycle = {
    "id": 1,
    "signal_uxid": "9f2c4b7e18a3d605",
    "strategy": "strat-a",
    "symbol": "XAUUSD",
    "status": status.value if status else None,
    "events": events,
    "revision": len(events),
  }
  cycle.update(overrides)
  return cycle


def _render(cycle, settings_dict=FOREX_SETTINGS, footer="FOOTER"):
  return format_cycle_message(cycle, settings_dict=settings_dict, footer=footer)


# ── Box structure ─────────────────────────────────────────────────────────── #


def test_renders_three_boxes():
  msg = _render(_cycle([_event(price=2340.0, volume=0.5)]))
  # Two breaks = three boxes; the outer <pre> pair is added by the caller.
  assert msg.count("</pre><pre>") == 2


def test_never_nests_a_pre_inside_another():
  msg = _render(_cycle([_event()]))
  # Every opening tag is preceded by a closing one, so no box is ever nested.
  assert "<pre>" not in msg.replace("</pre><pre>", "")


def test_boxes_are_position_actions_settings_in_that_order():
  position, actions, settings_box = _render(_cycle([_event(price=2340.0)])).split(
    "</pre><pre>"
  )
  assert "LONG XAUUSD" in position
  assert "<b>Actions</b>" in actions
  assert "<b>Settings</b>" in settings_box


def test_a_cycle_with_no_events_still_renders_position_and_settings():
  msg = _render(_cycle([]))
  assert msg.count("</pre><pre>") == 1
  assert "<b>Actions</b>" not in msg


# ── Box 1: the status headline ────────────────────────────────────────────── #


def test_headline_leads_with_the_status_and_its_icon():
  msg = _render(_cycle([_event()], status=CycleStatusEnum.OPENED))
  assert msg.splitlines()[0] == "[⏳ <b>OPENED</b>]"


def test_headline_flips_to_the_closed_icon_when_the_position_ends():
  msg = _render(_cycle([_event()], status=CycleStatusEnum.TP2))
  assert msg.splitlines()[0] == "[🏁 <b>TP2</b>]"


def test_headline_marks_a_rejected_entry():
  msg = _render(_cycle([_event()], status=CycleStatusEnum.REJECTED))
  assert msg.splitlines()[0] == "[🚫 <b>REJECTED</b>]"


def test_headline_reads_pending_before_any_status_is_known():
  msg = _render(_cycle([_event()], status=None))
  assert "PENDING" in msg.splitlines()[0]


def test_position_box_shows_the_entry_levels_and_size():
  msg = _render(
    _cycle(
      [
        _event(
          price=2340.15,
          volume=0.5,
          sl=2330.0,
          tp1=2350.0,
          tp2=2360.0,
          auto_volume=True,
          risk_percent=3.0,
          risk_custom=True,
        )
      ]
    )
  )
  position = msg.split("</pre><pre>")[0]
  assert "📈 <b>LONG XAUUSD</b>" in position
  assert "Price: 2340.15" in position
  assert "Volume: 0.5 lot ⚙️" in position  # gear = the worker sized it
  assert "SL: 2330 | TP1: 2350 | TP2: 2360" in position
  assert "Risk: 3% ⚙️" in position


def test_position_box_describes_the_entry_even_when_it_arrived_late():
  # A worker that restarted mid-trade records the exit first; the entry that
  # follows still supplies the levels the position box is built from.
  msg = _render(
    _cycle([_event(action="TP1", price=2350.0), _event(action="LONG", sl=2330.0)])
  )
  position = msg.split("</pre><pre>")[0]
  assert "LONG XAUUSD" in position
  assert "SL: 2330" in position


def test_position_box_falls_back_to_the_only_event_it_has():
  msg = _render(_cycle([_event(action="FLAT", price=2360.0)]))
  position = msg.split("</pre><pre>")[0]
  assert "FLAT XAUUSD" in position


def test_crypto_volume_carries_no_lot_unit():
  msg = _render(
    _cycle([_event(volume=0.02)]),
    settings_dict={**FOREX_SETTINGS, "market_type": MarketTypeEnum.CRYPTO},
  )
  assert "Volume: 0.02\n" in msg or "Volume: 0.02 |" in msg
  assert "lot" not in msg


# ── Box 2: the timeline ───────────────────────────────────────────────────── #


def test_every_action_appears_in_order():
  msg = _render(
    _cycle(
      [_event(), _event(action="TP1"), _event(action="TP2")],
      status=CycleStatusEnum.TP2,
    )
  )
  actions = msg.split("</pre><pre>")[1]
  assert actions.index("<b>LONG</b>") < actions.index("<b>TP1</b>")
  assert actions.index("<b>TP1</b>") < actions.index("<b>TP2</b>")


def test_outcome_icon_separates_a_filled_action_from_a_failed_one():
  msg = _render(
    _cycle(
      [_event(), _event(action="TP1", outcome=CycleOutcomeEnum.FAILED, reason="closed")]
    )
  )
  assert "✅ <b>LONG</b> — Filled" in msg
  assert "❌ <b>TP1</b> — Failed" in msg


def test_a_failure_explains_itself_with_the_broker_code():
  msg = _render(
    _cycle(
      [
        _event(
          outcome=CycleOutcomeEnum.FAILED,
          reason="Market is closed",
          gateway_return_code=10018,
        )
      ]
    )
  )
  assert "Reason: Market is closed (Code 10018)" in msg


def test_a_policy_rejection_shows_no_broker_code():
  # -1 is the worker's own marker for "never reached the broker".
  msg = _render(
    _cycle(
      [
        _event(
          outcome=CycleOutcomeEnum.REJECTED,
          reason="MAX_OPEN_ORDERS reached",
          gateway_return_code=-1,
        )
      ]
    )
  )
  assert "Reason: MAX_OPEN_ORDERS reached\n" in msg
  assert "Code -1" not in msg


def test_a_fills_broker_acknowledgement_is_not_shown_as_a_reason():
  msg = _render(_cycle([_event(reason="Request executed", gateway_return_code=10009)]))
  assert "Request executed" not in msg


def test_tp1_shows_the_percent_of_the_position_it_closed():
  msg = _render(_cycle([_event(action="TP1", volume=0.25, tp1_percent=50.0)]))
  assert "Volume: 0.25 lot (TP1 50%)" in msg


def test_free_text_from_the_broker_is_escaped():
  msg = _render(
    _cycle([_event(outcome=CycleOutcomeEnum.FAILED, reason="<b>injected</b> & co")])
  )
  assert "&lt;b&gt;injected&lt;/b&gt; &amp; co" in msg
  assert "<b>injected</b>" not in msg


# ── Box 3: the worker configuration ───────────────────────────────────────── #


def test_settings_box_reports_the_worker_configuration():
  msg = _render(_cycle([_event()]))
  settings_box = msg.split("</pre><pre>")[2]
  assert "VOLUME_DECISION_ENABLED: <b>ENABLED</b>" in settings_box
  assert "RISK_PERCENTAGE: <b>ENABLED (3.0%)</b>" in settings_box
  assert "MAX_OPEN_ORDERS: <b>5</b>" in settings_box


def test_settings_box_identifies_the_cycle_and_ends_with_the_footer():
  msg = _render(_cycle([_event()]), footer="Balance: 10240")
  settings_box = msg.split("</pre><pre>")[2]
  assert "Strategy: <b>strat-a</b>" in settings_box
  assert "Signal: <code>9f2c4b7e18a3d605</code>" in settings_box
  assert settings_box.rstrip().endswith("Balance: 10240")


def test_settings_box_survives_a_worker_with_no_settings():
  msg = format_cycle_message(_cycle([_event()]), settings_dict=None, footer="")
  assert "<b>Settings</b>" in msg
  assert "Strategy: <b>strat-a</b>" in msg


# ── Numbers ───────────────────────────────────────────────────────────────── #


def test_whole_numbers_lose_their_trailing_zero():
  msg = _render(_cycle([_event(price=2340.0)]))
  assert "Price: 2340\n" in msg


def test_small_crypto_prices_keep_their_precision():
  msg = _render(
    _cycle([_event(price=0.00001234)]),
    settings_dict={**FOREX_SETTINGS, "market_type": MarketTypeEnum.CRYPTO},
  )
  assert "Price: 0.00001234" in msg


def test_a_missing_value_renders_as_a_dash():
  msg = _render(_cycle([_event()]))
  assert "Price: —" in msg
