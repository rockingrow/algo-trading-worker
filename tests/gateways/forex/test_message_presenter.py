from helpers import make_signal

from worker.gateways.forex.message_presenter import (
  ForexMessagePresenter,
  format_volume,
)
from worker.icons import GEAR, REJECTED, SYNC
from worker.schemas.signal_schema import SignalActionEnum


def test_format_volume_plain():
  assert format_volume(0.5) == "0.5 lot"


def test_order_rejected_contains_reason_and_fields():
  signal = make_signal(SignalActionEnum.LONG)
  msg = ForexMessagePresenter.order_rejected(
    signal, "Max open orders reached (5/5); entry not placed (MAX_OPEN_ORDERS).", "FOOTER"
  )
  assert "Order Rejected" in msg and REJECTED in msg
  assert "XAUUSD" in msg and "strat-1" in msg and "LONG" in msg
  assert "Max open orders reached (5/5)" in msg
  assert "FOOTER" in msg


def test_startup_banner_shows_max_open_orders():
  msg = ForexMessagePresenter.startup({"capital": 1000, "max_open_orders": 7}, "FOOTER")
  assert "MAX_OPEN_ORDERS: <b>7</b>" in msg


def test_startup_banner_omits_max_open_orders_when_disabled():
  msg = ForexMessagePresenter.startup({"capital": 1000, "max_open_orders": 0}, "FOOTER")
  assert "MAX_OPEN_ORDERS" not in msg


def test_format_volume_auto_calculated_has_icon():
  out = format_volume(0.5, auto_calculated=True)
  assert "0.5 lot" in out and GEAR in out


def test_order_filled_contains_key_fields():
  signal = make_signal(SignalActionEnum.LONG)
  msg = ForexMessagePresenter.order_filled(
    signal, {"price": 2000.0, "volume": 0.1, "ticket": 5}, 5, "FOOTER"
  )
  assert "Order Filled" in msg
  assert "XAUUSD" in msg
  assert "strat-1" in msg
  assert "FOOTER" in msg
  assert msg.startswith("<pre>") and msg.endswith("</pre>")


def test_order_filled_shows_scale_position_block():
  # The broker pre-scales the signal, so tp1/tp2/sl here are already the final
  # values and are displayed verbatim; volume is the executed lot.
  signal = make_signal(
    SignalActionEnum.LONG,
    is_scale_position=True,
    tp1=2040.0,
    tp2=2100.0,
    sl=1980.0,
  )
  msg = ForexMessagePresenter.order_filled(
    signal, {"price": 2000.0, "volume": 0.2, "ticket": 5}, 5, "FOOTER"
  )
  assert "Scaled Position" in msg
  assert "TP1:" in msg and "2040.0" in msg
  assert "TP2:" in msg and "2100.0" in msg
  assert "SL:" in msg and "1980.0" in msg
  assert "0.2 lot" in msg  # final scaled volume


def test_order_filled_no_scale_block_for_normal_entry():
  signal = make_signal(SignalActionEnum.LONG)
  msg = ForexMessagePresenter.order_filled(
    signal, {"price": 2000.0, "volume": 0.1, "ticket": 5}, 5, "FOOTER"
  )
  assert "Scaled Position" not in msg


def test_order_failed_contains_error():
  signal = make_signal(SignalActionEnum.SL)
  msg = ForexMessagePresenter.order_failed(
    signal, {"comment": "rejected", "retcode": 10016, "price": None}, "FOOTER"
  )
  assert "Order Failed" in msg
  assert "strat-1" in msg
  assert "rejected" in msg
  assert "10016" in msg


def test_force_closed_message():
  msg = ForexMessagePresenter.force_closed(
    "XAUUSD",
    "strat-1",
    {"price": 1999.0, "volume": 0.2, "ref_id": 7, "ref_source_id": 3},
    "FOOTER",
  )
  assert "Force Closed" in msg
  assert "strat-1" in msg
  assert "7" in msg and "3" in msg


def test_startup_message_includes_config():
  settings_dict = {
    "volume_decision_enabled": True,
    "capital": 1000,
    "capital_currency": "USD",
    "risk_percentage": 2.0,
    "use_account_equity": False,
    "position_tp1_percent": 30,
  }
  msg = ForexMessagePresenter.startup(settings_dict, "FOOTER")
  assert "FOREX Worker" in msg
  assert "RISK_PERCENTAGE" in msg
  assert "FOOTER" in msg
  # Override lines are always shown; unset ones read DISABLED.
  assert "RISK_PERCENTAGE: <b>DISABLED</b>" in msg
  assert "POSITION_TP1_PERCENT: <b>DISABLED</b>" in msg
  assert "TP1_MOVE_SL_TO_BREAKEVEN: <b>DISABLED</b>" in msg


def test_startup_message_override_lines_enabled():
  base = {
    "volume_decision_enabled": True,
    "capital": 1000,
    "capital_currency": "USD",
    "risk_percentage": 2.0,
    "use_account_equity": False,
  }
  msg = ForexMessagePresenter.startup(
    {
      **base,
      "use_custom_risk_percentage": True,
      "use_custom_position_tp1_percent": True,
      "position_tp1_percent": 50,
      "tp1_move_sl_to_breakeven": False,
    },
    "F",
  )
  assert "RISK_PERCENTAGE: <b>ENABLED (2.0%)</b>" in msg
  assert "POSITION_TP1_PERCENT: <b>ENABLED (50%)</b>" in msg
  # A declared boolean override is ENABLED even when its value is false.
  assert "TP1_MOVE_SL_TO_BREAKEVEN: <b>ENABLED (false)</b>" in msg


def test_startup_message_enabled_override_with_unset_percent_reads_na():
  # use_custom_position_tp1_percent on but POSITION_TP1_PERCENT never configured:
  # render "ENABLED (n/a)" rather than a literal "None%".
  base = {
    "volume_decision_enabled": True,
    "capital": 1000,
    "capital_currency": "USD",
    "use_account_equity": False,
  }
  msg = ForexMessagePresenter.startup(
    {
      **base,
      "use_custom_risk_percentage": True,
      "risk_percentage": None,
      "use_custom_position_tp1_percent": True,
      "position_tp1_percent": None,
    },
    "F",
  )
  assert "RISK_PERCENTAGE: <b>ENABLED (n/a)</b>" in msg
  assert "POSITION_TP1_PERCENT: <b>ENABLED (n/a)</b>" in msg
  assert "None%" not in msg


def test_startup_message_tp1_be_line():
  base = {
    "volume_decision_enabled": True,
    "capital": 1000,
    "capital_currency": "USD",
    "risk_percentage": 2.0,
    "use_account_equity": False,
  }
  msg_on = ForexMessagePresenter.startup({**base, "tp1_move_sl_to_breakeven": True}, "F")
  assert "TP1_MOVE_SL_TO_BREAKEVEN: <b>ENABLED (true)</b>" in msg_on

  msg_off = ForexMessagePresenter.startup({**base, "tp1_move_sl_to_breakeven": False}, "F")
  assert "TP1_MOVE_SL_TO_BREAKEVEN: <b>ENABLED (false)</b>" in msg_off


def test_shutdown_message():
  assert "Disconnected" in ForexMessagePresenter.shutdown("FOOTER")


def test_admin_flat_closed_contains_key_fields():
  db_pos = {"symbol": "XAUUSD", "strategy": "strat-A", "ref_source_id": 10}
  result = {"price": 2001.5, "volume": 0.5, "ticket": 999}
  msg = ForexMessagePresenter.admin_flat_closed(db_pos, result, "FOOTER")
  assert "Admin FLAT" in msg
  assert "XAUUSD" in msg
  assert "strat-A" in msg
  assert "2001.5" in msg
  assert "10" in msg
  assert "FOOTER" in msg


def test_position_reconciled_closed_contains_key_fields():
  db_pos = {
    "symbol": "XAUUSD", "strategy": "strat-A", "ref_source_id": "4587420656",
    "volume": 0.01,
  }
  msg = ForexMessagePresenter.position_reconciled_closed(db_pos, 2000.0, "FOOTER")
  assert "Position Reconciled" in msg and SYNC in msg
  assert "XAUUSD" in msg and "strat-A" in msg
  assert "0.01 lot" in msg
  assert "2000.0" in msg
  assert "4587420656" in msg
  assert "FOOTER" in msg
