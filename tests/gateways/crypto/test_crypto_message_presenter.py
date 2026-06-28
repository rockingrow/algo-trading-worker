from helpers import make_signal

from worker.gateways.crypto.message_presenter import CryptoMessagePresenter
from worker.icons import SUCCESS
from worker.schemas.signal_schema import SignalActionEnum


def test_order_filled_shows_sl_set():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0)
  result = {
    "price": 30000.0,
    "volume": 0.02,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "Order Filled" in msg
  assert "SL:" in msg and "29000.0" in msg and SUCCESS in msg


def test_order_filled_warns_when_sl_not_set():
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD")
  result = {
    "price": 31000.0,
    "volume": 0.006,
    "ticket": 9,
    "sl_update": {"success": False, "comment": "SL rejected"},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "SL NOT SET" in msg and "SL rejected" in msg


def test_order_filled_no_sl_line_for_full_close():
  signal = make_signal(SignalActionEnum.TP2, symbol="BTCUSD")
  result = {"price": 32000.0, "volume": 0.02, "ticket": 11}  # no sl_update
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "SL:" not in msg and "SL NOT SET" not in msg


def test_order_filled_shows_scale_position_block():
  # The broker pre-scales the signal, so tp1/tp2/quantity here are already the
  # final values and are displayed verbatim.
  signal = make_signal(
    SignalActionEnum.LONG,
    symbol="BTCUSD",
    is_scale_position=True,
    tp1=31000.0,
    tp2=32000.0,
  )
  result = {"price": 30000.0, "volume": 0.04, "ticket": 7, "sl_update": {"success": True}}
  msg = CryptoMessagePresenter.order_filled(signal, result, 7, "FOOTER")
  assert "Scaled Position" in msg
  assert "TP1:" in msg and "31000.0" in msg
  assert "TP2:" in msg and "32000.0" in msg
  assert "0.04" in msg  # final scaled quantity


def test_order_filled_no_scale_block_for_normal_entry():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD")
  result = {"price": 30000.0, "volume": 0.02, "ticket": 5, "sl_update": {"success": True}}
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "Scaled Position" not in msg
  assert "TP1:" not in msg and "TP2:" not in msg


def test_order_filled_shows_override_section_and_tp1_qty_when_settings_given():
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD")
  result = {"price": 30000.0, "volume": 0.1353, "ticket": 5, "sl_update": {"success": True}}
  settings = {
    "use_custom_risk_percentage": True,
    "risk_percentage": 3.0,
    "use_account_equity": True,
    "use_custom_position_tp1_percent": True,
    "position_tp1_percent": 50.0,
    "tp1_move_sl_to_breakeven": None,
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER", settings_dict=settings)
  # Override section sits between the position block and the footer.
  assert "RISK_PERCENTAGE: <b>ENABLED (3.0%)</b>" in msg
  assert "USE_ACCOUNT_EQUITY: <b>True</b>" in msg
  assert "POSITION_TP1_PERCENT: <b>ENABLED (50.0%)</b>" in msg
  assert "TP1_MOVE_SL_TO_BREAKEVEN: <b>DISABLED</b>" in msg
  # Quantity line shows "TP1 <pct>%" with a gear only for TP1 action.
  assert "Quantity: <b>0.1353 (TP1 50.0% " in msg


def test_order_filled_tp1_qty_uses_signal_percent_without_gear():
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD", tp1_percent=40.0)
  result = {"price": 30000.0, "volume": 0.02, "ticket": 5, "sl_update": {"success": True}}
  settings = {"use_custom_position_tp1_percent": False}
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER", settings_dict=settings)
  assert "Quantity: <b>0.02 (TP1 40.0%)</b>" in msg


def test_order_filled_long_short_no_qty_suffix():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD", tp1_percent=50.0)
  result = {"price": 30000.0, "volume": 0.1353, "ticket": 5, "sl_update": {"success": True}}
  settings = {
    "use_custom_position_tp1_percent": True,
    "position_tp1_percent": 50.0,
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER", settings_dict=settings)
  assert "Quantity: <b>0.1353</b>" in msg


def test_order_filled_no_override_section_without_settings():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD")
  result = {"price": 30000.0, "volume": 0.02, "ticket": 5, "sl_update": {"success": True}}
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "RISK_PERCENTAGE" not in msg
  assert "Quantity: <b>0.02</b>" in msg
