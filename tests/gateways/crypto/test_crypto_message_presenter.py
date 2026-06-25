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
