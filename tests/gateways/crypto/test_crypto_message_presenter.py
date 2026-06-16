from helpers import make_signal

from worker.gateways.crypto.message_presenter import CryptoMessagePresenter
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
  assert "SL:" in msg and "29000.0" in msg and "✅" in msg


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
