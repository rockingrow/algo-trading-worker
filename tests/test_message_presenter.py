from conftest import make_signal

from worker.gateways.mt5.message_presenter import TradeMessagePresenter, format_volume
from worker.schemas.signal_schema import SignalActionEnum


def test_format_volume_plain():
  assert format_volume(0.5) == "0.5 lot"


def test_format_volume_auto_calculated_has_icon():
  out = format_volume(0.5, auto_calculated=True)
  assert "0.5 lot" in out and "⚙️" in out


def test_order_filled_contains_key_fields():
  signal = make_signal(SignalActionEnum.LONG)
  msg = TradeMessagePresenter.order_filled(
    signal, {"price": 2000.0, "volume": 0.1, "ticket": 5}, 5, "FOOTER"
  )
  assert "Order Filled" in msg
  assert "XAUUSD" in msg
  assert "strat-1" in msg
  assert "FOOTER" in msg
  assert msg.startswith("<pre>") and msg.endswith("</pre>")


def test_order_failed_contains_error():
  signal = make_signal(SignalActionEnum.SL)
  msg = TradeMessagePresenter.order_failed(
    signal, {"comment": "rejected", "retcode": 10016, "price": None}, "FOOTER"
  )
  assert "Order Failed" in msg
  assert "strat-1" in msg
  assert "rejected" in msg
  assert "10016" in msg


def test_force_closed_message():
  msg = TradeMessagePresenter.force_closed(
    "XAUUSD",
    "strat-1",
    {"price": 1999.0, "volume": 0.2, "ticket": 7, "source_ticket": 3},
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
  msg = TradeMessagePresenter.startup(settings_dict, "FOOTER")
  assert "MT5 Worker" in msg
  assert "RISK_PERCENTAGE" in msg
  assert "FOOTER" in msg


def test_shutdown_message():
  assert "Disconnected" in TradeMessagePresenter.shutdown("FOOTER")


def test_admin_flat_closed_contains_key_fields():
  db_pos = {"symbol": "XAUUSD", "strategy": "strat-A", "source_ticket": 10}
  result = {"price": 2001.5, "volume": 0.5, "ticket": 999}
  msg = TradeMessagePresenter.admin_flat_closed(db_pos, result, "FOOTER")
  assert "Admin FLAT" in msg
  assert "XAUUSD" in msg
  assert "strat-A" in msg
  assert "2001.5" in msg
  assert "10" in msg
  assert "FOOTER" in msg
