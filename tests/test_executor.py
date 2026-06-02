from conftest import FakeMt5, make_order_result, make_position, make_signal

from worker.mt5.executor import MT5Executor
from worker.schemas.signal_schema import SignalActionEnum


def _executor(mt5, config):
  return MT5Executor(magic_number=42, slippage_deviation=10, config=config, mt5_api=mt5)


def test_open_position_long_builds_buy_request(config):
  mt5 = FakeMt5()
  ex = _executor(mt5, config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG))
  assert res["success"] is True
  req = mt5.sent_requests[-1]
  assert req["type"] == mt5.ORDER_TYPE_BUY
  assert req["magic"] == 42
  assert req["price"] == 2000.0  # ask for buy
  assert "sl" in req and "tp" in req


def test_open_position_short_uses_bid(config):
  mt5 = FakeMt5()
  ex = _executor(mt5, config)
  ex.open_position(make_signal(SignalActionEnum.SHORT))
  req = mt5.sent_requests[-1]
  assert req["type"] == mt5.ORDER_TYPE_SELL
  assert req["price"] == 1999.5  # bid for sell


def test_open_position_rejected_retcode(config):
  mt5 = FakeMt5(order_results=[make_order_result(retcode=10016, comment="bad stops")])
  ex = _executor(mt5, config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG))
  assert res["success"] is False
  assert res["retcode"] == 10016


def test_open_position_none_result(config):
  mt5 = FakeMt5(order_results=[None])
  ex = _executor(mt5, config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG))
  assert res["success"] is False
  assert res["comment"] == "Send Failed"


def test_get_open_positions_filters_by_magic(config):
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, magic=42),
      make_position(ticket=2, magic=999),  # different magic
    ]
  )
  ex = _executor(mt5, config)
  positions = ex.get_open_positions("XAUUSD")
  assert [p.ticket for p in positions] == [1]


def test_partial_close_no_positions(config):
  mt5 = FakeMt5(positions=[])
  ex = _executor(mt5, config)
  res = ex.partial_close_position("XAUUSD", 0.5)
  assert res["success"] is False
  assert res["comment"] == "No Positions Found"


def test_partial_close_clamps_volume(config):
  mt5 = FakeMt5(positions=[make_position(ticket=5, type_=0, volume=1.0, magic=42)])
  ex = _executor(mt5, config)
  ex.partial_close_position("XAUUSD", 5.0, position_ticket=5)
  req = mt5.sent_requests[-1]
  assert req["volume"] == 1.0  # clamped to open volume
  assert req["type"] == mt5.ORDER_TYPE_SELL  # counter of BUY
  assert req["position"] == 5


def test_close_all_positions_uses_actual_volume(config):
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, type_=0, volume=0.7, magic=42),
      make_position(ticket=2, type_=1, volume=0.3, magic=42),
    ]
  )
  ex = _executor(mt5, config)
  res = ex.close_all_positions("XAUUSD", reason="TP2")
  assert res["success"] is True
  volumes = [r["volume"] for r in mt5.sent_requests]
  assert volumes == [0.7, 0.3]


def test_update_position_sl(config):
  mt5 = FakeMt5(positions=[make_position(ticket=8, tp=2050.0, magic=42)])
  ex = _executor(mt5, config)
  res = ex.update_position_sl("XAUUSD", new_sl=1995.0, position_ticket=8)
  assert res["success"] is True
  req = mt5.sent_requests[-1]
  assert req["action"] == mt5.TRADE_ACTION_SLTP
  assert req["sl"] == 1995.0
  assert req["tp"] == 2050.0  # preserved


def test_get_all_open_positions_filters_by_magic(config):
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, magic=42),
      make_position(ticket=2, magic=999),
    ]
  )
  ex = _executor(mt5, config)
  positions = ex.get_all_open_positions()
  assert [p.ticket for p in positions] == [1]


def test_close_single_position_buy_sends_sell(config):
  pos = make_position(ticket=7, type_=0, volume=0.6, magic=42)  # BUY
  mt5 = FakeMt5()
  ex = _executor(mt5, config)
  res = ex.close_single_position(pos, reason="FLAT")
  assert res["success"] is True
  req = mt5.sent_requests[-1]
  assert req["type"] == mt5.ORDER_TYPE_SELL
  assert req["volume"] == 0.6
  assert req["position"] == 7
  assert "FLAT" in req["comment"]


def test_close_single_position_sell_uses_ask(config):
  pos = make_position(ticket=8, type_=1, volume=0.3, magic=42)  # SELL
  mt5 = FakeMt5()
  ex = _executor(mt5, config)
  ex.close_single_position(pos, reason="FLAT")
  req = mt5.sent_requests[-1]
  assert req["type"] == mt5.ORDER_TYPE_BUY
  assert req["price"] == 2000.0  # ask for counter-sell


def test_close_single_position_failure_returns_error(config):
  pos = make_position(ticket=9, magic=42)
  mt5 = FakeMt5(order_results=[make_order_result(retcode=10016, comment="requote")])
  ex = _executor(mt5, config)
  res = ex.close_single_position(pos, reason="FLAT")
  assert res["success"] is False
  assert "requote" in res["comment"]
