from helpers import FakeMt5, make_order_result, make_position, make_signal

from worker.gateways.mt5.executor import MT5Executor
from worker.schemas.signal_schema import SignalActionEnum


def _executor(mt5, config):
  return MT5Executor(slippage_deviation=10, config=config, mt5_api=mt5, strategy_magic_map={"strat-1": 42, "strat-short": 42})


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


class FakeStore:
  """Minimal PositionStoreProtocol stand-in keyed by strategy column."""

  def __init__(self, rows_by_strategy=None):
    self._rows = rows_by_strategy or {}

  def get_open_positions_by_strategy(self, strategy, symbol):
    return list(self._rows.get(strategy, []))

  def update_position_status(self, **kwargs):
    pass


def test_get_open_positions_filters_by_strategy_column(config):
  # Strategy isolation is by magic number: strat-1 uses 42, strat-short uses 99.
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, magic=42),
      make_position(ticket=2, magic=99),
    ]
  )
  ex = MT5Executor(
    slippage_deviation=10, config=config, mt5_api=mt5, strategy_magic_map={"strat-1": 42, "strat-short": 99, "strat-long": 43}
  )
  positions = ex.get_open_positions("XAUUSD", strategy="strat-short")
  assert [p.ticket for p in positions] == [2]


def test_get_open_positions_strategy_matches_reticketed_source(config):
  # After a partial close the live ticket may equal the DB source_ticket.
  mt5 = FakeMt5(positions=[make_position(ticket=100, magic=42)])
  store = FakeStore({"strat-1": [{"ticket": 999, "source_ticket": 100}]})
  ex = MT5Executor(
    slippage_deviation=10, config=config, mt5_api=mt5, db=store, strategy_magic_map={"strat-1": 42, "strat-short": 42, "strat-long": 43}
  )
  assert [p.ticket for p in ex.get_open_positions("XAUUSD", strategy="strat-1")] == [100]


def test_close_all_positions_only_closes_matching_strategy_column(config):
  # Two strategies on the same symbol with distinct magic numbers; closing
  # 'strat-short' (magic=99) must leave 'strat-long' (magic=43) untouched.
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, type_=0, volume=0.7, magic=43),  # strat-long
      make_position(ticket=2, type_=1, volume=0.3, magic=99),  # strat-short
    ]
  )
  ex = MT5Executor(
    slippage_deviation=10, config=config, mt5_api=mt5, strategy_magic_map={"strat-1": 42, "strat-short": 99, "strat-long": 43}
  )
  res = ex.close_all_positions("XAUUSD", reason="TP2", strategy="strat-short")
  assert res["success"] is True
  closed_tickets = [r["position"] for r in mt5.sent_requests]
  assert closed_tickets == [2]  # only the short strategy's ticket was closed


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


# ── STRATEGY_MAGIC_MAP: per-strategy magic numbers ──────────────────────── #


def _executor_with_map(mt5, config, strategy_magic_map, db=None):
  return MT5Executor(
    slippage_deviation=10,
    config=config,
    mt5_api=mt5,
    db=db,
    strategy_magic_map=strategy_magic_map,
  )


def test_magic_for_resolves_mapped_strategy(config):
  ex = _executor_with_map(FakeMt5(), config, {"SCALP": 100, "SWING": 200})
  assert ex._magic_for("SCALP") == 100
  assert ex._magic_for("SWING") == 200


import pytest


def test_magic_for_raises_for_unmapped(config):
  ex = _executor_with_map(FakeMt5(), config, {"SCALP": 100})
  with pytest.raises(KeyError):
    ex._magic_for("UNMAPPED")
  with pytest.raises(ValueError):
    ex._magic_for(None)


def test_owned_magics_includes_only_mapped(config):
  ex = _executor_with_map(FakeMt5(), config, {"SCALP": 100, "SWING": 200})
  assert ex.owned_magics() == {100, 200}


def test_open_position_stamps_strategy_magic(config):
  mt5 = FakeMt5()
  ex = _executor_with_map(mt5, config, {"SCALP": 100})
  ex.open_position(make_signal(SignalActionEnum.LONG, strategy="SCALP"))
  assert mt5.sent_requests[-1]["magic"] == 100  # mapped, not the base 42


def test_open_position_unmapped_strategy_raises(config):
  mt5 = FakeMt5()
  ex = _executor_with_map(mt5, config, {"SCALP": 100})
  with pytest.raises(KeyError):
    ex.open_position(make_signal(SignalActionEnum.LONG, strategy="OTHER"))


def test_get_open_positions_mapped_strategy_filters_by_magic_without_db(config):
  # Two strategies share the symbol but have distinct magics. The mapped
  # strategy is isolated purely by magic — no DB is configured.
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, magic=100),  # SCALP
      make_position(ticket=2, magic=200),  # SWING
      make_position(ticket=3, magic=42),   # base / unmapped
    ]
  )
  ex = _executor_with_map(mt5, config, {"SCALP": 100, "SWING": 200})
  assert [p.ticket for p in ex.get_open_positions("XAUUSD", strategy="SCALP")] == [1]
  assert [p.ticket for p in ex.get_open_positions("XAUUSD", strategy="SWING")] == [2]


def test_close_all_positions_mapped_strategy_only_closes_its_magic(config):
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, type_=0, volume=0.5, magic=100),  # SCALP
      make_position(ticket=2, type_=1, volume=0.3, magic=200),  # SWING
    ]
  )
  ex = _executor_with_map(mt5, config, {"SCALP": 100, "SWING": 200})
  res = ex.close_all_positions("XAUUSD", reason="TP2", strategy="SCALP")
  assert res["success"] is True
  assert [r["position"] for r in mt5.sent_requests] == [1]  # only SCALP closed
  assert mt5.sent_requests[-1]["magic"] == 100  # close deal inherits pos magic


def test_get_open_positions_no_strategy_returns_all_owned_magics(config):
  # Without a strategy filter, returns all positions under any owned magic.
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, magic=100),  # SCALP — owned
      make_position(ticket=2, magic=200),  # SWING — owned
      make_position(ticket=3, magic=999),  # foreign EA — excluded
    ]
  )
  ex = _executor_with_map(mt5, config, {"SCALP": 100, "SWING": 200})
  tickets = sorted(p.ticket for p in ex.get_open_positions("XAUUSD"))
  assert tickets == [1, 2]


def test_get_all_open_positions_includes_mapped_magics(config):
  # All positions under any mapped magic are returned; foreign magics excluded.
  mt5 = FakeMt5(
    positions=[
      make_position(ticket=1, magic=100),  # SCALP — mapped
      make_position(ticket=2, magic=200),  # SWING — mapped
      make_position(ticket=3, magic=999),  # foreign — excluded
    ]
  )
  ex = _executor_with_map(mt5, config, {"SCALP": 100, "SWING": 200})
  tickets = sorted(p.ticket for p in ex.get_all_open_positions())
  assert tickets == [1, 2]
