"""Regression tests for ForexExecutor entry sizing."""

from dataclasses import replace

import pytest
from helpers import (
  FakePlatformGateway,
  make_platform_position,
  make_signal,
  make_symbol_spec,
)

from worker.gateways.forex.base import Tick
from worker.gateways.forex.executor import ForexExecutor
from worker.schemas.signal_schema import SignalActionEnum
from worker.schemas.trade_result import TradeResult


def _executor(cfg):
  return ForexExecutor(
    gateway=FakePlatformGateway(),
    config=cfg,
    strategy_magic_map={"strat-1": 12345},
  )


def test_open_position_custom_risk_does_not_crash(config):
  """USE_CUSTOM_RISK_PERCENTAGE=True must size a VOLUME_DECISION entry without
  raising.

  Regression: the risk-source log line referenced ``use_signal_risk``, a local
  only assigned in the non-custom branch, so a custom-risk entry raised
  ``UnboundLocalError`` after the lot was calculated.
  """
  cfg = replace(config, volume_decision_enabled=True, use_custom_risk_percentage=True)
  ex = _executor(cfg)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, sl=1990.0, risk_percent=None)
  )
  assert res["success"] is True


def test_open_position_signal_risk_does_not_crash(config):
  """The default (non-custom) path still sizes and logs without error."""
  cfg = replace(config, volume_decision_enabled=True, use_custom_risk_percentage=False)
  ex = _executor(cfg)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, sl=1990.0, risk_percent=1.5)
  )
  assert res["success"] is True


# ── Entry lot is sized on the stop that is actually placed ─────────────────── #
#
# StopValidator widens a stop that violates the broker's minimum stop distance,
# so the order can carry a wider SL than the signal asked for. Risk sizing spreads
# risk_cash over the SL distance, so sizing on the signal's narrower stop and then
# submitting the widened one makes the position risk MORE than RISK_PERCENTAGE.


def _sizing_executor(config, **spec_overrides):
  """Executor whose broker enforces a 10-point (0.10) minimum stop distance,
  quoting bid=1999.5 / ask=2000.0."""
  return ForexExecutor(
    gateway=FakePlatformGateway(
      spec=make_symbol_spec(stops_level=10, point=0.01, digits=2, **spec_overrides),
      tick=Tick(bid=1999.5, ask=2000.0),
    ),
    config=config,
    strategy_magic_map={"strat-1": 12345},
  )


def test_entry_lot_is_sized_on_the_widened_stop(config):
  """Regression: the lot used to be computed from ``signal.sl`` before
  StopValidator ran, so a widened stop shipped a lot sized for a tighter one.

  LONG at ask 2000.0 with sl=1999.45: the broker's 0.10 minimum stop distance
  pushes the stop to 1999.39, widening the risk from 55 to 61 points. With
  capital=1000 and risk=2% ($20) and a point value of 1.0, the correct lot is
  20 / 61 = 0.33 — not the 0.36 that 20 / 55 would give.
  """
  ex = _sizing_executor(config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, sl=1999.45))

  assert res["success"] is True
  placed = ex._gateway.placed[0]
  assert placed["sl"] == 1999.39, "order must carry the widened stop"
  assert placed["volume"] == 0.33, "lot must be sized on the widened stop, not 0.36"


def test_entry_lot_unchanged_when_the_stop_needs_no_adjustment(config):
  # sl=1990.0 clears the minimum distance, so nothing is widened and the lot is
  # sized on the signal's own stop: 20 / ((2000.0 - 1990.0) / 0.01) = 0.02.
  ex = _sizing_executor(config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, sl=1990.0))

  assert res["success"] is True
  placed = ex._gateway.placed[0]
  assert placed["sl"] == 1990.0
  assert placed["volume"] == 0.02


def test_short_entry_lot_is_sized_on_the_widened_stop(config):
  # SHORT at bid 1999.5; min_sl = ask + 0.10 = 2000.1, so sl=2000.05 is pushed
  # out to 2000.11. Risk distance from the 1999.5 entry: 61 points → 20/61 = 0.33.
  ex = _sizing_executor(config)
  res = ex.open_position(make_signal(SignalActionEnum.SHORT, sl=2000.05))

  assert res["success"] is True
  placed = ex._gateway.placed[0]
  assert placed["sl"] == 2000.11
  assert placed["volume"] == 0.33


def test_payload_quantity_mode_is_unaffected_by_stop_adjustment(config):
  # With VOLUME_DECISION off the lot comes from signal.quantity, so a widened
  # stop must not change it: 100 units / 100 contract size = 1.0 lot.
  cfg = replace(config, volume_decision_enabled=False)
  ex = _sizing_executor(cfg)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, sl=1999.45, quantity=100))

  assert res["success"] is True
  placed = ex._gateway.placed[0]
  assert placed["sl"] == 1999.39
  assert placed["volume"] == 1.0


def test_entry_without_sl_still_falls_back_to_min_lot(config):
  # No stop to widen and none to size from — the conservative min-lot path holds.
  ex = _sizing_executor(config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, sl=None))

  assert res["success"] is True
  placed = ex._gateway.placed[0]
  assert placed["sl"] is None
  assert placed["volume"] == 0.01


# ── Stops actually placed are reported back ────────────────────────────────── #


def test_result_reports_the_stops_actually_placed(config):
  # The caller (DB + notification) must see what the position really carries,
  # not what the signal asked for.
  ex = _sizing_executor(config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, sl=1999.45, tp2=2000.05))

  assert res["success"] is True
  assert res["sl"] == 1999.39, "widened stop, not the signal's 1999.45"
  assert res["tp"] == 2000.11, "widened target, not the signal's 2000.05"


# ── Live entry quote (feeds the staleness guard) ───────────────────────────── #


def test_get_entry_price_returns_ask_for_long(config):
  # Must match the side open_position actually pays, so the guard judges the
  # signal against the real fill price rather than a mid the broker never quotes.
  ex = ForexExecutor(
    gateway=FakePlatformGateway(tick=Tick(bid=1999.5, ask=2000.0)),
    config=config,
    strategy_magic_map={"strat-1": 12345},
  )
  assert ex.get_entry_price(make_signal(SignalActionEnum.LONG)) == 2000.0


def test_get_entry_price_returns_bid_for_short(config):
  ex = ForexExecutor(
    gateway=FakePlatformGateway(tick=Tick(bid=1999.5, ask=2000.0)),
    config=config,
    strategy_magic_map={"strat-1": 12345},
  )
  assert ex.get_entry_price(make_signal(SignalActionEnum.SHORT)) == 1999.5


def test_get_entry_price_is_none_without_a_tick(config):
  gateway = FakePlatformGateway()
  gateway.get_tick = lambda symbol: None
  ex = ForexExecutor(
    gateway=gateway, config=config, strategy_magic_map={"strat-1": 12345}
  )
  assert ex.get_entry_price(make_signal(SignalActionEnum.LONG)) is None


def test_get_entry_price_is_none_for_a_non_entry_action(config):
  ex = _executor(config)
  assert ex.get_entry_price(make_signal(SignalActionEnum.TP1)) is None


# ── Entry pricing guard ────────────────────────────────────────────────────── #


def test_open_position_refuses_a_zeroed_quote(config):
  """Regression (XPTUSD, retcode 10016): a gateway that hands back a bid=ask=0.0
  tick must not get an order out of the executor.

  Priced off zero, the SL distance became 172,686 points (flooring the lot to the
  minimum) and the stop validator turned SL into -0.01, so the broker rejected
  the entry with 'Invalid stops'."""
  gw = FakePlatformGateway(tick=Tick(bid=0.0, ask=0.0))
  ex = ForexExecutor(gateway=gw, config=config, strategy_magic_map={"strat-1": 12345})

  res = ex.open_position(make_signal(SignalActionEnum.LONG, sl=1726.86528))

  assert res["success"] is False
  assert gw.placed == []


def test_open_position_refuses_a_half_populated_quote(config):
  """A SHORT prices off the bid, so a zero bid is just as unusable."""
  gw = FakePlatformGateway(tick=Tick(bid=0.0, ask=1732.83))
  ex = ForexExecutor(gateway=gw, config=config, strategy_magic_map={"strat-1": 12345})

  res = ex.open_position(make_signal(SignalActionEnum.SHORT, sl=1745.38981))

  assert res["success"] is False
  assert gw.placed == []


def test_open_position_prices_a_healthy_quote(config):
  """The guard doesn't get in the way of a normal entry."""
  gw = FakePlatformGateway(tick=Tick(bid=1999.5, ask=2000.0))
  ex = ForexExecutor(gateway=gw, config=config, strategy_magic_map={"strat-1": 12345})

  res = ex.open_position(make_signal(SignalActionEnum.LONG, sl=1990.0))

  assert res["success"] is True
  assert gw.placed[0]["price"] == 2000.0  # LONG fills at the ask
  assert gw.placed[0]["sl"] == 1990.0


# ── Multi-strategy-per-symbol isolation (FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL) ── #
#
# The toggle relies entirely on native MT5 magic-number isolation inside
# ForexExecutor — two strategies sharing a symbol must never see, size against,
# or accidentally touch each other's ticket. FakeStrategy-level tests
# (test_signal_handler.py) and capability tests (test_market_strategy.py) cover
# the surrounding guard logic, but stub out the executor entirely, so neither
# exercises the actual magic-scoped filtering below.


def _multi_strategy_executor(config, positions=None):
  return ForexExecutor(
    gateway=FakePlatformGateway(positions=positions),
    config=config,
    strategy_magic_map={"strat-A": 111, "strat-B": 222},
  )


def test_two_strategies_open_independent_positions_on_same_symbol(config):
  """Each strategy's entry carries its own magic number, so the broker keeps
  both as separate tickets on the same symbol instead of merging them."""
  ex = _multi_strategy_executor(config)
  res_a = ex.open_position(make_signal(SignalActionEnum.LONG, strategy="strat-A"))
  res_b = ex.open_position(make_signal(SignalActionEnum.LONG, strategy="strat-B"))

  assert res_a["success"] is True
  assert res_b["success"] is True
  placed = ex._gateway.placed
  assert [order["magic"] for order in placed] == [111, 222]
  assert {order["symbol"] for order in placed} == {"XAUUSD"}  # same resolved symbol


def test_get_open_positions_scopes_to_the_requesting_strategy(config):
  """Querying one strategy's positions must never surface the other's ticket,
  even though both sit on the same symbol."""
  pos_a = make_platform_position(ticket=201, magic=111, volume=1.0)
  pos_b = make_platform_position(ticket=202, magic=222, volume=2.0)
  ex = _multi_strategy_executor(config, positions=[pos_a, pos_b])

  assert [p.ticket for p in ex.get_open_positions("XAUUSD", strategy="strat-A")] == [
    201
  ]
  assert [p.ticket for p in ex.get_open_positions("XAUUSD", strategy="strat-B")] == [
    202
  ]


def test_partial_close_only_touches_the_requesting_strategys_ticket(config):
  pos_a = make_platform_position(ticket=201, magic=111, volume=1.0)
  pos_b = make_platform_position(ticket=202, magic=222, volume=2.0)
  ex = _multi_strategy_executor(config, positions=[pos_a, pos_b])

  res = ex.partial_close_position("XAUUSD", close_volume=0.5, strategy="strat-A")

  assert res["success"] is True
  assert [c["position"].ticket for c in ex._gateway.closed] == [201]


def test_update_sl_only_touches_the_requesting_strategys_ticket(config):
  pos_a = make_platform_position(ticket=201, magic=111, volume=1.0)
  pos_b = make_platform_position(ticket=202, magic=222, volume=2.0)
  ex = _multi_strategy_executor(config, positions=[pos_a, pos_b])

  res = ex.update_position_sl("XAUUSD", new_sl=1234.5, strategy="strat-B")

  assert res["success"] is True
  assert [m["position"].ticket for m in ex._gateway.modified] == [202]


def test_close_all_for_one_strategy_leaves_the_others_position_open(config):
  """The core isolation guarantee: flattening strat-A must never close, or
  even touch, strat-B's live position on the same symbol."""
  pos_a = make_platform_position(ticket=201, magic=111, volume=1.0)
  pos_b = make_platform_position(ticket=202, magic=222, volume=2.0)
  ex = _multi_strategy_executor(config, positions=[pos_a, pos_b])

  res = ex.close_all_positions("XAUUSD", reason="FLAT", strategy="strat-A")

  assert res["success"] is True
  closed_tickets = [c["position"].ticket for c in ex._gateway.closed]
  assert closed_tickets == [201]
  assert 202 not in closed_tickets


def test_set_strategy_magic_map_replaces_resolution_and_owned_magics(config):
  """The broker pushes the magic map at runtime via SYSTEM STRATEGY_MAGIC_MAP, so
  ``set_strategy_magic_map`` must repoint both ``_magic_for`` and
  ``owned_magics`` at the new mapping."""
  ex = _executor(config)
  assert ex._magic_for("strat-1") == 12345
  assert ex.owned_magics() == {12345}

  ex.set_strategy_magic_map({"strat-2": 999, "strat-3": 111})
  assert ex._magic_for("strat-2") == 999
  assert ex.owned_magics() == {999, 111}
  # The previous strategy is gone — resolving it now raises (unmapped strategy).
  with pytest.raises(KeyError):
    ex._magic_for("strat-1")


def test_set_strategy_magic_map_none_clears(config):
  """A ``None``/empty push clears the worker's magics rather than crashing."""
  ex = _executor(config)
  ex.set_strategy_magic_map(None)
  assert ex.owned_magics() == set()


def test_set_strategy_magic_map_copies_input(config):
  """The executor stores a copy so a later mutation of the caller's dict — the
  live settings entry — can't silently change the executor's map."""
  ex = _executor(config)
  source = {"strat-9": 42}
  ex.set_strategy_magic_map(source)
  source["strat-9"] = 0
  assert ex._magic_for("strat-9") == 42


# ── Realized PnL of a close ─────────────────────────────────────────────────── #


def test_close_all_sums_the_pnl_of_every_ticket_it_closed(config):
  # A symbol can be held as several tickets, and FLAT/TP2 closes them all in one
  # logical exit. The notification reports one figure, so it must cover them all
  # rather than the last fill's alone (which is what price/volume report).
  gateway = FakePlatformGateway(
    positions=[
      make_platform_position(ticket=201, magic=12345, volume=0.5),
      make_platform_position(ticket=202, magic=12345, volume=0.3),
    ],
    close_results=[
      TradeResult.ok(retcode=10009, ticket="1", price=1999.5, volume=0.5, profit=11.5),
      TradeResult.ok(retcode=10009, ticket="2", price=1999.5, volume=0.3, profit=-4.25),
    ],
  )
  ex = ForexExecutor(
    gateway=gateway, config=config, strategy_magic_map={"strat-1": 12345}
  )

  res = ex.close_all_positions("XAUUSD", reason="TP2", strategy="strat-1")

  assert res["profit"] == pytest.approx(7.25)


def test_close_all_counts_only_the_tickets_that_actually_closed(config):
  # A failed close booked nothing, so it must not drag the reported figure.
  gateway = FakePlatformGateway(
    positions=[
      make_platform_position(ticket=201, magic=12345, volume=0.5),
      make_platform_position(ticket=202, magic=12345, volume=0.3),
    ],
    close_results=[
      TradeResult.ok(retcode=10009, ticket="1", price=1999.5, volume=0.5, profit=11.5),
      TradeResult.fail("Close Failed", profit=None),
    ],
  )
  ex = ForexExecutor(
    gateway=gateway, config=config, strategy_magic_map={"strat-1": 12345}
  )

  assert ex.close_all_positions("XAUUSD", reason="TP2", strategy="strat-1")[
    "profit"
  ] == pytest.approx(11.5)


def test_close_all_pnl_is_unknown_when_no_close_reported_one(config):
  # A gateway that cannot read the amount leaves it unknown rather than claiming
  # the exit broke even.
  gateway = FakePlatformGateway(
    positions=[make_platform_position(ticket=201, magic=12345, volume=0.5)]
  )
  ex = ForexExecutor(
    gateway=gateway, config=config, strategy_magic_map={"strat-1": 12345}
  )

  assert (
    ex.close_all_positions("XAUUSD", reason="SL", strategy="strat-1")["profit"] is None
  )


def test_partial_close_passes_the_gateway_pnl_through(config):
  gateway = FakePlatformGateway(
    positions=[make_platform_position(ticket=201, magic=12345, volume=0.5)],
    close_results=[
      TradeResult.ok(retcode=10009, ticket="1", price=1999.5, volume=0.15, profit=3.4)
    ],
  )
  ex = ForexExecutor(
    gateway=gateway, config=config, strategy_magic_map={"strat-1": 12345}
  )

  res = ex.partial_close_position("XAUUSD", close_volume=0.15, strategy="strat-1")
  assert res["profit"] == pytest.approx(3.4)
