"""Regression tests for ForexExecutor entry sizing."""

from dataclasses import replace

from helpers import FakePlatformGateway, make_platform_position, make_signal

from worker.gateways.forex.executor import ForexExecutor
from worker.schemas.signal_schema import SignalActionEnum


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

  assert [p.ticket for p in ex.get_open_positions("XAUUSD", strategy="strat-A")] == [201]
  assert [p.ticket for p in ex.get_open_positions("XAUUSD", strategy="strat-B")] == [202]


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
