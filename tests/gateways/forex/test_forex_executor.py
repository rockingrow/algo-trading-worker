"""Regression tests for ForexExecutor entry sizing."""

from dataclasses import replace

import pytest
from helpers import FakePlatformGateway, make_platform_position, make_signal

from worker.gateways.config import ExecutionConfig
from worker.gateways.forex.executor import ForexExecutor
from worker.schemas.signal_schema import SignalActionEnum

# A minimal config reused across the manual tests (values irrelevant to matching).
_BASE_CFG = ExecutionConfig(
  volume_decision_enabled=False,
  capital=1000.0,
  risk_percentage=1.0,
  use_account_equity=False,
)


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
  cfg = replace(
    config, volume_decision_enabled=True, use_custom_risk_percentage=True
  )
  ex = _executor(cfg)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, sl=1990.0, risk_percent=None)
  )
  assert res["success"] is True


def test_open_position_signal_risk_does_not_crash(config):
  """The default (non-custom) path still sizes and logs without error."""
  cfg = replace(
    config, volume_decision_enabled=True, use_custom_risk_percentage=False
  )
  ex = _executor(cfg)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, sl=1990.0, risk_percent=1.5)
  )
  assert res["success"] is True


# ── Manual-order (reserved strategy) matching ─────────────────────────────── #


def _manual_executor(positions, manual_strategy="MANUAL"):
  """Executor whose gateway holds *positions*; strat-1 owns magic 12345."""
  cfg = replace(_BASE_CFG, manual_order_strategy=manual_strategy)
  return ForexExecutor(
    gateway=FakePlatformGateway(positions=positions),
    config=cfg,
    strategy_magic_map={"strat-1": 12345},
  )


def test_get_open_positions_manual_strategy_returns_only_unowned():
  owned = make_platform_position(ticket=1, magic=12345, symbol="XAUUSDc")
  manual = make_platform_position(ticket=2, magic=0, symbol="XAUUSDc")
  ex = _manual_executor([owned, manual])

  result = ex.get_open_positions("XAUUSD", strategy="MANUAL")

  assert [p.ticket for p in result] == [2]


def test_get_open_positions_owned_strategy_excludes_manual():
  owned = make_platform_position(ticket=1, magic=12345, symbol="XAUUSDc")
  manual = make_platform_position(ticket=2, magic=0, symbol="XAUUSDc")
  ex = _manual_executor([owned, manual])

  result = ex.get_open_positions("XAUUSD", strategy="strat-1")

  assert [p.ticket for p in result] == [1]


def test_get_all_open_positions_manual_strategy_returns_unowned():
  manual_a = make_platform_position(ticket=2, magic=0, symbol="XAUUSDc")
  manual_b = make_platform_position(ticket=3, magic=999, symbol="EURUSD")
  owned = make_platform_position(ticket=1, magic=12345, symbol="XAUUSDc")
  ex = _manual_executor([owned, manual_a, manual_b])

  result = ex.get_all_open_positions(strategy="MANUAL")

  assert sorted(p.ticket for p in result) == [2, 3]


def test_manual_matching_inert_when_no_manual_strategy_configured():
  """Default config (manual_order_strategy=None) must not special-case anything."""
  manual = make_platform_position(ticket=2, magic=0, symbol="XAUUSDc")
  ex = ForexExecutor(
    gateway=FakePlatformGateway(positions=[manual]),
    config=_BASE_CFG,  # manual_order_strategy is None
    strategy_magic_map={"strat-1": 12345},
  )
  # "MANUAL" is now just an unmapped strategy → magic lookup raises, not adopted.
  with pytest.raises(KeyError):
    ex.get_open_positions("XAUUSD", strategy="MANUAL")


def test_close_all_positions_closes_manual_via_reserved_strategy():
  manual = make_platform_position(ticket=2, magic=0, symbol="XAUUSDc", volume=0.5)
  gateway = FakePlatformGateway(positions=[manual])
  ex = ForexExecutor(
    gateway=gateway,
    config=replace(_BASE_CFG, manual_order_strategy="MANUAL"),
    strategy_magic_map={"strat-1": 12345},
  )

  res = ex.close_all_positions("XAUUSD", reason="SL", strategy="MANUAL")

  assert res["success"] is True
  assert [c["position"].ticket for c in gateway.closed] == [2]
