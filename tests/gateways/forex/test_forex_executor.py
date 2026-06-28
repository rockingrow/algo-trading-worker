"""Regression tests for ForexExecutor entry sizing."""

from dataclasses import replace

from helpers import FakePlatformGateway, make_signal

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
