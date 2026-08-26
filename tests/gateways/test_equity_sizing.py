"""Entry-sizing mode resolution: settings.USE_ACCOUNT_EQUITY vs the payload's
``position.use_equity_sizing``.

Priority (highest first):
  1. ``USE_ACCOUNT_EQUITY`` in .env — True sizes off live equity (payload
     ``quantity`` ignored), False sizes off the payload ``quantity``.
  2. ``position.use_equity_sizing`` in the signal payload, same meaning.
  3. Neither set → legacy behaviour driven by ``VOLUME_DECISION_ENABLED``.
"""

from dataclasses import replace

import pytest
from helpers import FakePlatformGateway, make_signal

from worker.gateways.forex.executor import ForexExecutor
from worker.schemas.signal_schema import SignalActionEnum


def _executor(cfg, account=None):
  return ForexExecutor(
    gateway=FakePlatformGateway(account=account),
    config=cfg,
    strategy_magic_map={"strat-1": 12345},
  )


# ── ExecutionConfig resolution ─────────────────────────────────────────────── #


@pytest.mark.parametrize(
  "env_value, signal_flag, expected",
  [
    (True, False, True),  # env wins over the payload
    (False, True, False),  # env wins over the payload
    (None, True, True),  # payload decides when env is unset
    (None, False, False),  # payload decides when env is unset
    (None, None, None),  # nobody decided → legacy behaviour
    (True, None, True),
    (False, None, False),
  ],
)
def test_resolve_equity_sizing_priority(config, env_value, signal_flag, expected):
  cfg = replace(config, use_account_equity=env_value)
  assert cfg.resolve_equity_sizing(signal_flag) is expected


@pytest.mark.parametrize(
  "env_value, signal_flag, vde, expected",
  [
    (None, None, True, False),  # legacy: VDE on → worker sizes the entry
    (None, None, False, True),  # legacy: VDE off → payload quantity
    (None, True, False, False),  # payload flag overrides VDE off
    (None, False, True, True),  # payload flag overrides VDE on
    (False, True, True, True),  # env False wins → payload quantity
    (True, False, False, False),  # env True wins → equity sizing
  ],
)
def test_uses_payload_quantity(config, env_value, signal_flag, vde, expected):
  cfg = replace(config, use_account_equity=env_value, volume_decision_enabled=vde)
  assert cfg.uses_payload_quantity(signal_flag) is expected


# ── Forex executor honours the resolved mode ───────────────────────────────── #


def test_signal_equity_sizing_ignores_payload_quantity(config):
  """use_equity_sizing=True: quantity in the payload is ignored and the lot is
  risk-sized off the account's real equity."""
  cfg = replace(config, use_account_equity=None, volume_decision_enabled=False)
  ex = _executor(cfg, account={"balance": 10000.0, "equity": 50000.0})
  ex.open_position(
    make_signal(SignalActionEnum.LONG, quantity=7, sl=1990.0, use_equity_sizing=True)
  )
  lot_from_quantity = ex.convert_quantity_to_lots("XAUUSD", 7)
  assert ex._gateway.placed[0]["volume"] != lot_from_quantity


def test_signal_equity_sizing_false_uses_payload_quantity(config):
  """use_equity_sizing=False sizes from the payload quantity even with
  VOLUME_DECISION_ENABLED on."""
  cfg = replace(config, use_account_equity=None, volume_decision_enabled=True)
  ex = _executor(cfg)
  ex.open_position(
    make_signal(SignalActionEnum.LONG, quantity=100, sl=1990.0, use_equity_sizing=False)
  )
  assert ex._gateway.placed[0]["volume"] == ex.convert_quantity_to_lots("XAUUSD", 100)


def test_env_use_account_equity_false_overrides_signal(config):
  """USE_ACCOUNT_EQUITY=false in .env beats use_equity_sizing=true."""
  cfg = replace(config, use_account_equity=False, volume_decision_enabled=True)
  ex = _executor(cfg)
  ex.open_position(
    make_signal(SignalActionEnum.LONG, quantity=100, sl=1990.0, use_equity_sizing=True)
  )
  assert ex._gateway.placed[0]["volume"] == ex.convert_quantity_to_lots("XAUUSD", 100)


def test_equity_sizing_scales_with_account_equity(config):
  """A bigger equity yields a bigger risk-sized lot — the sizing base really is
  the live account equity, not CAPITAL."""
  cfg = replace(config, use_account_equity=None, volume_decision_enabled=True)
  small = _executor(cfg, account={"balance": 1000.0, "equity": 10000.0})
  large = _executor(cfg, account={"balance": 1000.0, "equity": 100000.0})
  sig = make_signal(SignalActionEnum.LONG, sl=1990.0, use_equity_sizing=True)
  small.open_position(sig)
  large.open_position(sig)
  assert large._gateway.placed[0]["volume"] > small._gateway.placed[0]["volume"]


def test_equity_unavailable_falls_back_to_min_lot(config):
  cfg = replace(config, use_account_equity=True, volume_decision_enabled=True)
  ex = _executor(cfg, account={})
  ex.open_position(make_signal(SignalActionEnum.LONG, sl=1990.0))
  assert ex._gateway.placed[0]["volume"] == 0.01
