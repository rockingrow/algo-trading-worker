"""
tests/gateways/forex/test_hedging_account_guard.py
──────────────────────────────────────────────────
FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL only works on a **hedging** account: a
netting account merges every order on a symbol into one position, so the second
strategy would never own a position and all of its exits would fail. The
processor checks this once at connect and escalates — warn-only, since refusing
to trade entirely is worse than falling back to the old behaviour.
"""

from types import SimpleNamespace

from worker.gateways.forex.signal_processor import (
  MT5_MARGIN_MODE_HEDGING,
  ForexSignalProcessor,
)


class _FakeGateway:
  def __init__(self, account: dict | None) -> None:
    self._account = account

  def get_account(self):
    return self._account

  def get_account_footer(self) -> str:
    return "FOOTER"


def _processor(settings: dict, account: dict | None) -> ForexSignalProcessor:
  """Build the processor without running __init__ (which would spin up a real
  gateway, executor and NATS stack) — only the connect-time check is exercised."""
  proc = ForexSignalProcessor.__new__(ForexSignalProcessor)
  proc.settings = settings
  proc.gateway = _FakeGateway(account)
  proc.ctx = SimpleNamespace(notifier=_FakeNotifier())
  return proc


class _FakeNotifier:
  def __init__(self) -> None:
    self.messages: list[str] = []

  def send_message(self, msg: str) -> None:
    self.messages.append(msg)


def test_warns_when_multi_strategy_on_and_account_is_netting():
  proc = _processor(
    {"forex_allow_multi_strategy_per_symbol": True},
    {"margin_mode": 0},  # ACCOUNT_MARGIN_MODE_RETAIL_NETTING
  )
  proc._warn_if_multi_strategy_needs_hedging()
  assert len(proc.ctx.notifier.messages) == 1
  assert "hedging account" in proc.ctx.notifier.messages[0]


def test_silent_when_account_is_hedging():
  proc = _processor(
    {"forex_allow_multi_strategy_per_symbol": True},
    {"margin_mode": MT5_MARGIN_MODE_HEDGING},
  )
  proc._warn_if_multi_strategy_needs_hedging()
  assert proc.ctx.notifier.messages == []


def test_silent_when_toggle_is_off_even_on_a_netting_account():
  """No behavioural claim is being made, so a netting account is fine."""
  proc = _processor(
    {"forex_allow_multi_strategy_per_symbol": False}, {"margin_mode": 0}
  )
  proc._warn_if_multi_strategy_needs_hedging()
  assert proc.ctx.notifier.messages == []


def test_silent_when_margin_mode_is_unavailable():
  """A gateway that does not report margin_mode must not trigger a false alarm."""
  for account in ({}, None):
    proc = _processor({"forex_allow_multi_strategy_per_symbol": True}, account)
    proc._warn_if_multi_strategy_needs_hedging()
    assert proc.ctx.notifier.messages == []
