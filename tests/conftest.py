"""
tests/conftest.py
─────────────────
Shared pytest fixtures.  Helper builders/fakes live in helpers.py.
"""

import pytest
from helpers import (  # noqa: F401 (re-exported for legacy imports)
  FakeMt5,
  make_signal,
  make_symbol_info,
)

from worker.gateways.config import ExecutionConfig


@pytest.fixture
def config():
  return ExecutionConfig(
    volume_decision_enabled=True,
    capital=1000.0,
    risk_percentage=2.0,
    use_account_equity=False,
    position_tp1_percent=30.0,
  )


@pytest.fixture
def fake_mt5():
  return FakeMt5()
