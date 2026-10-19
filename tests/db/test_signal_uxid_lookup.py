"""
End-to-end SQL tests for PositionRepository.get_open_positions_by_strategy's
``signal_uxid`` filter — the third component of the composite position key
(symbol + strategy + signal_uxid) that lets one strategy hold several
positions on a symbol (FOREX_ALLOW_MULTI_POSITIONS_PER_SYMBOL).

Uses a temp DB file (in-memory does not survive the connection close/open
cycle the repository performs per call).
"""

import pytest

from worker.db.repository import PositionRepository
from worker.db.schema import db_init
from worker.settings import settings


@pytest.fixture
def repo(tmp_path, monkeypatch):
  monkeypatch.setattr(settings.database, "file", str(tmp_path / "uxid_test.sqlite"))
  db_init()
  r = PositionRepository()
  # Two signals of ONE strategy holding the same symbol at the same time...
  r.insert_position(
    ref_id="1",
    strategy="strat-1",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
    signal_uxid="uxid-A",
  )
  r.insert_position(
    ref_id="2",
    strategy="strat-1",
    symbol="XAUUSD",
    action="long",
    volume=0.3,
    opened_price=4600.0,
    signal_uxid="uxid-B",
  )
  # ...plus another strategy on the same symbol, which must never leak in.
  r.insert_position(
    ref_id="3",
    strategy="strat-2",
    symbol="XAUUSD",
    action="short",
    volume=0.2,
    opened_price=4610.0,
    signal_uxid="uxid-C",
  )
  return r


def test_uxid_filter_resolves_exactly_one_position(repo):
  rows = repo.get_open_positions_by_strategy("strat-1", "XAUUSD", signal_uxid="uxid-B")
  assert [r["ref_source_id"] for r in rows] == ["2"]


def test_without_a_uxid_every_signal_of_the_strategy_is_returned(repo):
  """The pre-existing (strategy, symbol) behaviour is unchanged when the third
  key component is not supplied."""
  rows = repo.get_open_positions_by_strategy("strat-1", "XAUUSD")
  assert sorted(r["ref_source_id"] for r in rows) == ["1", "2"]


def test_uxid_filter_never_crosses_strategies(repo):
  assert (
    repo.get_open_positions_by_strategy("strat-1", "XAUUSD", signal_uxid="uxid-C") == []
  )


def test_unknown_uxid_returns_nothing(repo):
  assert (
    repo.get_open_positions_by_strategy("strat-1", "XAUUSD", signal_uxid="uxid-ZZZ")
    == []
  )


def test_closed_positions_are_excluded(repo):
  from worker.schemas.position_schema import PositionStatusEnum

  repo.update_position_status(ref_source_id="2", status=PositionStatusEnum.TP2)
  assert (
    repo.get_open_positions_by_strategy("strat-1", "XAUUSD", signal_uxid="uxid-B") == []
  )
