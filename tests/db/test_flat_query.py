"""
End-to-end SQL tests for PositionRepository.get_open_positions_for_flat —
the query that backs the ADMIN FLAT subject.

Exercises the full filter matrix against a real (temp-file) SQLite DB:

    no strategy, no symbol  → close ALL active positions
    strategy only           → all symbols for that strategy
    symbol only             → all strategies for that symbol
    strategy + symbol        → only that exact strategy+symbol

Uses a temp DB file (in-memory does not survive the connection close/open
cycle the repository performs per call).
"""

import pytest

from worker.db.repository import PositionRepository
from worker.db.schema import db_init
from worker.settings import settings


@pytest.fixture
def repo(tmp_path, monkeypatch):
  monkeypatch.setattr(settings, "db_file", str(tmp_path / "flat_test.sqlite"))
  db_init()
  r = PositionRepository()
  # Three distinct active positions + one closed one that must never match.
  r.insert_position(ref_id="1", strategy="strat-long", symbol="XAUUSD", action="long", volume=0.5, opened_price=4500.0)
  r.insert_position(ref_id="2", strategy="strat-short", symbol="XAUUSD", action="short", volume=0.3, opened_price=4600.0)
  r.insert_position(ref_id="3", strategy="strat-long", symbol="EURUSD", action="long", volume=0.2, opened_price=1.1)
  r.insert_position(ref_id="4", strategy="strat-long", symbol="GBPUSD", action="long", volume=0.4, opened_price=1.3)
  # Close the GBPUSD one so it is excluded from every FLAT query.
  from worker.schemas.position_schema import PositionStatusEnum

  r.update_position_status(ref_source_id="4", status=PositionStatusEnum.TP2)
  return r


def _strat_symbol(rows):
  return {(row["strategy"], row["symbol"]) for row in rows}


def test_flat_no_filters_returns_all_active(repo):
  rows = repo.get_open_positions_for_flat()
  assert _strat_symbol(rows) == {
    ("strat-long", "XAUUSD"),
    ("strat-short", "XAUUSD"),
    ("strat-long", "EURUSD"),
  }


def test_flat_strategy_only_returns_all_symbols_for_strategy(repo):
  rows = repo.get_open_positions_for_flat(strategy="strat-long")
  assert _strat_symbol(rows) == {
    ("strat-long", "XAUUSD"),
    ("strat-long", "EURUSD"),
  }


def test_flat_symbol_only_returns_all_strategies_for_symbol(repo):
  rows = repo.get_open_positions_for_flat(symbol="XAUUSD")
  assert _strat_symbol(rows) == {
    ("strat-long", "XAUUSD"),
    ("strat-short", "XAUUSD"),
  }


def test_flat_strategy_and_symbol_returns_exact_pair(repo):
  rows = repo.get_open_positions_for_flat(strategy="strat-long", symbol="XAUUSD")
  assert _strat_symbol(rows) == {("strat-long", "XAUUSD")}


def test_flat_excludes_closed_positions(repo):
  # GBPUSD (TP2) must never appear in any query.
  for rows in (
    repo.get_open_positions_for_flat(),
    repo.get_open_positions_for_flat(strategy="strat-long"),
    repo.get_open_positions_for_flat(symbol="GBPUSD"),
  ):
    assert all(row["symbol"] != "GBPUSD" for row in rows)
