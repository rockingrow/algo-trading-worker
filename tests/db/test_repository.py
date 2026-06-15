"""
Repository boundary tests: ref_id / ref_source_id stored and returned as str.
"""

import sqlite3

import pytest

from worker.db.repository import PositionRepository
from worker.db.schema import db_init
from worker.schemas.position_schema import PositionStatusEnum
from worker.settings import settings


@pytest.fixture
def repo(tmp_path, monkeypatch):
  monkeypatch.setattr(settings, "db_file", str(tmp_path / "repo_test.sqlite"))
  db_init()
  return PositionRepository()


def _raw(db_file, sql):
  conn = sqlite3.connect(db_file)
  conn.row_factory = sqlite3.Row
  rows = [dict(r) for r in conn.execute(sql).fetchall()]
  conn.close()
  return rows


def test_insert_then_get_returns_str_ids(repo):
  repo.insert_position(
    ref_id="12345", strategy="strat-a", symbol="XAUUSD", action="long",
    volume=0.5, opened_price=4500.0, gateway_return_code=10009,
    message='{"x":1}', strategy_code=777,
  )
  pos = repo.get_position("12345")
  assert pos["ref_id"] == "12345" and isinstance(pos["ref_id"], str)
  assert pos["ref_source_id"] == "12345" and isinstance(pos["ref_source_id"], str)
  assert pos["strategy_code"] == 777
  assert pos["gateway_return_code"] == 10009
  assert pos["gateway_message"] == '{"x":1}'
  # Old MT5-specific aliases are not part of the gateway-neutral row.
  for k in ("ticket", "source_ticket", "magic", "mt5_retcode", "message"):
    assert k not in pos


def test_ref_ids_physically_stored_as_text(repo):
  repo.insert_position(
    ref_id="999", strategy="strat-a", symbol="BTCUSD", action="long",
    volume=1.0, opened_price=30000.0,
  )
  rows = _raw(settings.db_file, "SELECT ref_id, ref_source_id, typeof(ref_id) AS t FROM positions")
  assert rows[0]["ref_id"] == "999"
  assert rows[0]["ref_source_id"] == "999"
  assert rows[0]["t"] == "text"


def test_update_status_matches_by_ref_source_id(repo):
  repo.insert_position(
    ref_id="100", strategy="strat-a", symbol="XAUUSD", action="long",
    volume=0.5, opened_price=4500.0,
  )
  repo.update_position_status(
    ref_source_id="100", status=PositionStatusEnum.TP1,
    ref_id="200", closed_price=4600.0,
  )
  pos = repo.get_position("100")
  assert pos["status"] == "TP1"
  assert pos["ref_id"] == "200"
  assert pos["ref_source_id"] == "100"


def test_prices_persisted_without_lossy_rounding(repo):
  """Regression: prices must keep full precision. A 2-decimal round (forex-era)
  collapsed low-priced crypto (e.g. SHIB ~0.00002345 → 0.00), corrupting the
  stored opened/closed price and every downstream PositionEvent."""
  entry = 0.00002345
  close = 0.00002567
  repo.insert_position(
    ref_id="42", strategy="strat-a", symbol="SHIBUSD", action="long",
    volume=1_000_000.0, opened_price=entry,
  )
  repo.update_position_status(
    ref_source_id="42", status=PositionStatusEnum.SL, closed_price=close,
  )
  pos = repo.get_position("42")
  assert pos["opened_price"] == entry  # not rounded to 0.0
  assert pos["closed_price"] == close

  repo.log_position(
    strategy="strat-a", ref_id="42", ref_source_id="42", symbol="SHIBUSD",
    action="SL", volume=1_000_000.0, price=close, sl=None, tp1=None,
    gateway_return_code=0, comment="", market_type="CRYPTO",
  )
  rows = _raw(settings.db_file, "SELECT price FROM position_logs")
  assert rows[0]["price"] == close


def test_log_position_persists_generic_columns(repo):
  repo.log_position(
    strategy="strat-a", ref_id="5", ref_source_id="5", symbol="XAUUSD",
    action="SL", volume=0.5, price=4400.0, sl=None, tp1=None,
    gateway_return_code=0, comment="x", message='{"a":1}',
    author="terminal", market_type="FOREX",
  )
  rows = _raw(settings.db_file, "SELECT ref_id, ref_source_id, gateway_return_code, gateway_message, market_type FROM position_logs")
  assert rows[0]["ref_id"] == "5"
  assert rows[0]["ref_source_id"] == "5"
  assert rows[0]["gateway_message"] == '{"a":1}'
  assert rows[0]["market_type"] == "FOREX"
