"""
Repository boundary tests: gateway-neutral TEXT columns ↔ app-domain int ids.

Confirms the repository stores ref_id/ref_source_id as TEXT but hands callers
back ``ticket`` / ``source_ticket`` as ``int`` (and maps strategy_code→magic,
gateway_return_code→mt5_retcode, gateway_message→message).
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


def test_insert_then_get_returns_app_keys_as_int(repo):
  repo.insert_position(
    ticket=12345, strategy="strat-a", symbol="XAUUSD", action="long",
    volume=0.5, opened_price=4500.0, mt5_retcode=10009, message='{"x":1}', magic=777,
  )
  pos = repo.get_position(12345)
  assert pos["ticket"] == 12345 and isinstance(pos["ticket"], int)
  assert pos["source_ticket"] == 12345 and isinstance(pos["source_ticket"], int)
  assert pos["magic"] == 777            # strategy_code → magic
  assert pos["mt5_retcode"] == 10009    # gateway_return_code → mt5_retcode
  assert pos["message"] == '{"x":1}'    # gateway_message → message
  # Old generic column names must not leak into the app dict.
  for k in ("ref_id", "ref_source_id", "strategy_code", "gateway_return_code", "gateway_message"):
    assert k not in pos


def test_ref_ids_physically_stored_as_text(repo):
  repo.insert_position(
    ticket=999, strategy="strat-a", symbol="BTCUSD", action="long",
    volume=1.0, opened_price=30000.0,
  )
  rows = _raw(settings.db_file, "SELECT ref_id, ref_source_id, typeof(ref_id) AS t FROM positions")
  assert rows[0]["ref_id"] == "999"          # stored as text
  assert rows[0]["ref_source_id"] == "999"
  assert rows[0]["t"] == "text"


def test_update_status_matches_by_source_ticket_int(repo):
  repo.insert_position(
    ticket=100, strategy="strat-a", symbol="XAUUSD", action="long",
    volume=0.5, opened_price=4500.0,
  )
  repo.update_position_status(
    source_ticket=100, status=PositionStatusEnum.TP1, new_ticket=200, closed_price=4600.0,
  )
  pos = repo.get_position(100)
  assert pos["status"] == "TP1"
  assert pos["ticket"] == 200            # ref_id updated via new_ticket
  assert pos["source_ticket"] == 100     # unchanged


def test_log_position_persists_generic_columns(repo):
  repo.log_position(
    strategy="strat-a", ticket=5, source_ticket=5, symbol="XAUUSD", action="SL",
    volume=0.5, price=4400.0, sl=None, tp1=None, mt5_retcode=0,
    comment="x", message='{"a":1}', author="terminal", market_type="FOREX",
  )
  rows = _raw(settings.db_file, "SELECT ref_id, ref_source_id, gateway_return_code, gateway_message, market_type FROM position_logs")
  assert rows[0]["ref_id"] == "5"
  assert rows[0]["gateway_message"] == '{"a":1}'
  assert rows[0]["market_type"] == "FOREX"
