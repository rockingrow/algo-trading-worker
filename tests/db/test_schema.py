"""
Tests for worker/db/schema.py — the gateway-neutral schema.

Uses an in-memory SQLite DB so they are fast, isolated, and leave no files on
disk. No migrations: the schema is created from scratch via ``_create_tables``.
"""

import sqlite3

import pytest

from worker.db.schema import _create_tables


def _make_db() -> sqlite3.Connection:
  conn = sqlite3.connect(":memory:")
  conn.row_factory = sqlite3.Row
  _create_tables(conn)
  return conn


def _insert(
  conn, *, ref_source_id, strategy, symbol, status="OPENED", strategy_code=None
):
  conn.execute(
    "INSERT INTO positions (ref_source_id, ref_id, strategy, symbol, action, volume, opened_price, status, strategy_code) "
    "VALUES (?,?,?,?,?,?,?,?,?)",
    (
      ref_source_id,
      ref_source_id,
      strategy,
      symbol,
      "long",
      1.0,
      0.0,
      status,
      strategy_code,
    ),
  )
  conn.commit()


def _cols(conn, table):
  return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


# ── Generic columns ───────────────────────────────────────────────────────── #


def test_positions_has_generic_columns():
  cols = _cols(_make_db(), "positions")
  for c in (
    "ref_id",
    "ref_source_id",
    "strategy_code",
    "gateway_return_code",
    "gateway_message",
    "market_type",
  ):
    assert c in cols
  # Old MT5-specific names must be gone.
  for c in ("ticket", "source_ticket", "magic", "mt5_retcode", "message"):
    assert c not in cols


def test_position_logs_has_generic_columns():
  cols = _cols(_make_db(), "position_logs")
  for c in (
    "ref_id",
    "ref_source_id",
    "gateway_return_code",
    "gateway_message",
    "market_type",
  ):
    assert c in cols
  for c in ("ticket", "source_ticket", "mt5_retcode", "message"):
    assert c not in cols


def test_create_tables_is_idempotent():
  conn = _make_db()
  _create_tables(conn)  # second run must not raise


def test_strategy_code_stores_integer():
  conn = _make_db()
  _insert(
    conn, ref_source_id="10", strategy="strat-a", symbol="XAUUSD", strategy_code=12345
  )
  row = conn.execute(
    "SELECT strategy_code FROM positions WHERE ref_source_id = '10'"
  ).fetchone()
  assert row["strategy_code"] == 12345


def test_ref_source_id_is_text():
  conn = _make_db()
  col = next(
    r
    for r in conn.execute("PRAGMA table_info(positions)").fetchall()
    if r[1] == "ref_source_id"
  )
  assert col[2] == "TEXT"


# ── One-active-per-(strategy, symbol) invariant ───────────────────────────── #


def test_unique_index_prevents_second_active_insert():
  conn = _make_db()
  _insert(conn, ref_source_id="1", strategy="strat-b", symbol="XAUUSD", status="OPENED")
  with pytest.raises(sqlite3.IntegrityError):
    _insert(
      conn, ref_source_id="2", strategy="strat-b", symbol="XAUUSD", status="OPENED"
    )


def test_unique_index_allows_multiple_closed_rows():
  conn = _make_db()
  _insert(conn, ref_source_id="1", strategy="strat-b", symbol="XAUUSD", status="TP2")
  _insert(conn, ref_source_id="2", strategy="strat-b", symbol="XAUUSD", status="SL")
  assert len(conn.execute("SELECT 1 FROM positions").fetchall()) == 2


def test_unique_index_allows_same_strategy_on_different_symbols():
  conn = _make_db()
  _insert(conn, ref_source_id="1", strategy="strat-c", symbol="XAUUSD", status="OPENED")
  _insert(conn, ref_source_id="2", strategy="strat-c", symbol="EURUSD", status="OPENED")
  assert (
    len(conn.execute("SELECT 1 FROM positions WHERE status='OPENED'").fetchall()) == 2
  )


def test_unique_index_allows_different_strategies_on_same_symbol():
  conn = _make_db()
  _insert(
    conn, ref_source_id="1", strategy="strat-long", symbol="XAUUSD", status="OPENED"
  )
  _insert(
    conn, ref_source_id="2", strategy="strat-short", symbol="XAUUSD", status="OPENED"
  )
  assert (
    len(conn.execute("SELECT 1 FROM positions WHERE status='OPENED'").fetchall()) == 2
  )


# ── Signal-cycle tables ───────────────────────────────────────────────────── #


def test_cycle_tables_exist():
  conn = _make_db()
  tables = {
    row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
  }
  assert {"notification_cycles", "notification_cycle_chats"} <= tables


def test_positions_carry_the_cycle_key():
  for table in ("positions", "position_logs"):
    assert "signal_uxid" in _cols(_make_db(), table)


def test_one_cycle_per_strategy_and_uxid():
  conn = _make_db()
  conn.execute(
    "INSERT INTO notification_cycles (signal_uxid, strategy, symbol) VALUES (?,?,?)",
    ("9f2c4b7e18a3d605", "strat-a", "XAUUSD"),
  )
  with pytest.raises(sqlite3.IntegrityError):
    conn.execute(
      "INSERT INTO notification_cycles (signal_uxid, strategy, symbol) VALUES (?,?,?)",
      ("9f2c4b7e18a3d605", "strat-a", "XAUUSD"),
    )


def test_the_same_uxid_under_another_strategy_is_its_own_cycle():
  conn = _make_db()
  for strategy in ("strat-a", "strat-b"):
    conn.execute(
      "INSERT INTO notification_cycles (signal_uxid, strategy, symbol) VALUES (?,?,?)",
      ("9f2c4b7e18a3d605", strategy, "XAUUSD"),
    )
  assert len(conn.execute("SELECT 1 FROM notification_cycles").fetchall()) == 2


def test_one_delivery_row_per_cycle_and_chat():
  conn = _make_db()
  conn.execute(
    "INSERT INTO notification_cycle_chats (cycle_id, chat_id) VALUES (1, '-1001')"
  )
  with pytest.raises(sqlite3.IntegrityError):
    conn.execute(
      "INSERT INTO notification_cycle_chats (cycle_id, chat_id) VALUES (1, '-1001')"
    )


def test_missing_columns_are_retrofitted_onto_an_older_database():
  # A DB created before signal_uxid shipped has the tables but not the column;
  # CREATE TABLE IF NOT EXISTS would silently leave it missing.
  conn = sqlite3.connect(":memory:")
  conn.execute(
    "CREATE TABLE positions (id INTEGER PRIMARY KEY, ref_source_id TEXT, ref_id TEXT, "
    "strategy TEXT, symbol TEXT, action TEXT, volume REAL, opened_price REAL, "
    "status TEXT DEFAULT 'OPENED')"
  )
  conn.execute(
    "CREATE TABLE position_logs (id INTEGER PRIMARY KEY, strategy TEXT, symbol TEXT, "
    "action TEXT)"
  )
  _create_tables(conn)
  for table in ("positions", "position_logs"):
    assert {"signal_id", "signal_uxid"} <= _cols(conn, table)
