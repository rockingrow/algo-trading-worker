"""
Tests for worker/db/schema.py — focused on the _apply_migrations path.

All tests use an in-memory SQLite DB so they are fast, isolated, and leave no
files on disk.
"""

import sqlite3

import pytest

from worker.db.schema import _apply_migrations


def _make_db() -> sqlite3.Connection:
  """Return an in-memory DB with the positions table (no indexes yet)."""
  conn = sqlite3.connect(":memory:")
  conn.row_factory = sqlite3.Row
  conn.execute("""
      CREATE TABLE positions (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          source_ticket INTEGER NOT NULL UNIQUE,
          ticket        INTEGER NOT NULL,
          strategy      TEXT    NOT NULL,
          symbol        TEXT    NOT NULL,
          action        TEXT    NOT NULL DEFAULT 'long',
          volume        REAL    NOT NULL DEFAULT 1.0,
          opened_price  REAL    NOT NULL DEFAULT 0.0,
          status        TEXT    NOT NULL DEFAULT 'OPENED',
          mt5_retcode   INTEGER,
          comment       TEXT,
          message       TEXT,
          sync_status   TEXT    NOT NULL DEFAULT 'PENDING',
          sync_time     DATETIME,
          closed_price  REAL,
          created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
      )
  """)
  return conn


def _insert(conn, *, source_ticket, strategy, symbol, status="OPENED"):
  conn.execute(
    "INSERT INTO positions (source_ticket, ticket, strategy, symbol, status) VALUES (?,?,?,?,?)",
    (source_ticket, source_ticket, strategy, symbol, status),
  )
  conn.commit()


def _rows(conn, status=None):
  q = "SELECT * FROM positions"
  params = ()
  if status:
    q += " WHERE status = ?"
    params = (status,)
  return [dict(r) for r in conn.execute(q, params).fetchall()]


def test_migration_creates_unique_index():
  conn = _make_db()
  _apply_migrations(conn)
  indexes = conn.execute(
    "SELECT name FROM sqlite_master WHERE type='index' AND name='uidx_positions_one_active_per_strategy_symbol'"
  ).fetchone()
  assert indexes is not None, "unique index should be created"


def test_migration_is_idempotent():
  conn = _make_db()
  _apply_migrations(conn)
  _apply_migrations(conn)  # running twice must not raise


def test_migration_on_clean_db_preserves_active_rows():
  # The migration assumes the DB is already clean (at most one active row per
  # strategy+symbol). It does not deduplicate — it only creates the index and
  # must leave existing, non-conflicting rows untouched.
  conn = _make_db()
  _insert(conn, source_ticket=1, strategy="strat-a", symbol="XAUUSD", status="OPENED")
  _insert(conn, source_ticket=2, strategy="strat-a", symbol="EURUSD", status="TP1")

  _apply_migrations(conn)

  statuses = {r["source_ticket"]: r["status"] for r in _rows(conn)}
  assert statuses == {1: "OPENED", 2: "TP1"}  # both preserved, no FORCED_CLOSED


def test_migration_raises_on_preexisting_duplicate_active_rows():
  # Contract: the migration no longer self-heals duplicates. If a DB already
  # violates the one-active-per-(strategy, symbol) invariant, creating the
  # unique index fails — the operator must clean the data first.
  conn = _make_db()
  _insert(conn, source_ticket=1, strategy="strat-a", symbol="XAUUSD", status="OPENED")
  _insert(conn, source_ticket=2, strategy="strat-a", symbol="XAUUSD", status="OPENED")

  with pytest.raises(sqlite3.IntegrityError):
    _apply_migrations(conn)


def test_migration_leaves_closed_rows_untouched():
  conn = _make_db()
  _insert(conn, source_ticket=1, strategy="strat-a", symbol="XAUUSD", status="TP2")
  _insert(conn, source_ticket=2, strategy="strat-a", symbol="XAUUSD", status="SL")
  _insert(conn, source_ticket=3, strategy="strat-a", symbol="XAUUSD", status="OPENED")

  _apply_migrations(conn)

  # Closed rows stay closed; only the single active row is kept
  statuses = {r["source_ticket"]: r["status"] for r in _rows(conn)}
  assert statuses[1] == "TP2"
  assert statuses[2] == "SL"
  assert statuses[3] == "OPENED"


def test_unique_index_prevents_second_active_insert():
  conn = _make_db()
  _apply_migrations(conn)
  _insert(conn, source_ticket=1, strategy="strat-b", symbol="XAUUSD", status="OPENED")

  with pytest.raises(sqlite3.IntegrityError):
    _insert(conn, source_ticket=2, strategy="strat-b", symbol="XAUUSD", status="OPENED")


def test_unique_index_allows_multiple_closed_rows():
  conn = _make_db()
  _apply_migrations(conn)
  _insert(conn, source_ticket=1, strategy="strat-b", symbol="XAUUSD", status="TP2")
  _insert(conn, source_ticket=2, strategy="strat-b", symbol="XAUUSD", status="SL")
  # No exception — closed rows are exempt from the unique constraint
  assert len(_rows(conn)) == 2


def test_unique_index_allows_same_strategy_on_different_symbols():
  conn = _make_db()
  _apply_migrations(conn)
  _insert(conn, source_ticket=1, strategy="strat-c", symbol="XAUUSD", status="OPENED")
  _insert(conn, source_ticket=2, strategy="strat-c", symbol="EURUSD", status="OPENED")
  # Different symbols → allowed
  assert len([r for r in _rows(conn) if r["status"] == "OPENED"]) == 2


def test_unique_index_allows_different_strategies_on_same_symbol():
  conn = _make_db()
  _apply_migrations(conn)
  _insert(conn, source_ticket=1, strategy="strat-long", symbol="XAUUSD", status="OPENED")
  _insert(conn, source_ticket=2, strategy="strat-short", symbol="XAUUSD", status="OPENED")
  # Different strategies → allowed (this is the multi-strategy use-case)
  assert len([r for r in _rows(conn) if r["status"] == "OPENED"]) == 2


def test_migration_adds_magic_column():
  conn = _make_db()
  _apply_migrations(conn)
  columns = {row[1] for row in conn.execute("PRAGMA table_info(positions)").fetchall()}
  assert "magic" in columns


def test_migration_magic_column_is_idempotent():
  conn = _make_db()
  _apply_migrations(conn)
  _apply_migrations(conn)  # second run must not raise


def test_magic_column_stores_and_reads_integer():
  conn = _make_db()
  _apply_migrations(conn)
  conn.execute(
    "INSERT INTO positions (source_ticket, ticket, strategy, symbol, status, magic) VALUES (?,?,?,?,?,?)",
    (10, 10, "strat-a", "XAUUSD", "OPENED", 12345),
  )
  conn.commit()
  row = dict(conn.execute("SELECT magic FROM positions WHERE source_ticket = 10").fetchone())
  assert row["magic"] == 12345


def test_magic_column_nullable_for_old_rows():
  conn = _make_db()
  _apply_migrations(conn)
  _insert(conn, source_ticket=20, strategy="strat-b", symbol="EURUSD", status="OPENED")
  row = dict(conn.execute("SELECT magic FROM positions WHERE source_ticket = 20").fetchone())
  assert row["magic"] is None
