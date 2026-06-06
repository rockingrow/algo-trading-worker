"""
worker/db/schema.py
────────────────────
Schema creation + idempotent migrations for the worker's SQLite database.

The schema is intentionally **market-agnostic**: the same ``positions`` /
``position_logs`` / ``notifications`` tables back both the FOREX (MT5) and the
CRYPTO (CEX) workers. Two columns make this possible:

  * ``magic``        — broker-side isolation handle. Used by MT5 (per-strategy
                       magic number); left ``NULL`` by crypto exchanges that
                       have no equivalent (strategy isolation is logical only).
  * ``market_type``  — tags every row as ``FOREX`` / ``CRYPTO`` so a shared DB
                       can be queried per market.

``db_init`` first creates the tables for a fresh DB, then runs
``_apply_migrations`` so databases created by an older worker version are
upgraded in place without losing data.
"""

from worker.db.connection import _get_conn
from worker.logger import get_logger

logger = get_logger("worker.db.schema")


_POSITIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_ticket INTEGER NOT NULL UNIQUE,
        ticket INTEGER NOT NULL,
        strategy TEXT NOT NULL,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        volume REAL NOT NULL,
        opened_price REAL NOT NULL,
        closed_price REAL,
        status TEXT NOT NULL DEFAULT 'OPENED',
        mt5_retcode INTEGER,
        comment TEXT,
        message TEXT,
        magic INTEGER,
        market_type TEXT,
        sync_status TEXT NOT NULL DEFAULT 'PENDING',
        sync_time DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
"""

_POSITION_LOGS_TABLE = """
    CREATE TABLE IF NOT EXISTS position_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy TEXT NOT NULL,
        ticket INTEGER,
        source_ticket INTEGER,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        volume REAL,
        price REAL,
        sl REAL,
        tp1 REAL,
        mt5_retcode INTEGER,
        comment TEXT,
        message TEXT,
        market_type TEXT,
        author TEXT NOT NULL DEFAULT 'broker',
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
"""

_NOTIFICATIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS notifications (
        id              INTEGER  PRIMARY KEY AUTOINCREMENT,
        platform        TEXT     NOT NULL,
        channel         TEXT     NOT NULL,
        category        TEXT,
        message_text    TEXT     NOT NULL,
        mode            TEXT     NOT NULL DEFAULT 'VERBOSE',
        attempts        INTEGER  NOT NULL DEFAULT 0,
        max_attempts    INTEGER  NOT NULL DEFAULT 5,
        last_error      TEXT,
        next_attempt_at DATETIME,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
    )
"""

_NOTIFICATIONS_INDEX = """
    CREATE INDEX IF NOT EXISTS idx_notifications_pending
        ON notifications (next_attempt_at, id)
"""

# At most one active (OPENED/TP1) position per (strategy, symbol). Shared by both
# markets — a crypto exchange in one-way mode also holds a single net position
# per symbol, so the same invariant applies.
_ONE_ACTIVE_INDEX = """
    CREATE UNIQUE INDEX IF NOT EXISTS uidx_positions_one_active_per_strategy_symbol
        ON positions (strategy, symbol)
        WHERE status = 'OPENED' OR status = 'TP1'
"""


def _table_exists(conn, table: str) -> bool:
  row = conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
  ).fetchone()
  return row is not None


def _columns(conn, table: str) -> set:
  return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_column_if_missing(conn, table: str, column: str, ddl: str) -> None:
  if column not in _columns(conn, table):
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
    logger.info("Migration: added %s.%s", table, column)


def _create_tables(conn) -> None:
  conn.execute(_POSITIONS_TABLE)
  conn.execute(_POSITION_LOGS_TABLE)
  conn.execute(_NOTIFICATIONS_TABLE)
  conn.execute(_NOTIFICATIONS_INDEX)


def _apply_migrations(conn) -> None:
  """Bring an existing DB up to the current schema. Safe to run repeatedly.

  Each step is guarded so the function is idempotent and works whether the DB
  was created by an old worker version or freshly by ``_create_tables``.
  """
  # Columns added after the original release.
  _add_column_if_missing(conn, "positions", "magic", "magic INTEGER")
  _add_column_if_missing(conn, "positions", "market_type", "market_type TEXT")
  if _table_exists(conn, "position_logs"):
    _add_column_if_missing(
      conn, "position_logs", "market_type", "market_type TEXT"
    )

  # Enforce the one-active-position invariant. Raises IntegrityError if the DB
  # already violates it — the operator must clean the data first.
  conn.execute(_ONE_ACTIVE_INDEX)
  conn.commit()


def db_init():
  try:
    conn = _get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    _create_tables(conn)
    _apply_migrations(conn)
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully.")
  except Exception as e:
    logger.exception(f"Database initialization failed: {e}")
