"""
worker/db/schema.py
────────────────────
Schema creation for the worker's SQLite database.

The ``positions`` / ``position_logs`` / ``notifications`` tables are shared by
every gateway (FOREX/MT5 and CRYPTO/CEX), so the columns are deliberately
**gateway-neutral**:

  * ``strategy_code``        INTEGER — broker-side isolation handle (MT5 magic
                             number; ``NULL`` for exchanges that have none).
  * ``gateway_return_code``  INTEGER — broker/exchange numeric status code.
  * ``gateway_message``      TEXT    — raw signal/event JSON kept for audit.
  * ``ref_id``               TEXT    — broker reference for the live order/deal
                             (MT5 ticket, exchange order id). Stored as text so
                             any gateway's id format fits; the repository parses
                             text↔int at the boundary.
  * ``ref_source_id``        TEXT    — the originating position reference (stable
                             across re-ticketing after a partial close).
  * ``market_type``          TEXT    — FOREX / CRYPTO tag.

No runtime migrations: PROD has no data yet, so the database is simply created
from scratch (drop the sqlite file to recreate).
"""

from worker.db.connection import _get_conn
from worker.logger import get_logger

logger = get_logger("worker.db.schema")


_POSITIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ref_source_id TEXT NOT NULL,
        ref_id TEXT NOT NULL,
        signal_id TEXT,
        strategy TEXT NOT NULL,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        volume REAL NOT NULL,
        opened_price REAL NOT NULL,
        closed_price REAL,
        status TEXT NOT NULL DEFAULT 'OPENED',
        gateway_return_code INTEGER,
        comment TEXT,
        gateway_message TEXT,
        strategy_code INTEGER,
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
        ref_id TEXT,
        ref_source_id TEXT,
        signal_id TEXT,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        volume REAL,
        price REAL,
        sl REAL,
        tp1 REAL,
        gateway_return_code INTEGER,
        comment TEXT,
        gateway_message TEXT,
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

# Fast dedup lookup for RETRY_SIGNALS: the base processor's replay handler
# checks each incoming signal_id against position_logs (every processed signal
# is logged there, whether it succeeded or was rejected) and skips ones already
# seen. The audit log is the single source of truth here — positions may or may
# not exist for a given signal_id (rejections have no OPENED row), while
# position_logs always does.
_POSITION_LOGS_SIGNAL_ID_INDEX = """
    CREATE INDEX IF NOT EXISTS idx_position_logs_signal_id
        ON position_logs (signal_id)
        WHERE signal_id IS NOT NULL
"""


def _create_tables(conn) -> None:
  conn.execute(_POSITIONS_TABLE)
  conn.execute(_POSITION_LOGS_TABLE)
  conn.execute(_NOTIFICATIONS_TABLE)
  conn.execute(_NOTIFICATIONS_INDEX)
  conn.execute(_ONE_ACTIVE_INDEX)
  conn.execute(_POSITION_LOGS_SIGNAL_ID_INDEX)


def db_init():
  try:
    conn = _get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    _create_tables(conn)
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully.")
  except Exception as e:
    logger.exception(f"Database initialization failed: {e}")
