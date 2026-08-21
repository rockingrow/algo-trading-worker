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

Table creation uses CREATE TABLE IF NOT EXISTS, so a brand-new column added to
an existing table (e.g. ``signal_id``) won't retrofit onto a DB created before
that change. Those columns get a small ``ALTER TABLE ... ADD COLUMN`` guarded
by a ``PRAGMA table_info`` check in ``_create_tables`` below.

``notification_cycles`` / ``notification_cycle_chats`` back the one-message-per
-trade Telegram notification: a cycle keeps its own timeline and rendered body,
and each chat it addresses keeps the id of the message being edited in place.
They are notification state, not trade state — dropping them loses chat history
formatting, never a position.
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

# ── Signal-cycle notifications ─────────────────────────────────────────────
#
# One row per trade cycle (the broker's ``signal_uxid``), holding the whole
# timeline plus the body last rendered from it. ``revision`` is bumped on every
# recorded action; each chat row remembers the highest revision it has been
# shown, which is what lets a slow edit never overwrite a newer body with an
# older one.
_NOTIFICATION_CYCLES_TABLE = """
    CREATE TABLE IF NOT EXISTS notification_cycles (
        id            INTEGER  PRIMARY KEY AUTOINCREMENT,
        signal_uxid   TEXT     NOT NULL,
        strategy      TEXT     NOT NULL,
        symbol        TEXT     NOT NULL,
        status        TEXT,
        events        TEXT     NOT NULL DEFAULT '[]',
        message_text  TEXT,
        revision      INTEGER  NOT NULL DEFAULT 0,
        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
    )
"""

# The cycle key. ``strategy`` is part of it because two strategies may trade the
# same instrument, and a uxid is only unique within the strategy that minted it.
_NOTIFICATION_CYCLES_KEY_INDEX = """
    CREATE UNIQUE INDEX IF NOT EXISTS uidx_notification_cycles_key
        ON notification_cycles (strategy, signal_uxid)
"""

# One row per (cycle, chat): a cycle addressed to several channel chats owns a
# separate Telegram message in each, so the message id — and the retry
# bookkeeping around editing it — is per chat, not per cycle.
_NOTIFICATION_CYCLE_CHATS_TABLE = """
    CREATE TABLE IF NOT EXISTS notification_cycle_chats (
        id                 INTEGER  PRIMARY KEY AUTOINCREMENT,
        cycle_id           INTEGER  NOT NULL,
        chat_id            TEXT     NOT NULL,
        message_id         TEXT,
        delivered_revision INTEGER  NOT NULL DEFAULT 0,
        attempts           INTEGER  NOT NULL DEFAULT 0,
        max_attempts       INTEGER  NOT NULL DEFAULT 5,
        last_error         TEXT,
        next_attempt_at    DATETIME,
        created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP
    )
"""

_NOTIFICATION_CYCLE_CHATS_KEY_INDEX = """
    CREATE UNIQUE INDEX IF NOT EXISTS uidx_notification_cycle_chats_key
        ON notification_cycle_chats (cycle_id, chat_id)
"""

# Drives the dispatcher's poll: undelivered chats, oldest first.
_NOTIFICATION_CYCLE_CHATS_PENDING_INDEX = """
    CREATE INDEX IF NOT EXISTS idx_notification_cycle_chats_pending
        ON notification_cycle_chats (delivered_revision, next_attempt_at, id)
"""

# At most one active (OPENED/TP1) position per (strategy, symbol). Shared by both
# markets — a crypto exchange in one-way mode also holds a single net position
# per symbol, so the same invariant applies.
_ONE_ACTIVE_INDEX = """
    CREATE UNIQUE INDEX IF NOT EXISTS uidx_positions_one_active_per_strategy_symbol
        ON positions (strategy, symbol)
        WHERE status = 'OPENED' OR status = 'TP1'
"""

# Fast dedup lookup for the ACK replay: the base processor's replay handler
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

# positions / position_logs gained ``signal_id`` and later ``signal_uxid`` after
# these tables first shipped, so a database created before either change has the
# tables but not the columns. CREATE TABLE IF NOT EXISTS won't retrofit it, and
# the index above requires position_logs.signal_id to exist — add the missing
# columns first, skipping the ones a from-scratch DB already got out of
# _POSITIONS_TABLE / _POSITION_LOGS_TABLE.
_RETROFIT_COLUMNS = (
  ("positions", "signal_id", "TEXT"),
  ("positions", "signal_uxid", "TEXT"),
  ("position_logs", "signal_id", "TEXT"),
  ("position_logs", "signal_uxid", "TEXT"),
)


def _ensure_column(conn, table: str, column: str, column_type: str) -> None:
  columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
  if column not in columns:
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def _ensure_retrofit_columns(conn) -> None:
  for table, column, column_type in _RETROFIT_COLUMNS:
    _ensure_column(conn, table, column, column_type)


def _create_tables(conn) -> None:
  conn.execute(_POSITIONS_TABLE)
  conn.execute(_POSITION_LOGS_TABLE)
  conn.execute(_NOTIFICATIONS_TABLE)
  conn.execute(_NOTIFICATIONS_INDEX)
  conn.execute(_NOTIFICATION_CYCLES_TABLE)
  conn.execute(_NOTIFICATION_CYCLES_KEY_INDEX)
  conn.execute(_NOTIFICATION_CYCLE_CHATS_TABLE)
  conn.execute(_NOTIFICATION_CYCLE_CHATS_KEY_INDEX)
  conn.execute(_NOTIFICATION_CYCLE_CHATS_PENDING_INDEX)
  _ensure_retrofit_columns(conn)
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
