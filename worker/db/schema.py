from worker.db.connection import _get_conn
from worker.logger import get_logger

logger = get_logger("worker.db.schema")


def db_init():
  try:
    conn = _get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()
    cursor.execute("""
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
                sync_status TEXT NOT NULL DEFAULT 'PENDING',
                sync_time DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS position_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                ticket INTEGER,
                source_ticket INTEGER,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                volume REAL NOT NULL,
                price REAL NOT NULL,
                sl REAL,
                tp1 REAL,
                mt5_retcode INTEGER,
                comment TEXT,
                message TEXT,
                author TEXT NOT NULL DEFAULT 'broker',
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    cursor.execute("""
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
        """)
    cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_pending
                ON notifications (next_attempt_at, id)
        """)
    conn.commit()
    _apply_migrations(conn)
    conn.close()
    logger.info("Database initialized successfully.")
  except Exception as e:
    logger.exception(f"Database initialization failed: {e}")


def _apply_migrations(conn) -> None:
  """Run idempotent schema migrations on an existing connection.

  Each migration is guarded so it is safe to re-run on every startup.
  """
  cursor = conn.cursor()

  # ── Migration: enforce at most one active position per (strategy, symbol) ──
  #
  # Before adding the unique index we deduplicate any existing rows so the
  # CREATE INDEX does not fail on a DB that was created before this constraint
  # existed.  The oldest row (MIN id) is kept; any extras are marked
  # FORCED_CLOSED so they surface in audit queries but no longer block inserts.
  cursor.execute("""
      UPDATE positions
      SET status     = 'FORCED_CLOSED',
          comment    = 'Deduplicated on startup: multiple active rows for same strategy+symbol',
          updated_at = CURRENT_TIMESTAMP
      WHERE status IN ('OPENED', 'TP1')
        AND id NOT IN (
            SELECT MIN(id)
            FROM   positions
            WHERE  status IN ('OPENED', 'TP1')
            GROUP  BY strategy, symbol
        )
  """)
  if cursor.rowcount:
    logger.warning(
      "[db_migrate] Deduplicated %d orphan active position row(s) before applying unique index.",
      cursor.rowcount,
    )

  # Partial unique index: only one OPENED or TP1 row allowed per strategy+symbol.
  # Closed/force-closed rows are unrestricted.
  cursor.execute("""
      CREATE UNIQUE INDEX IF NOT EXISTS uidx_positions_one_active_per_strategy_symbol
          ON positions (strategy, symbol)
          WHERE status = 'OPENED' OR status = 'TP1'
  """)
  conn.commit()

