import sqlite3

from worker.logger import get_logger

logger = get_logger("worker.db")

DB_FILE = "worker_data.sqlite"


def _get_conn() -> sqlite3.Connection:
  conn = sqlite3.connect(DB_FILE, timeout=5)
  conn.execute("PRAGMA busy_timeout=5000")
  return conn


def init_db():
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
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully.")
  except Exception as e:
    logger.exception(f"Database initialization failed: {e}")
