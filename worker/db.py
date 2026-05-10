import sqlite3
from typing import Optional

from worker.logger import get_logger
logger = get_logger("worker.db")

DB_FILE = "worker_data.sqlite"


def init_db():
  try:
    conn = sqlite3.connect(DB_FILE)
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


def log_position(
  strategy: str,
  ticket: Optional[int],
  source_ticket: Optional[int],
  symbol: str,
  action: str,
  volume: float,
  price: float,
  sl: Optional[float],
  tp1: Optional[float],
  mt5_retcode: int,
  comment: str = "",
  message: Optional[str] = None,
  author: str = "broker",
):
  """Save order execution results to DB."""
  try:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
      """
            INSERT INTO position_logs (strategy, ticket, source_ticket, symbol, action, volume, price, sl, tp1, mt5_retcode, comment, message, author)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      (
        strategy,
        ticket,
        source_ticket,
        symbol,
        action,
        volume,
        price,
        sl,
        tp1,
        mt5_retcode,
        comment,
        message,
        author,
      ),
    )
    conn.commit()
    conn.close()
    logger.debug(
      f"Order logged to DB: Ticket={ticket}, Retcode={mt5_retcode}, Author={author}"
    )
  except Exception as e:
    logger.exception(f"Failed to log order to DB: {e}")


def insert_position(
  ticket: int,
  strategy: str,
  symbol: str,
  action: str,
  volume: float,
  opened_price: float,
  mt5_retcode: Optional[int] = None,
  comment: Optional[str] = None,
  message: Optional[str] = None,
):
  """Insert a newly opened position. Only call after a successful open order."""
  try:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
      """
            INSERT INTO positions (source_ticket, ticket, strategy, symbol, action, volume, opened_price, status, mt5_retcode, comment, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      (ticket, ticket, strategy, symbol, action, volume, opened_price, "OPENED", mt5_retcode, comment, message),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Position inserted: source_ticket={ticket}, symbol={symbol}, action={action}")
  except Exception as e:
    logger.exception(f"Failed to insert position ticket={ticket}: {e}")


def update_position_status(
  source_ticket: int,
  status: str,
  new_ticket: Optional[int] = None,
  closed_price: Optional[float] = None,
  mt5_retcode: Optional[int] = None,
  comment: Optional[str] = None,
  message: Optional[str] = None,
):
  """Update position status. Pass new_ticket to sync the latest MT5 deal ticket."""
  try:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
      """
            UPDATE positions
            SET status = ?,
                ticket = COALESCE(?, ticket),
                closed_price = COALESCE(?, closed_price),
                mt5_retcode = COALESCE(?, mt5_retcode),
                comment = COALESCE(?, comment),
                message = COALESCE(?, message),
                updated_at = CURRENT_TIMESTAMP
            WHERE source_ticket = ?
        """,
      (status, new_ticket, closed_price, mt5_retcode, comment, message, source_ticket),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Position updated: source_ticket={source_ticket}, new_ticket={new_ticket}, status={status}")
  except Exception as e:
    logger.exception(f"Failed to update position source_ticket={source_ticket}: {e}")


def get_position(source_ticket: int) -> Optional[dict]:
  """Fetch a position row by source_ticket. Returns None if not found."""
  try:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM positions WHERE source_ticket = ?", (source_ticket,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None
  except Exception as e:
    logger.exception(f"Failed to fetch position source_ticket={source_ticket}: {e}")
    return None


def get_open_positions_by_strategy(strategy: str, symbol: str) -> list:
  """Fetch all OPENED or TP1 positions for a given strategy+symbol."""
  try:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
      "SELECT * FROM positions WHERE strategy = ? AND symbol = ? AND status IN ('OPENED', 'TP1')",
      (strategy, symbol),
    )
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
  except Exception as e:
    logger.exception(f"Failed to fetch open positions for strategy={strategy} symbol={symbol}: {e}")
    return []
