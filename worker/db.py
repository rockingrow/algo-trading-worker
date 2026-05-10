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
            CREATE TABLE IF NOT EXISTS order_logs (
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


def log_order(
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
            INSERT INTO order_logs (strategy, ticket, source_ticket, symbol, action, volume, price, sl, tp1, mt5_retcode, comment, message, author)
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
