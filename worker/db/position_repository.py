"""
worker/db/position_repository.py
──────────────────────────────────
SQLite persistence for the ``positions`` and ``position_logs`` tables.

Split out of the former monolithic ``DBService`` so position persistence has a
single reason to change, separate from the notification outbox (Single
Responsibility).
"""

import sqlite3
from typing import Optional

from worker.db.connection import _get_conn
from worker.logger import get_logger
from worker.schemas.position_schema import PositionStatusEnum

logger = get_logger("worker.db.position_repository")


class PositionRepository:
  """Reads/writes trade positions and the append-only position log."""

  def log_position(
    self,
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
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            INSERT INTO position_logs (strategy, ticket, source_ticket, symbol, action, volume, price, sl, tp1, mt5_retcode, comment, message, author)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      (strategy, ticket, source_ticket, symbol, action, volume, round(price, 2), sl, tp1, mt5_retcode, comment, message, author),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Order logged to DB: Ticket={ticket}, Retcode={mt5_retcode}, Author={author}")

  def insert_position(
    self,
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
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            INSERT INTO positions (source_ticket, ticket, strategy, symbol, action, volume, opened_price, status, mt5_retcode, comment, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      (ticket, ticket, strategy, symbol, action, volume, round(opened_price, 2), "OPENED", mt5_retcode, comment, message),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Position inserted: source_ticket={ticket}, symbol={symbol}, action={action}")

  def update_position_status(
    self,
    source_ticket: int,
    status: PositionStatusEnum,
    new_ticket: Optional[int] = None,
    closed_price: Optional[float] = None,
    mt5_retcode: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ):
    conn = _get_conn()
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
                sync_status = 'PENDING',
                updated_at = CURRENT_TIMESTAMP
            WHERE source_ticket = ?
        """,
      (status.value, new_ticket, round(closed_price, 2) if closed_price is not None else None, mt5_retcode, comment, message, source_ticket),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Position updated: source_ticket={source_ticket}, new_ticket={new_ticket}, status={status}")

  def get_position(self, source_ticket: int) -> Optional[dict]:
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM positions WHERE source_ticket = ?", (source_ticket,))
      row = cursor.fetchone()
      conn.close()
      return dict(row) if row else None
    except Exception as e:
      logger.exception(f"Failed to fetch position source_ticket={source_ticket}: {e}")
      return None

  def get_pending_sync_positions(self) -> list:
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM positions WHERE sync_status = 'PENDING'")
      rows = cursor.fetchall()
      conn.close()
      return [dict(row) for row in rows]
    except Exception as e:
      logger.exception(f"Failed to fetch pending sync positions: {e}")
      return []

  def mark_position_synced(self, position_id: int, updated_at: str) -> bool:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            UPDATE positions
            SET sync_status = 'PUBLISHED',
                sync_time = CURRENT_TIMESTAMP
            WHERE id = ? AND updated_at = ? AND sync_status = 'PENDING'
        """,
      (position_id, updated_at),
    )
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return changed

  def get_open_positions_by_strategy(self, strategy: str, symbol: str) -> list:
    try:
      conn = _get_conn()
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
