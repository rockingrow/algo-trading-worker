"""
worker/db/repository.py
───────────────────────
SQLite persistence for positions, position logs, and the notification outbox.

The physical ``positions`` / ``position_logs`` columns are gateway-neutral
(``ref_id``, ``ref_source_id``, ``strategy_code``, ``gateway_return_code``,
``gateway_message`` — see :mod:`worker.db.schema`). This repository is the single
translation boundary between those columns and the application domain, which
continues to use ``ticket`` / ``source_ticket`` (as ``int``), ``magic``,
``mt5_retcode`` and ``message`` — so callers and the NATS ``PositionEvent``
contract are unaffected.

``ref_id`` / ``ref_source_id`` are stored as TEXT (any gateway's id format fits)
and parsed text↔int here at the boundary.
"""

import sqlite3
from typing import Optional

from worker.db.connection import _get_conn
from worker.logger import get_logger
from worker.utils.parsing import int_to_string, string_to_int
from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationModeEnum,
  NotificationPlatformEnum,
)
from worker.schemas.position_schema import PositionStatusEnum

logger = get_logger("worker.db.repository")


class PositionRepository:
  """Reads/writes trade positions and the append-only position log."""

  @staticmethod
  def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    if "ref_id" in d:
      d["ref_id"] = string_to_int(d["ref_id"])
    if "ref_source_id" in d:
      d["ref_source_id"] = string_to_int(d["ref_source_id"])
    return d

  def log_position(
    self,
    strategy: str,
    ref_id: Optional[int],
    ref_source_id: Optional[int],
    symbol: str,
    action: str,
    volume: float,
    price: float,
    sl: Optional[float],
    tp1: Optional[float],
    gateway_return_code: int,
    comment: str = "",
    message: Optional[str] = None,
    author: str = "broker",
    market_type: Optional[str] = None,
  ):
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            INSERT INTO position_logs (strategy, ref_id, ref_source_id, symbol, action, volume, price, sl, tp1, gateway_return_code, comment, gateway_message, author, market_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      (strategy, int_to_string(ref_id), int_to_string(ref_source_id), symbol, action, volume, round(price, 2) if price is not None else None, sl, tp1, gateway_return_code, comment, message, author, market_type),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Order logged to DB: ref_id={ref_id}, code={gateway_return_code}, Author={author}")

  def insert_position(
    self,
    ref_id: int,
    strategy: str,
    symbol: str,
    action: str,
    volume: float,
    opened_price: float,
    gateway_return_code: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
    strategy_code: Optional[int] = None,
    market_type: Optional[str] = None,
  ):
    conn = _get_conn()
    cursor = conn.cursor()
    ref = int_to_string(ref_id)
    cursor.execute(
      """
            INSERT INTO positions (ref_source_id, ref_id, strategy, symbol, action, volume, opened_price, status, gateway_return_code, comment, gateway_message, strategy_code, market_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      (ref, ref, strategy, symbol, action, volume, round(opened_price, 2), "OPENED", gateway_return_code, comment, message, strategy_code, market_type),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Position inserted: ref_source_id={ref}, symbol={symbol}, action={action}")

  def update_position_status(
    self,
    ref_source_id: int,
    status: PositionStatusEnum,
    ref_id: Optional[int] = None,
    closed_price: Optional[float] = None,
    gateway_return_code: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ):
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            UPDATE positions
            SET status = ?,
                ref_id = COALESCE(?, ref_id),
                closed_price = COALESCE(?, closed_price),
                gateway_return_code = COALESCE(?, gateway_return_code),
                comment = COALESCE(?, comment),
                gateway_message = COALESCE(?, gateway_message),
                sync_status = 'PENDING',
                updated_at = CURRENT_TIMESTAMP
            WHERE ref_source_id = ?
        """,
      (status.value, int_to_string(ref_id), round(closed_price, 2) if closed_price is not None else None, gateway_return_code, comment, message, int_to_string(ref_source_id)),
    )
    conn.commit()
    conn.close()
    logger.debug(f"Position updated: ref_source_id={ref_source_id}, new ref_id={ref_id}, status={status}")

  def get_position(self, ref_source_id: int) -> Optional[dict]:
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM positions WHERE ref_source_id = ?", (int_to_string(ref_source_id),))
      row = cursor.fetchone()
      conn.close()
      return self._row_to_dict(row) if row else None
    except Exception as e:
      logger.exception(f"Failed to fetch position ref_source_id={ref_source_id}: {e}")
      return None

  def get_pending_sync_positions(self) -> list:
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM positions WHERE sync_status = 'PENDING'")
      rows = cursor.fetchall()
      conn.close()
      return [self._row_to_dict(row) for row in rows]
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
      return [self._row_to_dict(row) for row in rows]
    except Exception as e:
      logger.exception(f"Failed to fetch open positions for strategy={strategy} symbol={symbol}: {e}")
      return []

  def get_open_positions_for_flat(
    self,
    strategy: Optional[str] = None,
    symbol: Optional[str] = None,
  ) -> list:
    """Fetch all OPENED/TP1 positions, optionally filtered by strategy and/or symbol."""
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      conditions = ["status IN ('OPENED', 'TP1')"]
      params: list = []
      if strategy:
        conditions.append("strategy = ?")
        params.append(strategy)
      if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
      cursor.execute(f"SELECT * FROM positions WHERE {' AND '.join(conditions)}", params)
      rows = cursor.fetchall()
      conn.close()
      return [self._row_to_dict(row) for row in rows]
    except Exception as e:
      logger.exception(f"Failed to fetch open positions for flat (strategy={strategy}, symbol={symbol}): {e}")
      return []


class NotificationOutboxRepository:
  """Enqueues, fetches, and retires rows in the notification outbox."""

  def enqueue_notification(
    self,
    platform: NotificationPlatformEnum,
    channel: NotificationChannelEnum,
    message_text: str,
    category: Optional[str] = None,
    mode: NotificationModeEnum = NotificationModeEnum.VERBOSE,
    max_attempts: int = 5,
  ) -> None:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            INSERT INTO notifications (platform, channel, category, message_text, mode, max_attempts)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
      (platform.value, channel.value, category, message_text, mode.value, max_attempts),
    )
    conn.commit()
    conn.close()

  def get_due_notifications(self, limit: int = 20) -> list:
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute(
        """
                SELECT * FROM notifications
                WHERE mode = 'VERBOSE'
                  AND attempts < max_attempts
                  AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
                ORDER BY id ASC
                LIMIT ?
            """,
        (limit,),
      )
      rows = cursor.fetchall()
      conn.close()
      return [dict(row) for row in rows]
    except Exception as e:
      logger.exception(f"Failed to fetch due notifications: {e}")
      return []

  def delete_notification(self, notification_id: int) -> None:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM notifications WHERE id = ?", (notification_id,))
    conn.commit()
    conn.close()

  def mark_notification_failed(self, notification_id: int, error: str, next_attempt_at: str) -> None:
    conn = _get_conn()
    cursor = conn.cursor()
    cursor.execute(
      """
            UPDATE notifications
            SET attempts = attempts + 1,
                last_error = ?,
                next_attempt_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
      (error, next_attempt_at, notification_id),
    )
    conn.commit()
    conn.close()
