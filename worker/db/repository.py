"""
worker/db/repository.py
───────────────────────
SQLite persistence for positions, position logs, and the notification outbox.

The physical ``positions`` / ``position_logs`` columns are gateway-neutral
(``ref_id``, ``ref_source_id``, ``strategy_code``, ``gateway_return_code``,
``gateway_message`` — see :mod:`worker.db.schema`). Callers, the
``PositionEvent`` NATS contract, and the rest of the worker all speak these same
generic names, so there is no name translation here.

The one boundary concern this repository owns is id representation: ``ref_id`` /
``ref_source_id`` are stored as TEXT (so any gateway's id format fits) and
returned as strings; callers cast to their preferred numeric type as needed.

Three repositories live here, one per lifecycle:
:class:`PositionRepository` (trade state), :class:`NotificationOutboxRepository`
(send-once messages) and :class:`NotificationCycleRepository` (the one message
per trade that is edited in place as the trade progresses).
"""

import json
import sqlite3
from typing import Iterable, Optional

from worker.db.connection import _get_conn
from worker.logger import get_logger
from worker.schemas.cycle_schema import CycleStatusEnum, merge_status
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
    return dict(row)

  def log_position(
    self,
    strategy: str,
    ref_id: Optional[str],
    ref_source_id: Optional[str],
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
    signal_id: Optional[str] = None,
    signal_uxid: Optional[str] = None,
  ):
    conn = None
    try:
      conn = _get_conn()
      cursor = conn.cursor()
      cursor.execute(
        """
              INSERT INTO position_logs (strategy, ref_id, ref_source_id, signal_id, signal_uxid, symbol, action, volume, price, sl, tp1, gateway_return_code, comment, gateway_message, author, market_type)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """,
        # Store the broker-native price as-is. Rounding to 2 decimals (a forex-era
        # assumption) corrupts low-priced crypto (e.g. SHIB → 0.00) and is
        # inconsistent with sl/tp1, which are already stored unrounded. The caller
        # supplies a price already quantized to the instrument's tick.
        (
          strategy,
          ref_id,
          ref_source_id,
          signal_id,
          signal_uxid,
          symbol,
          action,
          volume,
          price,
          sl,
          tp1,
          gateway_return_code,
          comment,
          message,
          author,
          market_type,
        ),
      )
      conn.commit()
      logger.debug(
        f"Order logged to DB: ref_id={ref_id}, code={gateway_return_code}, Author={author}"
      )
    except Exception as exc:
      logger.error(
        "log_position failed (strategy=%s symbol=%s action=%s): %s",
        strategy,
        symbol,
        action,
        exc,
      )
    finally:
      if conn:
        conn.close()

  def _insert_position_row(
    self,
    *,
    ref_id: str,
    strategy: str,
    symbol: str,
    action: str,
    volume: float,
    opened_price: float,
    status: str,
    gateway_return_code: Optional[int],
    comment: Optional[str],
    message: Optional[str],
    strategy_code: Optional[int],
    market_type: Optional[str],
    signal_id: Optional[str] = None,
    signal_uxid: Optional[str] = None,
  ) -> None:
    """Insert a single positions row at ``status``. ref_source_id is seeded from
    ref_id (they diverge later only when a partial close re-tickets the order).
    Shared by :meth:`insert_position` (OPENED) and :meth:`insert_rejected_position`
    (REJECTED); each owns its own error handling."""
    conn = None
    try:
      conn = _get_conn()
      cursor = conn.cursor()
      cursor.execute(
        """
              INSERT INTO positions (ref_source_id, ref_id, signal_id, signal_uxid, strategy, symbol, action, volume, opened_price, status, gateway_return_code, comment, gateway_message, strategy_code, market_type)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """,
        # Broker-native price, no rounding — see log_position for the rationale.
        (
          ref_id,
          ref_id,
          signal_id,
          signal_uxid,
          strategy,
          symbol,
          action,
          volume,
          opened_price,
          status,
          gateway_return_code,
          comment,
          message,
          strategy_code,
          market_type,
        ),
      )
      conn.commit()
    finally:
      if conn:
        conn.close()

  def insert_position(
    self,
    ref_id: str,
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
    signal_id: Optional[str] = None,
    signal_uxid: Optional[str] = None,
  ):
    try:
      self._insert_position_row(
        ref_id=ref_id,
        strategy=strategy,
        symbol=symbol,
        action=action,
        volume=volume,
        opened_price=opened_price,
        status="OPENED",
        gateway_return_code=gateway_return_code,
        comment=comment,
        message=message,
        strategy_code=strategy_code,
        market_type=market_type,
        signal_id=signal_id,
        signal_uxid=signal_uxid,
      )
      logger.debug(
        f"Position inserted: ref_source_id={ref_id}, symbol={symbol}, action={action}"
      )
    except Exception as exc:
      logger.critical(
        "insert_position FAILED (ref_id=%s strategy=%s symbol=%s): %s — "
        "position is open on exchange but NOT tracked in DB. Manual reconciliation required.",
        ref_id,
        strategy,
        symbol,
        exc,
      )
      raise

  def insert_rejected_position(
    self,
    ref_id: str,
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
    signal_id: Optional[str] = None,
    signal_uxid: Optional[str] = None,
  ):
    """Record an entry that a worker-side policy rejected (e.g. MAX_OPEN_ORDERS)
    as a REJECTED row, so it is auditable and picked up by :class:`PositionCDC` and
    forwarded to the broker on the TRADE subject with status REJECTED.

    Unlike :meth:`insert_position`, a failure here is logged but NOT re-raised: no
    live order exists to reconcile, and the rejection must not abort signal
    processing (the operator notification still needs to fire)."""
    try:
      self._insert_position_row(
        ref_id=ref_id,
        strategy=strategy,
        symbol=symbol,
        action=action,
        volume=volume,
        opened_price=opened_price,
        status=PositionStatusEnum.REJECTED.value,
        gateway_return_code=gateway_return_code,
        comment=comment,
        message=message,
        strategy_code=strategy_code,
        market_type=market_type,
        signal_id=signal_id,
        signal_uxid=signal_uxid,
      )
      logger.debug(
        f"Rejected entry recorded: ref_source_id={ref_id}, symbol={symbol}, action={action}"
      )
    except Exception as exc:
      logger.error(
        "insert_rejected_position failed (ref_id=%s strategy=%s symbol=%s): %s",
        ref_id,
        strategy,
        symbol,
        exc,
      )

  def update_position_status(
    self,
    ref_source_id: str,
    status: PositionStatusEnum,
    ref_id: Optional[str] = None,
    closed_price: Optional[float] = None,
    gateway_return_code: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ):
    conn = None
    try:
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
        # Broker-native close price, no rounding — see log_position for the rationale.
        (
          status.value,
          ref_id,
          closed_price,
          gateway_return_code,
          comment,
          message,
          ref_source_id,
        ),
      )
      conn.commit()
      logger.debug(
        f"Position updated: ref_source_id={ref_source_id}, new ref_id={ref_id}, status={status}"
      )
    except Exception as exc:
      logger.critical(
        "update_position_status FAILED (ref_source_id=%s status=%s): %s — "
        "position status not updated in DB. Manual reconciliation may be required.",
        ref_source_id,
        status,
        exc,
      )
      raise
    finally:
      if conn:
        conn.close()

  def get_position(self, ref_source_id: str) -> Optional[dict]:
    conn = None
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute(
        "SELECT * FROM positions WHERE ref_source_id = ?", (ref_source_id,)
      )
      row = cursor.fetchone()
      return self._row_to_dict(row) if row else None
    except Exception as e:
      logger.exception(f"Failed to fetch position ref_source_id={ref_source_id}: {e}")
      return None
    finally:
      if conn:
        conn.close()

  def get_pending_sync_positions(self) -> list:
    conn = None
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM positions WHERE sync_status = 'PENDING'")
      rows = cursor.fetchall()
      return [self._row_to_dict(row) for row in rows]
    except Exception as e:
      logger.exception(f"Failed to fetch pending sync positions: {e}")
      return []
    finally:
      if conn:
        conn.close()

  def mark_position_synced(self, position_id: int, updated_at: str) -> bool:
    conn = None
    try:
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
      return changed
    except Exception as exc:
      logger.error("mark_position_synced failed (id=%s): %s", position_id, exc)
      return False
    finally:
      if conn:
        conn.close()

  def get_open_positions_by_strategy(self, strategy: str, symbol: str) -> list:
    conn = None
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute(
        "SELECT * FROM positions WHERE strategy = ? AND symbol = ? AND status IN ('OPENED', 'TP1')",
        (strategy, symbol),
      )
      rows = cursor.fetchall()
      return [self._row_to_dict(row) for row in rows]
    except Exception as e:
      logger.exception(
        f"Failed to fetch open positions for strategy={strategy} symbol={symbol}: {e}"
      )
      return []
    finally:
      if conn:
        conn.close()

  def signal_exists(self, signal_id: str) -> bool:
    """True if any position_logs row already carries this ``signal_id``.

    Called by the ACK signal-replay handler in
    :class:`~worker.gateways.processor.BaseSignalProcessor` to skip a replay for
    a signal the worker has already processed (successfully or as a REJECT).
    An empty/None ``signal_id`` is treated as "not seen" — the caller decides
    what to do with a replay carrying no id (currently: pass it through)."""
    if not signal_id:
      return False
    conn = None
    try:
      conn = _get_conn()
      cursor = conn.cursor()
      cursor.execute(
        "SELECT 1 FROM position_logs WHERE signal_id = ? LIMIT 1",
        (signal_id,),
      )
      return cursor.fetchone() is not None
    except Exception as exc:
      logger.exception("signal_exists failed (signal_id=%s): %s", signal_id, exc)
      # Fail-safe: treat as "seen" so a DB error does NOT let a replay
      # double-execute the signal.
      return True
    finally:
      if conn:
        conn.close()

  def get_open_positions_for_flat(
    self,
    strategy: Optional[str] = None,
    symbol: Optional[str] = None,
    ref_id: Optional[str] = None,
  ) -> list:
    """Fetch all OPENED/TP1 positions, optionally filtered by strategy, symbol and/or ref_id."""
    conn = None
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
      if ref_id:
        conditions.append("ref_id = ?")
        params.append(ref_id)
      cursor.execute(
        f"SELECT * FROM positions WHERE {' AND '.join(conditions)}", params
      )
      rows = cursor.fetchall()
      return [self._row_to_dict(row) for row in rows]
    except Exception as e:
      logger.exception(
        f"Failed to fetch open positions for flat (strategy={strategy}, symbol={symbol}): {e}"
      )
      return []
    finally:
      if conn:
        conn.close()


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
    conn = None
    try:
      conn = _get_conn()
      cursor = conn.cursor()
      cursor.execute(
        """
              INSERT INTO notifications (platform, channel, category, message_text, mode, max_attempts)
              VALUES (?, ?, ?, ?, ?, ?)
          """,
        (
          platform.value,
          channel.value,
          category,
          message_text,
          mode.value,
          max_attempts,
        ),
      )
      conn.commit()
    except Exception as exc:
      logger.error("enqueue_notification failed (channel=%s): %s", channel, exc)
    finally:
      if conn:
        conn.close()

  def get_due_notifications(self, limit: int = 20) -> list:
    conn = None
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
      return [dict(row) for row in rows]
    except Exception as e:
      logger.exception(f"Failed to fetch due notifications: {e}")
      return []
    finally:
      if conn:
        conn.close()

  def delete_notification(self, notification_id: int) -> None:
    conn = None
    try:
      conn = _get_conn()
      cursor = conn.cursor()
      cursor.execute("DELETE FROM notifications WHERE id = ?", (notification_id,))
      conn.commit()
    except Exception as exc:
      logger.error("delete_notification failed (id=%s): %s", notification_id, exc)
    finally:
      if conn:
        conn.close()

  def mark_notification_failed(
    self, notification_id: int, error: str, next_attempt_at: str
  ) -> None:
    conn = None
    try:
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
    except Exception as exc:
      logger.error("mark_notification_failed failed (id=%s): %s", notification_id, exc)
    finally:
      if conn:
        conn.close()


class NotificationCycleRepository:
  """Stores one signal cycle per (strategy, signal_uxid) and its per-chat state.

  Split from :class:`NotificationOutboxRepository` because the two have opposite
  lifecycles: an outbox row is a *message* that is sent once and deleted, while a
  cycle row is a *conversation* that keeps growing and whose one Telegram message
  is rewritten in place. Sharing the outbox table would have meant either
  re-sending the whole trade on every action or teaching the outbox to update
  rows it is meant to hard-delete.

  Ordering safety comes from ``revision``: appending an action bumps it, and each
  chat records the highest revision it has actually been shown
  (``delivered_revision``), so a slow edit can never overwrite a newer body with
  an older one.
  """

  @staticmethod
  def _decode_events(raw: Optional[str]) -> list:
    """Parse the ``events`` JSON column, tolerating a corrupt value.

    A cycle whose JSON cannot be read is worth restarting from empty rather than
    aborting the trade notification: the message is a report, never the record.
    """
    if not raw:
      return []
    try:
      events = json.loads(raw)
    except (TypeError, ValueError):
      logger.warning("notification_cycles.events is not valid JSON — starting fresh")
      return []
    return events if isinstance(events, list) else []

  def append_event(
    self,
    *,
    strategy: str,
    signal_uxid: str,
    symbol: str,
    event: dict,
    status: Optional[CycleStatusEnum] = None,
  ) -> Optional[dict]:
    """Add *event* to the cycle's timeline, creating the cycle on first sight.

    Returns the cycle as the caller must render it — ``id``, ``revision``, the
    full ``events`` list and the merged ``status`` — or ``None`` when the write
    failed. The read-modify-write runs inside one IMMEDIATE transaction so two
    actions arriving back to back cannot lose each other's event.
    """
    conn = None
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      conn.isolation_level = None  # explicit transaction control below
      cursor = conn.cursor()
      cursor.execute("BEGIN IMMEDIATE")
      cursor.execute(
        "SELECT id, status, events, revision FROM notification_cycles "
        "WHERE strategy = ? AND signal_uxid = ?",
        (strategy, signal_uxid),
      )
      row = cursor.fetchone()

      events = self._decode_events(row["events"]) if row else []
      events.append(event)
      current = CycleStatusEnum(row["status"]) if row and row["status"] else None
      merged = merge_status(current, status)
      merged_value = merged.value if merged is not None else None

      if row is None:
        cursor.execute(
          "INSERT INTO notification_cycles (signal_uxid, strategy, symbol, status, events, revision) "
          "VALUES (?, ?, ?, ?, ?, 1)",
          (signal_uxid, strategy, symbol, merged_value, json.dumps(events)),
        )
        cycle_id = cursor.lastrowid
        revision = 1
      else:
        cycle_id = row["id"]
        revision = row["revision"] + 1
        cursor.execute(
          "UPDATE notification_cycles "
          "SET status = ?, events = ?, revision = ?, updated_at = CURRENT_TIMESTAMP "
          "WHERE id = ?",
          (merged_value, json.dumps(events), revision, cycle_id),
        )
      cursor.execute("COMMIT")
      return {
        "id": cycle_id,
        "signal_uxid": signal_uxid,
        "strategy": strategy,
        "symbol": symbol,
        "status": merged_value,
        "events": events,
        "revision": revision,
      }
    except Exception as exc:
      logger.error(
        "append_event failed (strategy=%s signal_uxid=%s): %s",
        strategy,
        signal_uxid,
        exc,
      )
      if conn is not None:
        try:
          conn.execute("ROLLBACK")
        except Exception:  # pragma: no cover — nothing left to roll back
          pass
      return None
    finally:
      if conn:
        conn.close()

  def set_message_text(self, cycle_id: int, message_text: str, revision: int) -> None:
    """Store the body rendered for *revision*.

    The ``revision <= ?`` guard makes a late render harmless: if a newer action
    has already re-rendered the cycle, the older text is dropped instead of
    replacing it.
    """
    conn = None
    try:
      conn = _get_conn()
      conn.execute(
        "UPDATE notification_cycles SET message_text = ?, updated_at = CURRENT_TIMESTAMP "
        "WHERE id = ? AND revision <= ?",
        (message_text, cycle_id, revision),
      )
      conn.commit()
    except Exception as exc:
      logger.error("set_message_text failed (cycle_id=%s): %s", cycle_id, exc)
    finally:
      if conn:
        conn.close()

  def ensure_cycle_chats(self, cycle_id: int, chat_ids: Iterable[str]) -> None:
    """Make sure the cycle has a delivery row per chat.

    ``chat_ids`` are the raw setting entries (topic suffix included), which is
    what makes two topics of one group two separate deliveries.

    Called on every recorded action rather than only at creation, so a chat id
    added to the configuration mid-trade still picks the cycle up (it receives
    the whole story in its first message, since the body is always rendered in
    full).
    """
    conn = None
    try:
      conn = _get_conn()
      conn.executemany(
        "INSERT OR IGNORE INTO notification_cycle_chats (cycle_id, chat_id) VALUES (?, ?)",
        [(cycle_id, str(chat_id)) for chat_id in chat_ids if chat_id],
      )
      conn.commit()
    except Exception as exc:
      logger.error("ensure_cycle_chats failed (cycle_id=%s): %s", cycle_id, exc)
    finally:
      if conn:
        conn.close()

  def get_due_cycle_deliveries(self, limit: int = 20) -> list:
    """Chat rows whose message is behind the cycle's current body.

    A row is due when its ``delivered_revision`` trails the cycle's ``revision``,
    its backoff has elapsed and it has attempts left. Rows that exhausted
    ``max_attempts`` stay in place as dead letters, exactly like the outbox.
    """
    conn = None
    try:
      conn = _get_conn()
      conn.row_factory = sqlite3.Row
      cursor = conn.cursor()
      cursor.execute(
        """
                SELECT c.id           AS chat_row_id,
                       c.chat_id      AS chat_id,
                       c.message_id   AS message_id,
                       c.attempts     AS attempts,
                       m.id           AS cycle_id,
                       m.revision     AS revision,
                       m.message_text AS message_text,
                       m.signal_uxid  AS signal_uxid,
                       m.strategy     AS strategy
                FROM notification_cycle_chats c
                JOIN notification_cycles m ON m.id = c.cycle_id
                WHERE c.delivered_revision < m.revision
                  AND m.message_text IS NOT NULL
                  AND c.attempts < c.max_attempts
                  AND (c.next_attempt_at IS NULL OR c.next_attempt_at <= CURRENT_TIMESTAMP)
                ORDER BY m.id ASC, c.id ASC
                LIMIT ?
            """,
        (limit,),
      )
      return [dict(row) for row in cursor.fetchall()]
    except Exception as exc:
      logger.exception("get_due_cycle_deliveries failed: %s", exc)
      return []
    finally:
      if conn:
        conn.close()

  def mark_cycle_delivered(
    self, chat_row_id: int, message_id: Optional[str], revision: int
  ) -> None:
    """Record that *chat_row_id* now shows the body rendered for *revision*.

    ``delivered_revision`` only ever moves forward (``MAX``), so a delivery that
    overtook a newer one is not undone by the slower of the two finishing last.
    """
    conn = None
    try:
      conn = _get_conn()
      conn.execute(
        """
              UPDATE notification_cycle_chats
              SET message_id = COALESCE(?, message_id),
                  delivered_revision = MAX(delivered_revision, ?),
                  attempts = 0,
                  last_error = NULL,
                  next_attempt_at = NULL,
                  updated_at = CURRENT_TIMESTAMP
              WHERE id = ?
          """,
        (message_id, revision, chat_row_id),
      )
      conn.commit()
    except Exception as exc:
      logger.error("mark_cycle_delivered failed (id=%s): %s", chat_row_id, exc)
    finally:
      if conn:
        conn.close()

  def mark_cycle_delivery_failed(
    self, chat_row_id: int, error: str, next_attempt_at: str
  ) -> None:
    conn = None
    try:
      conn = _get_conn()
      conn.execute(
        """
              UPDATE notification_cycle_chats
              SET attempts = attempts + 1,
                  last_error = ?,
                  next_attempt_at = ?,
                  updated_at = CURRENT_TIMESTAMP
              WHERE id = ?
          """,
        (error, next_attempt_at, chat_row_id),
      )
      conn.commit()
    except Exception as exc:
      logger.error("mark_cycle_delivery_failed failed (id=%s): %s", chat_row_id, exc)
    finally:
      if conn:
        conn.close()

  def clear_cycle_message_id(self, chat_row_id: int) -> None:
    """Forget the Telegram message id after an edit found it gone.

    The row stays due, so the next pass posts a fresh message for the cycle
    instead of editing one the chat no longer has.
    """
    conn = None
    try:
      conn = _get_conn()
      conn.execute(
        "UPDATE notification_cycle_chats "
        "SET message_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (chat_row_id,),
      )
      conn.commit()
    except Exception as exc:
      logger.error("clear_cycle_message_id failed (id=%s): %s", chat_row_id, exc)
    finally:
      if conn:
        conn.close()
