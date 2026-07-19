"""
worker/jobs/position_cdc_job.py
────────────────────────────────────
Background polling thread that implements Change Data Capture on the SQLite
`positions` table. Watches for rows whose `sync_status` is PENDING (either
freshly inserted or just updated), publishes a PositionEvent to the NATS TRADE
subject, and marks the row as PUBLISHED. Update detection is row-scoped via
`sync_status`, so polling cost stays O(pending) instead of O(table).
"""

from __future__ import annotations

import json
import threading
from typing import Any, Callable, Dict, Optional

from worker.interfaces.db_protocol import PositionSyncStoreProtocol
from worker.interfaces.publisher_protocol import MessagePublisherProtocol
from worker.logger import get_logger
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionEvent, PositionEventType

log = get_logger("worker.jobs.position_cdc_job")

_POLL_INTERVAL = 2  # seconds

# Position-row columns that map 1-to-1 to PositionEvent fields.
_EVENT_FIELDS = {
  "id",
  "ref_source_id",
  "ref_id",
  "strategy",
  "symbol",
  "action",
  "volume",
  "opened_price",
  "closed_price",
  "status",
  "gateway_return_code",
  "comment",
  "message",
  "strategy_code",
  "created_at",
  "updated_at",
  "sync_status",
  "sync_time",
}


class PositionCDC:
  """Daemon thread that implements Change Data Capture on the SQLite positions table.

  Polls for rows with sync_status=PENDING, publishes a PositionEvent to the
  NATS TRADE subject, then marks the row as PUBLISHED. Provides at-least-once
  delivery semantics; the broker handler is expected to be idempotent.
  """

  def __init__(
    self,
    account_id: str,
    publisher: MessagePublisherProtocol,
    db_service: PositionSyncStoreProtocol,
    account_info_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
    poll_interval: int = _POLL_INTERVAL,
    account_name: Optional[str] = None,
    market_type: Optional[str] = None,
    gateway: Optional[str] = None,
    strategy_magic_map: Optional[Dict[str, int]] = None,
  ) -> None:
    self._account_id = account_id
    self._account_name = account_name
    self._market_type = market_type
    self._gateway = gateway or ""
    self._publisher = publisher
    self._db = db_service
    self._account_info_fn = account_info_fn
    self._poll_interval = poll_interval
    self._strategy_magic_map: Dict[str, int] = strategy_magic_map or {}
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  def _magic_for(self, strategy: Optional[str]) -> Optional[int]:
    if not strategy:
      return None
    return self._strategy_magic_map.get(strategy)


  def start(self, stop_event=None) -> None:
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run,
      name="position-cdc",
      daemon=True,
    )
    self._thread.start()
    log.info("[PositionCDC] Started (poll_interval=%ds)", self._poll_interval)

  def stop(self) -> None:
    self._stop_event.set()

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        self._poll()
      except Exception as exc:
        log.exception("[PositionCDC] Unexpected error during poll: %s", exc)
      self._stop_event.wait(self._poll_interval)

  def _poll(self) -> None:
    rows = self._db.get_pending_sync_positions()
    if not rows:
      return
    account_snapshot = self._snapshot_account()
    for row in rows:
      event_type = (
        PositionEventType.CREATED
        if row.get("sync_time") is None
        else PositionEventType.UPDATED
      )
      payload = {k: row[k] for k in _EVENT_FIELDS if k in row}
      # DB stores the raw signal JSON in "gateway_message"; PositionEvent calls it "message".
      gateway_msg = row.get("gateway_message")
      if gateway_msg is not None:
        payload["message"] = gateway_msg
      payload.update(self._extract_signal_fields(gateway_msg))
      strategy_code = payload.get("strategy_code")
      if strategy_code is None:
        magic = self._magic_for(row.get("strategy"))
        payload["strategy_code"] = str(magic) if magic is not None else None
      else:
        payload["strategy_code"] = str(strategy_code)
      payload.update(account_snapshot)
      event = PositionEvent(
        event=event_type,
        account_id=self._account_id,
        account_name=self._account_name,
        market=self._market_type,
        gateway=self._gateway,
        **payload,
      )
      event_json = event.model_dump_json()
      log.info(
        "[PositionCDC] Publishing TRADE event | event=%s status=%s source_ticket=%s\n%s",
        event_type.value,
        row.get("status"),
        row.get("ref_source_id"),
        event_json,
      )
      self._publisher.publish(NatsSubjectEnum.TRADE, event_json)
      # Publish-then-mark gives at-least-once delivery; the broker handler is
      # idempotent (upsert by market + gateway + account_id + ticket).
      marked = self._db.mark_position_synced(row["id"], row["updated_at"])
      if not marked:
        log.debug(
          "[PositionCDC] Row id=%s was modified concurrently; left PENDING.",
          row["id"],
        )

  def _snapshot_account(self) -> Dict[str, Any]:
    if self._account_info_fn is None:
      return {}
    try:
      info = self._account_info_fn()
    except Exception as exc:
      log.warning("[PositionCDC] account_info_fn failed: %s", exc)
      return {}
    if not info:
      return {}
    snapshot: Dict[str, Any] = {}
    if info.get("leverage") is not None:
      snapshot["account_leverage"] = int(info["leverage"])
    if info.get("balance") is not None:
      snapshot["account_balance"] = float(info["balance"])
    return snapshot

  @staticmethod
  def _extract_signal_fields(message: Optional[str]) -> Dict[str, Any]:
    """Parse the original signal JSON stored in positions.message and pull out
    the fields the broker needs to create a Trade row."""
    if not message:
      return {}
    try:
      data = json.loads(message)
    except (json.JSONDecodeError, TypeError):
      return {}
    extracted: Dict[str, Any] = {}
    for key in ("signal_id", "sl", "tp1", "tp2"):
      if data.get(key) is not None:
        extracted[key] = data[key]
    if data.get("risk_percent") is not None:
      extracted["risk_percent"] = data["risk_percent"]
    return extracted
