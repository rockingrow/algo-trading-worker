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

from worker.logger import get_logger
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.trade_event_schema import PositionEvent, PositionEventType
from worker.services.db_service import DBService
from worker.services.nats_service import NATSPublisher

log = get_logger("worker.jobs.position_cdc_job")

_POLL_INTERVAL = 2  # seconds

# Position-row columns that map 1-to-1 to PositionEvent fields.
_EVENT_FIELDS = {
  "id",
  "source_ticket",
  "ticket",
  "strategy",
  "symbol",
  "action",
  "volume",
  "opened_price",
  "closed_price",
  "status",
  "mt5_retcode",
  "comment",
  "message",
  "created_at",
  "updated_at",
  "sync_status",
  "sync_time",
}


class PositionCDC:
  def __init__(
    self,
    account_id: str,
    publisher: NATSPublisher,
    db_service: DBService,
    account_info_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
    poll_interval: int = _POLL_INTERVAL,
    account_name: Optional[str] = None,
    market_type: Optional[str] = None,
  ) -> None:
    self._account_id = account_id
    self._account_name = account_name
    self._market_type = market_type
    self._publisher = publisher
    self._db = db_service
    self._account_info_fn = account_info_fn
    self._poll_interval = poll_interval
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

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
      payload.update(self._extract_signal_fields(row.get("message")))
      payload.update(account_snapshot)
      event = PositionEvent(
        event=event_type,
        account_id=self._account_id,
        account_name=self._account_name,
        market_type=self._market_type,
        **payload,
      )
      event_json = event.model_dump_json()
      log.info(
        "[PositionCDC] Publishing TRADE event | event=%s status=%s source_ticket=%s\n%s",
        event_type.value,
        row.get("status"),
        row.get("source_ticket"),
        event_json,
      )
      self._publisher.publish(NatsSubjectEnum.TRADE, event_json)
      # Publish-then-mark gives at-least-once delivery; the broker handler is
      # idempotent (upsert by account_id + ticket).
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
    action = data.get("action")
    signal_id = data.get("signal_id")
    if action and signal_id:
      extracted["magic"] = f"{action}|{signal_id}"
    return extracted
