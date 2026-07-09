"""
tests/jobs/test_cdc_job.py
──────────────────────────
Covers the Change-Data-Capture → NATS TRADE publisher (PositionCDC), focusing on
the payload published to the TRADE subject: it must carry market_type, account_id
*and* the gateway so the broker can persist the full worker identity
"<market>-<gateway>-<account_id>".
"""

import json

from worker.jobs.cdc_job import PositionCDC
from worker.schemas.nats_schema import NatsSubjectEnum


class _FakePublisher:
  def __init__(self):
    self.published: list[tuple] = []

  def publish(self, subject, data):
    self.published.append((subject, data))


class _FakeDb:
  def __init__(self, rows):
    self._rows = rows
    self.synced: list = []

  def get_pending_sync_positions(self):
    # Return the queued rows once, then nothing (single poll under test).
    rows, self._rows = self._rows, []
    return rows

  def mark_position_synced(self, row_id, updated_at):
    self.synced.append((row_id, updated_at))
    return True


def _row(**overrides):
  row = {
    "id": 1,
    "ref_source_id": "111",
    "ref_id": "111",
    "strategy": "strat-1",
    "symbol": "XAUUSD",
    "action": "long",
    "volume": 1.0,
    "opened_price": 2000.0,
    "closed_price": None,
    "status": "OPENED",
    "gateway_return_code": 0,
    "comment": "",
    "gateway_message": None,
    "strategy_code": "42",
    "created_at": "2026-07-09T00:00:00",
    "updated_at": "2026-07-09T00:00:00",
    "sync_status": "PENDING",
    "sync_time": None,
  }
  row.update(overrides)
  return row


def test_trade_event_includes_gateway_market_and_account():
  publisher = _FakePublisher()
  db = _FakeDb([_row()])
  cdc = PositionCDC(
    account_id="1000",
    publisher=publisher,
    db_service=db,
    market_type="FOREX",
    gateway="MT5",
  )

  cdc._poll()

  assert len(publisher.published) == 1
  subject, raw = publisher.published[0]
  assert subject == NatsSubjectEnum.TRADE
  payload = json.loads(raw)
  # The three identity components the broker needs to rebuild the worker
  # identity "<market>-<gateway>-<account_id>".
  assert payload["market_type"] == "FOREX"
  assert payload["gateway"] == "MT5"
  assert payload["account_id"] == "1000"


def test_trade_event_gateway_defaults_to_none_when_unset():
  publisher = _FakePublisher()
  db = _FakeDb([_row()])
  cdc = PositionCDC(
    account_id="1000",
    publisher=publisher,
    db_service=db,
    market_type="FOREX",
  )

  cdc._poll()

  _, raw = publisher.published[0]
  assert json.loads(raw)["gateway"] is None
