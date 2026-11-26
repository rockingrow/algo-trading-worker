"""
Tests for PositionCDC delivery: a row is marked PUBLISHED only after the NATS
server confirmed the event, and only at the ``sync_version`` that was published.
"""

from worker.jobs.cdc_job import PositionCDC
from worker.schemas.nats_schema import NatsSubjectEnum


def _row(row_id, status="OPENED", sync_version=0):
  return {
    "id": row_id,
    "ref_source_id": str(100 + row_id),
    "ref_id": str(100 + row_id),
    "strategy": "strat-a",
    "symbol": "XAUUSD",
    "action": "long",
    "volume": 0.5,
    "opened_price": 4500.0,
    "status": status,
    "sync_status": "PENDING",
    "sync_time": None,
    "sync_version": sync_version,
    "created_at": "2026-11-24 10:00:00",
    "updated_at": "2026-11-24 10:00:00",
    "gateway_message": None,
  }


class _Store:
  def __init__(self, rows):
    self.rows = rows
    self.marked = []

  def get_pending_sync_positions(self):
    return list(self.rows)

  def mark_position_synced(self, position_id, sync_version):
    self.marked.append((position_id, sync_version))
    return True


class _Publisher:
  """Confirms every publish, or fails from the *fail_from*-th one on."""

  def __init__(self, fail_from=None):
    self.fail_from = fail_from
    self.published = []

  def publish(self, subject, data):  # pragma: no cover - CDC must not use it
    raise AssertionError("PositionCDC must wait for a confirmed publish")

  def publish_confirmed(self, subject, data, timeout=5.0):
    if self.fail_from is not None and len(self.published) >= self.fail_from:
      raise TimeoutError("nats: flush timeout")
    self.published.append((subject, data))


def _cdc(store, publisher):
  return PositionCDC(
    account_id="ACC", publisher=publisher, db_service=store, market_type="FOREX"
  )


def test_rows_are_marked_at_the_published_version():
  store = _Store([_row(1, sync_version=3), _row(2)])
  publisher = _Publisher()

  _cdc(store, publisher)._poll()

  assert [s for s, _ in publisher.published] == [NatsSubjectEnum.TRADE] * 2
  assert store.marked == [(1, 3), (2, 0)]


def test_an_unconfirmed_publish_leaves_the_row_and_later_ones_pending():
  """The event was only queued in memory before; marking it then lost it when
  the send failed or the process stopped. Later rows wait too, so a position's
  events are never delivered out of order."""
  store = _Store([_row(1), _row(2, status="TP1"), _row(3)])
  publisher = _Publisher(fail_from=1)

  _cdc(store, publisher)._poll()

  assert len(publisher.published) == 1
  assert store.marked == [(1, 0)]


def test_publishing_resumes_once_nats_confirms_again():
  store = _Store([_row(1)])
  publisher = _Publisher(fail_from=0)
  cdc = _cdc(store, publisher)

  cdc._poll()
  assert store.marked == []

  publisher.fail_from = None
  cdc._poll()
  assert store.marked == [(1, 0)]
