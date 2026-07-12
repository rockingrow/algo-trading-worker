"""Unit tests for ManualOrderSyncJob — adoption of hand-opened MT5 positions."""

from typing import List

from helpers import make_platform_position

from worker.gateways.forex.base import SIDE_SHORT
from worker.jobs.manual_order_sync_job import ManualOrderSyncJob
from worker.schemas.job_schema import LogAuthorEnum


class _FakeExecutor:
  """Minimal executor exposing what the job consumes."""

  def __init__(self, manual_positions):
    self._manual = manual_positions

  def get_manual_open_positions(self):
    return list(self._manual)

  def base_symbol(self, resolved_symbol):
    # Simulate a suffixed broker: strip a trailing 'c'.
    return resolved_symbol[:-1] if resolved_symbol.endswith("c") else resolved_symbol


class _FakeDB:
  """Records inserts/logs; get_position/by_strategy are configurable."""

  def __init__(self, existing_tickets=None, existing_by_strategy=None):
    self._existing = set(existing_tickets or ())
    self._by_strategy = existing_by_strategy or {}
    self.inserted: List[dict] = []
    self.logged: List[dict] = []

  def get_position(self, ref_source_id):
    return {"ref_source_id": ref_source_id} if ref_source_id in self._existing else None

  def get_open_positions_by_strategy(self, strategy, symbol):
    return self._by_strategy.get((strategy, symbol), [])

  def log_position(self, **kwargs):
    self.logged.append(kwargs)

  def insert_position(self, **kwargs):
    self.inserted.append(kwargs)
    # Emulate the store now tracking it.
    self._existing.add(kwargs["ref_id"])
    self._by_strategy.setdefault((kwargs["strategy"], kwargs["symbol"]), []).append(
      {"ref_source_id": kwargs["ref_id"]}
    )


class _FakeNotifier:
  def __init__(self):
    self.messages: List[str] = []

  def send_message(self, msg):
    self.messages.append(msg)


def _job(executor, db, notifier=None, manual_strategy="MANUAL"):
  return ManualOrderSyncJob(
    executor=executor,
    db_service=db,
    notifier=notifier or _FakeNotifier(),
    manual_strategy=manual_strategy,
  )


def test_adopts_untracked_manual_position():
  pos = make_platform_position(ticket=555, magic=0, symbol="XAUUSDc", volume=0.2)
  db = _FakeDB()
  notifier = _FakeNotifier()
  _job(_FakeExecutor([pos]), db, notifier)._scan()

  assert len(db.inserted) == 1
  row = db.inserted[0]
  assert row["ref_id"] == "555"
  assert row["strategy"] == "MANUAL"
  assert row["symbol"] == "XAUUSD"  # base symbol, not the suffixed XAUUSDc
  assert row["action"] == "long"
  assert row["strategy_code"] == 0
  # Audit log written with author='manual'.
  assert db.logged[0]["author"] == LogAuthorEnum.MANUAL.value
  # Operator notified.
  assert len(notifier.messages) == 1


def test_short_side_recorded():
  pos = make_platform_position(ticket=7, magic=0, side=SIDE_SHORT, symbol="EURUSD")
  db = _FakeDB()
  _job(_FakeExecutor([pos]), db)._scan()
  assert db.inserted[0]["action"] == "short"


def test_skips_already_tracked_ticket():
  pos = make_platform_position(ticket=555, magic=0, symbol="XAUUSDc")
  db = _FakeDB(existing_tickets={"555"})
  _job(_FakeExecutor([pos]), db)._scan()
  assert db.inserted == []


def test_skips_when_symbol_slot_already_taken():
  """One-active-per-(strategy, symbol): don't collide with the unique index."""
  pos = make_platform_position(ticket=999, magic=0, symbol="XAUUSDc")
  db = _FakeDB(existing_by_strategy={("MANUAL", "XAUUSD"): [{"ref_source_id": "111"}]})
  _job(_FakeExecutor([pos]), db)._scan()
  assert db.inserted == []


def test_idempotent_across_scans():
  pos = make_platform_position(ticket=42, magic=0, symbol="XAUUSDc")
  db = _FakeDB()
  job = _job(_FakeExecutor([pos]), db)
  job._scan()
  job._scan()  # second scan sees it already tracked
  assert len(db.inserted) == 1


def test_one_bad_position_does_not_abort_scan():
  good = make_platform_position(ticket=1, magic=0, symbol="EURUSD")

  class _Boom:
    ticket = 2
    # Accessing .symbol raises to simulate a malformed position.

    @property
    def symbol(self):
      raise RuntimeError("boom")

  db = _FakeDB()
  _job(_FakeExecutor([_Boom(), good]), db)._scan()
  # The good one is still adopted despite the bad one raising.
  assert [r["ref_id"] for r in db.inserted] == ["1"]
