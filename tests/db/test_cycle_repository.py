"""
Tests for NotificationCycleRepository — the store behind the one-message-per
-trade notification.

Two things it must never get wrong: an action may not be lost when it lands on
an existing cycle, and a chat may not be told it has seen a body newer than the
one it actually holds.
"""

import pytest

from worker.db.repository import NotificationCycleRepository
from worker.db.schema import db_init
from worker.schemas.cycle_schema import CycleStatusEnum
from worker.settings import settings

UXID = "9f2c4b7e18a3d605"


@pytest.fixture
def repo(tmp_path, monkeypatch):
  monkeypatch.setattr(settings.database, "file", str(tmp_path / "cycle_test.sqlite"))
  db_init()
  return NotificationCycleRepository()


def _append(repo, action="LONG", status=CycleStatusEnum.OPENED, uxid=UXID, **overrides):
  payload = {"action": action, "outcome": "FILLED"}
  payload.update(overrides)
  return repo.append_event(
    strategy="strat-a",
    signal_uxid=uxid,
    symbol="XAUUSD",
    event=payload,
    status=status,
  )


# ── append_event ──────────────────────────────────────────────────────────── #


def test_first_event_creates_the_cycle_at_revision_one(repo):
  cycle = _append(repo)
  assert cycle["revision"] == 1
  assert cycle["status"] == "OPENED"
  assert [e["action"] for e in cycle["events"]] == ["LONG"]


def test_second_event_appends_to_the_same_cycle(repo):
  _append(repo)
  cycle = _append(repo, action="TP1", status=CycleStatusEnum.TP1)
  assert cycle["revision"] == 2
  assert [e["action"] for e in cycle["events"]] == ["LONG", "TP1"]
  assert cycle["status"] == "TP1"


def test_a_different_uxid_is_a_different_cycle(repo):
  first = _append(repo)
  second = _append(repo, uxid="0123456789abcdef")
  assert first["id"] != second["id"]
  assert second["revision"] == 1


def test_status_merge_rules_apply_across_writes(repo):
  _append(repo)
  _append(repo, action="SL", status=CycleStatusEnum.SL)
  # A TP1 redelivered after the SL must not reopen the trade.
  cycle = _append(repo, action="TP1", status=CycleStatusEnum.TP1)
  assert cycle["status"] == "SL"
  assert len(cycle["events"]) == 3  # ...but the action is still recorded


def test_event_without_a_status_keeps_the_previous_headline(repo):
  _append(repo)
  cycle = _append(repo, action="TP1", status=None)
  assert cycle["status"] == "OPENED"


def test_corrupt_events_json_restarts_the_timeline_instead_of_raising(repo, tmp_path):
  import sqlite3

  cycle = _append(repo)
  conn = sqlite3.connect(settings.database.file)
  conn.execute(
    "UPDATE notification_cycles SET events = ? WHERE id = ?", ("not json", cycle["id"])
  )
  conn.commit()
  conn.close()

  recovered = _append(repo, action="TP1", status=CycleStatusEnum.TP1)
  assert [e["action"] for e in recovered["events"]] == ["TP1"]


# ── Chats + delivery bookkeeping ──────────────────────────────────────────── #


def test_ensure_cycle_chats_is_idempotent(repo):
  cycle = _append(repo)
  repo.ensure_cycle_chats(cycle["id"], ["-1001", "-1002"])
  repo.ensure_cycle_chats(cycle["id"], ["-1001", "-1002"])
  repo.set_message_text(cycle["id"], "body", cycle["revision"])
  assert len(repo.get_due_cycle_deliveries()) == 2


def test_a_cycle_without_a_rendered_body_is_not_due(repo):
  cycle = _append(repo)
  repo.ensure_cycle_chats(cycle["id"], ["-1001"])
  assert repo.get_due_cycle_deliveries() == []


def test_delivery_row_carries_the_body_and_revision(repo):
  cycle = _append(repo)
  repo.set_message_text(cycle["id"], "the body", cycle["revision"])
  repo.ensure_cycle_chats(cycle["id"], ["-1001"])
  (row,) = repo.get_due_cycle_deliveries()
  assert row["message_text"] == "the body"
  assert row["revision"] == 1
  assert row["message_id"] is None
  assert row["signal_uxid"] == UXID


def test_marking_delivered_clears_the_row_until_the_next_action(repo):
  cycle = _append(repo)
  repo.set_message_text(cycle["id"], "body 1", cycle["revision"])
  repo.ensure_cycle_chats(cycle["id"], ["-1001"])
  (row,) = repo.get_due_cycle_deliveries()
  repo.mark_cycle_delivered(row["chat_row_id"], "555", row["revision"])
  assert repo.get_due_cycle_deliveries() == []

  updated = _append(repo, action="TP1", status=CycleStatusEnum.TP1)
  repo.set_message_text(updated["id"], "body 2", updated["revision"])
  (row,) = repo.get_due_cycle_deliveries()
  # The chat keeps its message id, so the next pass edits rather than re-posts.
  assert row["message_id"] == "555"
  assert row["message_text"] == "body 2"


def test_delivered_revision_never_moves_backwards(repo):
  cycle = _append(repo)
  repo.set_message_text(cycle["id"], "body", cycle["revision"])
  repo.ensure_cycle_chats(cycle["id"], ["-1001"])
  (row,) = repo.get_due_cycle_deliveries()
  repo.mark_cycle_delivered(row["chat_row_id"], "555", 5)
  # A slower delivery of an older body finishing last must not undo revision 5.
  repo.mark_cycle_delivered(row["chat_row_id"], "555", 2)
  _append(repo, action="TP1", status=CycleStatusEnum.TP1)
  assert repo.get_due_cycle_deliveries() == []


def test_set_message_text_ignores_a_render_older_than_the_cycle(repo):
  cycle = _append(repo)
  _append(repo, action="TP1", status=CycleStatusEnum.TP1)  # now at revision 2
  repo.set_message_text(cycle["id"], "stale body", 1)
  repo.ensure_cycle_chats(cycle["id"], ["-1001"])
  assert repo.get_due_cycle_deliveries() == []


def test_failed_delivery_backs_off_then_gives_up(repo):
  cycle = _append(repo)
  repo.set_message_text(cycle["id"], "body", cycle["revision"])
  repo.ensure_cycle_chats(cycle["id"], ["-1001"])
  (row,) = repo.get_due_cycle_deliveries()

  # A future next_attempt_at takes the row out of the poll.
  repo.mark_cycle_delivery_failed(row["chat_row_id"], "boom", "2999-01-01 00:00:00")
  assert repo.get_due_cycle_deliveries() == []

  # Past its backoff it is due again, until max_attempts is spent.
  for _ in range(5):
    repo.mark_cycle_delivery_failed(row["chat_row_id"], "boom", "2000-01-01 00:00:00")
  assert repo.get_due_cycle_deliveries() == []


def test_clearing_the_message_id_makes_the_row_send_again(repo):
  cycle = _append(repo)
  repo.set_message_text(cycle["id"], "body", cycle["revision"])
  repo.ensure_cycle_chats(cycle["id"], ["-1001"])
  (row,) = repo.get_due_cycle_deliveries()
  repo.clear_cycle_message_id(row["chat_row_id"])
  (row,) = repo.get_due_cycle_deliveries()
  assert row["message_id"] is None
