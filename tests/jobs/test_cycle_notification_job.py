"""
Tests for CycleNotificationJob — the dispatcher that keeps each chat's message
in step with its cycle.

Unlike the outbox dispatcher it never deletes anything: the first pass posts,
every later pass edits. The interesting cases are the recoveries — an edit whose
message is gone, and an edit that merely failed.
"""

from worker.jobs.cycle_notification_job import CycleNotificationJob
from worker.services.notification_service import EditOutcome


class FakeSender:
  """Stand-in for TelegramCycleNotification with scriptable outcomes."""

  def __init__(self, send_ids=None, edit_outcomes=None):
    self._send_ids = list(send_ids) if send_ids is not None else None
    self._edit_outcomes = list(edit_outcomes or [])
    self.sent = []
    self.edited = []

  def send(self, chat_id, message_text):
    self.sent.append((chat_id, message_text))
    if self._send_ids is None:
      return "555"
    return self._send_ids.pop(0) if self._send_ids else None

  def edit(self, chat_id, message_id, message_text):
    self.edited.append((chat_id, message_id, message_text))
    if not self._edit_outcomes:
      return EditOutcome.OK
    return self._edit_outcomes.pop(0)


class FakeStore:
  def __init__(self, rows):
    self._rows = rows
    self.delivered = []
    self.failed = []
    self.cleared = []

  def get_due_cycle_deliveries(self, limit=20):
    rows, self._rows = self._rows, []
    return rows

  def mark_cycle_delivered(self, chat_row_id, message_id, revision):
    self.delivered.append((chat_row_id, message_id, revision))

  def mark_cycle_delivery_failed(self, chat_row_id, error, next_attempt_at):
    self.failed.append((chat_row_id, error, next_attempt_at))

  def clear_cycle_message_id(self, chat_row_id):
    self.cleared.append(chat_row_id)


def _row(**overrides):
  row = {
    "chat_row_id": 1,
    "chat_id": "-1001",
    "message_id": None,
    "attempts": 0,
    "cycle_id": 7,
    "revision": 3,
    "message_text": "<pre>body</pre>",
    "signal_uxid": "9f2c4b7e18a3d605",
    "strategy": "strat-a",
  }
  row.update(overrides)
  return row


def _run(rows, sender):
  store = FakeStore(rows)
  CycleNotificationJob(db_service=store, sender=sender)._poll()
  return store


# ── First delivery ────────────────────────────────────────────────────────── #


def test_a_cycle_without_a_message_is_posted_and_its_id_stored():
  sender = FakeSender(send_ids=["999"])
  store = _run([_row()], sender)
  assert sender.sent == [("-1001", "<pre>body</pre>")]
  assert sender.edited == []
  assert store.delivered == [(1, "999", 3)]


def test_a_send_that_returns_no_id_is_retried_rather_than_marked_delivered():
  # Without an id there is nothing to edit later, so it cannot count as sent.
  sender = FakeSender(send_ids=[None])
  store = _run([_row()], sender)
  assert store.delivered == []
  assert len(store.failed) == 1


# ── Later deliveries ──────────────────────────────────────────────────────── #


def test_a_cycle_with_a_message_is_edited_in_place():
  sender = FakeSender()
  store = _run([_row(message_id="555")], sender)
  assert sender.edited == [("-1001", "555", "<pre>body</pre>")]
  assert sender.sent == []
  # message_id is None so the stored id stands — an edit never changes it.
  assert store.delivered == [(1, None, 3)]


def test_a_deleted_message_is_forgotten_so_the_next_pass_re_posts():
  sender = FakeSender(edit_outcomes=[EditOutcome.MISSING])
  store = _run([_row(message_id="555")], sender)
  assert store.cleared == [1]
  assert store.delivered == []
  # Not counted as a failure: the row stays due and recovers by re-sending.
  assert store.failed == []


def test_a_transient_edit_failure_keeps_the_message_id_and_backs_off():
  sender = FakeSender(edit_outcomes=[EditOutcome.FAILED])
  store = _run([_row(message_id="555", attempts=2)], sender)
  assert store.cleared == []
  assert store.delivered == []
  chat_row_id, error, next_attempt_at = store.failed[0]
  assert chat_row_id == 1
  assert "attempt 3" in error
  assert next_attempt_at  # a concrete timestamp, so the row is skipped until then


# ── Batches ───────────────────────────────────────────────────────────────── #


def test_each_chat_of_one_cycle_is_delivered_independently():
  sender = FakeSender(send_ids=["901", "902"])
  store = _run(
    [_row(chat_row_id=1, chat_id="-1001"), _row(chat_row_id=2, chat_id="-1002")],
    sender,
  )
  assert [chat for chat, _ in sender.sent] == ["-1001", "-1002"]
  assert store.delivered == [(1, "901", 3), (2, "902", 3)]


def test_one_chat_failing_does_not_stop_the_others():
  sender = FakeSender(send_ids=[None, "902"])
  store = _run(
    [_row(chat_row_id=1, chat_id="-1001"), _row(chat_row_id=2, chat_id="-1002")],
    sender,
  )
  assert store.failed[0][0] == 1
  assert store.delivered == [(2, "902", 3)]
