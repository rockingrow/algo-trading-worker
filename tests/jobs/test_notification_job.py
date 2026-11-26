"""
Tests for the outbox dispatcher's claim-before-send delivery.

A send is counted before it happens, not after it fails. The job used to send
and then delete, so a crash in between re-sent the message on the next poll —
and kept re-sending it, because ``attempts`` counted only failures and never
bounded sends.
"""

import sqlite3

import pytest

from worker.db.repository import NotificationOutboxRepository
from worker.db.schema import db_init
from worker.jobs.notification_job import NotificationJob
from worker.schemas.notification_schema import (
  NotificationChannelEnum,
  NotificationPlatformEnum,
)
from worker.settings import settings


@pytest.fixture
def outbox(tmp_path, monkeypatch):
  monkeypatch.setattr(settings.database, "file", str(tmp_path / "outbox.sqlite"))
  db_init()
  return NotificationOutboxRepository()


def _enqueue(outbox, text="hello", max_attempts=5):
  outbox.enqueue_notification(
    platform=NotificationPlatformEnum.TELEGRAM,
    channel=NotificationChannelEnum.INDIVIDUAL,
    message_text=text,
    max_attempts=max_attempts,
  )
  return outbox.get_due_notifications()[0]


def _rows():
  conn = sqlite3.connect(settings.database.file)
  conn.row_factory = sqlite3.Row
  rows = [dict(r) for r in conn.execute("SELECT * FROM notifications")]
  conn.close()
  return rows


class _Notifier:
  """Records sends; *succeed* mirrors what the real sender returns."""

  def __init__(self, succeed=True):
    self.succeed = succeed
    self.sent = []

  def send_message(self, text):
    self.sent.append(text)
    return self.succeed


def _job(outbox, notifier):
  return NotificationJob(
    db_service=outbox,
    notifiers={NotificationChannelEnum.INDIVIDUAL.value: notifier},
  )


# ── The claim itself ──────────────────────────────────────────────────────── #


def test_a_claim_is_a_compare_and_set_on_the_attempt_count(outbox):
  row = _enqueue(outbox)

  assert outbox.claim_notification(row["id"], row["attempts"], "2099-01-01 00:00:00")
  # The same claim again loses: attempts has moved on.
  assert not outbox.claim_notification(
    row["id"], row["attempts"], "2099-01-01 00:00:00"
  )
  assert _rows()[0]["attempts"] == 1


def test_a_lost_claim_sends_nothing(outbox):
  row = _enqueue(outbox)
  notifier = _Notifier()
  # Something else already moved the row on since the poll read it.
  outbox.claim_notification(row["id"], row["attempts"], "2099-01-01 00:00:00")

  _job(outbox, notifier)._poll()

  assert notifier.sent == []


# ── The dispatcher ───────────────────────────────────────────────────────── #


def test_the_send_is_counted_before_it_happens(outbox):
  """What the row says while the send is in flight is the whole point: a crash
  there must leave an attempt charged and a backoff set, not a row that looks
  untouched."""
  seen = []
  _enqueue(outbox)
  notifier = _Notifier()
  notifier.send_message = lambda text: (seen.append(_rows()[0]["attempts"]), True)[1]

  _job(outbox, notifier)._poll()

  assert seen == [1]  # already counted when the send ran
  assert _rows() == []  # and deleted once it succeeded


def test_a_successful_send_deletes_the_row(outbox):
  _enqueue(outbox, text="done")
  notifier = _Notifier()

  _job(outbox, notifier)._poll()

  assert notifier.sent == ["done"]
  assert _rows() == []


def test_a_failed_send_records_the_error_without_double_counting(outbox):
  """The claim already charged the attempt; charging it again in the failure
  path would halve max_attempts."""
  _enqueue(outbox)
  notifier = _Notifier(succeed=False)

  _job(outbox, notifier)._poll()

  row = _rows()[0]
  assert row["attempts"] == 1
  assert "Send failed" in row["last_error"]


def test_max_attempts_bounds_sends_not_just_failures(outbox):
  """A row that keeps being claimed and never deleted (the crash-mid-send loop)
  drops out of the poll once it has had its allowance of *sends*."""
  outbox.enqueue_notification(
    platform=NotificationPlatformEnum.TELEGRAM,
    channel=NotificationChannelEnum.INDIVIDUAL,
    message_text="x",
    max_attempts=2,
  )
  notifier = _Notifier()
  job = _job(outbox, notifier)

  for _ in range(4):
    # Clear the backoff so the row is due again, as a later poll would find it.
    conn = sqlite3.connect(settings.database.file)
    conn.execute("UPDATE notifications SET next_attempt_at = NULL")
    conn.commit()
    conn.close()
    # Model the crash: the send happens, the delete never does.
    notifier.send_message = lambda text: (notifier.sent.append(text), False)[1]
    job._poll()

  assert len(notifier.sent) == 2
  assert _rows()[0]["attempts"] == 2  # dead-lettered, left in place
