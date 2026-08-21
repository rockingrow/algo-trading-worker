"""
Tests for worker/schemas/cycle_schema.py — the status-folding rules that decide
what headline a signal cycle advertises.
"""

from datetime import datetime, timezone

from worker.schemas.cycle_schema import (
  CycleEventSchema,
  CycleOutcomeEnum,
  CycleStatusEnum,
  merge_status,
)
from worker.schemas.position_schema import PositionStatusEnum

# ── Status conversion ─────────────────────────────────────────────────────── #


def test_every_position_status_converts_to_a_cycle_status():
  for status in PositionStatusEnum:
    assert CycleStatusEnum.from_position_status(status) is not None


def test_from_position_status_accepts_bare_strings_and_none():
  assert CycleStatusEnum.from_position_status("TP1") is CycleStatusEnum.TP1
  assert CycleStatusEnum.from_position_status(None) is None
  assert CycleStatusEnum.from_position_status("NOT_A_STATUS") is None


# ── merge_status ──────────────────────────────────────────────────────────── #


def test_first_status_is_taken_as_is():
  assert merge_status(None, CycleStatusEnum.OPENED) is CycleStatusEnum.OPENED


def test_action_without_a_status_leaves_the_headline_alone():
  assert merge_status(CycleStatusEnum.OPENED, None) is CycleStatusEnum.OPENED


def test_open_position_advances_through_tp1():
  assert (
    merge_status(CycleStatusEnum.OPENED, CycleStatusEnum.TP1) is CycleStatusEnum.TP1
  )


def test_closed_is_terminal_against_a_late_tp1():
  # A TP1 redelivered after the SL must not advertise the trade as live again.
  assert merge_status(CycleStatusEnum.SL, CycleStatusEnum.TP1) is CycleStatusEnum.SL


def test_every_terminal_status_is_sticky():
  for terminal in (
    CycleStatusEnum.TP2,
    CycleStatusEnum.SL,
    CycleStatusEnum.R_SL,
    CycleStatusEnum.FLATTED,
    CycleStatusEnum.FORCED_CLOSED,
    CycleStatusEnum.TERMINAL_CLOSED,
  ):
    assert merge_status(terminal, CycleStatusEnum.OPENED) is terminal


def test_failed_action_does_not_close_an_open_position():
  # The broker refused a TP1; the position is untouched and still open.
  assert (
    merge_status(CycleStatusEnum.OPENED, CycleStatusEnum.FAILED)
    is CycleStatusEnum.OPENED
  )


def test_rejected_action_does_not_close_an_open_position():
  assert (
    merge_status(CycleStatusEnum.TP1, CycleStatusEnum.REJECTED) is CycleStatusEnum.TP1
  )


def test_failure_stands_when_no_position_was_ever_opened():
  assert merge_status(None, CycleStatusEnum.FAILED) is CycleStatusEnum.FAILED
  assert (
    merge_status(CycleStatusEnum.REJECTED, CycleStatusEnum.FAILED)
    is CycleStatusEnum.FAILED
  )


def test_a_real_status_replaces_an_earlier_failure():
  # The entry failed, a retry filled: the cycle now tracks a real position.
  assert (
    merge_status(CycleStatusEnum.FAILED, CycleStatusEnum.OPENED)
    is CycleStatusEnum.OPENED
  )


# ── Event serialisation ───────────────────────────────────────────────────── #


def test_to_event_renders_a_json_ready_dict():
  event = CycleEventSchema(
    action="LONG",
    outcome=CycleOutcomeEnum.FILLED,
    timestamp=datetime(2026, 6, 2, 8, 0, tzinfo=timezone.utc),
    price=2340.0,
  )
  data = event.to_event()
  assert data["action"] == "LONG"
  # use_enum_values keeps the outcome a plain string, and the timestamp is ISO
  # text — both so the dict survives a json.dumps round-trip into SQLite.
  assert data["outcome"] == "FILLED"
  assert data["timestamp"] == "2026-06-02T08:00:00+00:00"
