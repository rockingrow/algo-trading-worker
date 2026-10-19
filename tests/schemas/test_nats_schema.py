"""Tests for NatsSubjectEnum's wire-name formatting.

nats-py builds its SUB/PUB protocol lines with f-strings, so how a member
formats *is* the subject the server sees. On Python 3.11+ a plain
``str``-mix-in enum formats as ``NatsSubjectEnum.SYSTEM``, which the server
accepts as a perfectly valid subject — the worker goes deaf to SYSTEM and ADMIN
without a single error anywhere. These assertions are that silence's alarm.
"""

from worker.schemas.nats_schema import NatsSubjectEnum


def test_f_string_yields_the_wire_name():
  # Exactly what nats.protocol.command.sub_cmd does with the subject.
  assert f"{NatsSubjectEnum.SYSTEM}" == "SYSTEM"
  assert f"SUB {NatsSubjectEnum.ADMIN}  1" == "SUB ADMIN  1"


def test_str_and_format_yield_the_wire_name_for_every_member():
  for subject in NatsSubjectEnum:
    assert str(subject) == subject.value
    assert format(subject) == subject.value
    assert f"{subject}" == subject.value
    assert "%s" % subject == subject.value


def test_members_still_compare_and_encode_as_their_value():
  assert NatsSubjectEnum.TRADE == "TRADE"
  assert NatsSubjectEnum.TRADE.encode() == b"TRADE"
  assert NatsSubjectEnum("SYSTEM") is NatsSubjectEnum.SYSTEM
