"""
worker/schemas/nats_schema.py
─────────────────────────────
The NATS subject names this worker subscribes to and publishes on.

``NatsSubjectEnum`` mixes in ``str`` so a member can be handed to anything that
expects a subject name. That mix-in alone is not enough on Python 3.11+, where
``Enum`` overrides ``__str__``/``__format__`` to print ``NatsSubjectEnum.SYSTEM``
instead of ``SYSTEM`` — and nats-py builds its SUB/PUB protocol lines with
f-strings, so a bare member would silently register interest in a subject
literally named ``NatsSubjectEnum.SYSTEM``. The server accepts that as a valid
subject and nothing ever errors; the worker simply stops hearing SYSTEM and
ADMIN. The two overrides below restore ``str``'s behaviour, so formatting a
member always yields the wire name on every supported Python version.
"""

from enum import Enum


class NatsSubjectEnum(str, Enum):
  SIGNAL = "SIGNAL"
  ADMIN = "ADMIN"
  SYSTEM = "SYSTEM"
  TRADE = "TRADE"

  # See the module docstring: without these, f"{NatsSubjectEnum.SYSTEM}" is
  # "NatsSubjectEnum.SYSTEM" on Python 3.11+ and the worker subscribes to the
  # wrong subject without a single error.
  __str__ = str.__str__
  __format__ = str.__format__
