from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict


class AdminActionEnum(str, Enum):
  FLAT = "FLAT"
  # Signal-execution control (private subject only). BLOCK_SIGNAL makes the
  # worker skip executing every incoming SIGNAL; ALLOW_SIGNAL resumes normal
  # execution. Account-scoped, so both travel on the private ADMIN subject.
  BLOCK_SIGNAL = "BLOCK_SIGNAL"
  ALLOW_SIGNAL = "ALLOW_SIGNAL"


class AdminMessageSchema(BaseModel):
  action: AdminActionEnum
  timestamp: datetime


class AdminFlatSchema(AdminMessageSchema):
  """FLAT delivered on the **public** ``ADMIN`` subject.

  The broker fans a public FLAT out to every worker, so it carries no
  ``account_id`` — the public subject is not account-scoped. ``market`` and
  ``gateway`` are optional filters: unset means "don't filter on this
  dimension" (a broadcast FLAT with both unset targets every worker matching
  strategy/symbol). Account-scoped FLATs travel on the private subject instead
  (see :class:`PrivateAdminFlatSchema`)."""

  strategy: Optional[str] = None
  symbol: Optional[str] = None
  market: Optional[str] = None
  gateway: Optional[str] = None


class PrivateAdminSchema(AdminMessageSchema):
  """Base for any directive delivered on a worker's **private** ADMIN subject,
  whose name is ``ADMIN.<market>.<gateway>.<account_id>``.

  The private subject addresses exactly one worker, so the payload must carry
  the full composite identity: ``market``/``gateway``/``account_id`` are all
  **required**. The worker re-validates that they match its own identity before
  acting (defence-in-depth on top of the subject match).

  ``coerce_numbers_to_str`` lets ``account_id`` arrive as either the raw broker
  id (e.g. MT5 login as int ``413652379``) or the already-stringified form —
  both are normalised to ``str`` before the identity re-check. Without this,
  pydantic v2 rejects the int payload with a ``string_type`` ValidationError
  and the FLAT is silently dropped (logged but no close happens)."""

  model_config = ConfigDict(coerce_numbers_to_str=True)

  market: str
  gateway: str
  account_id: str


class PrivateAdminFlatSchema(PrivateAdminSchema):
  """FLAT on the private ADMIN subject. Adds the optional ``strategy``/``symbol``
  close filters to the required composite identity."""

  strategy: Optional[str] = None
  symbol: Optional[str] = None


class PrivateAdminSignalControlSchema(PrivateAdminSchema):
  """``BLOCK_SIGNAL`` / ``ALLOW_SIGNAL`` on the private ADMIN subject — toggles
  whether the worker executes incoming SIGNALs. Carries only the required
  composite identity (no extra fields); the ``action`` decides block vs allow."""
