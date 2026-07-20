from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class AdminActionEnum(str, Enum):
  FLAT = "FLAT"


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


class PrivateAdminFlatSchema(AdminMessageSchema):
  """FLAT delivered on a worker's **private** ADMIN subject, whose name is
  ``ADMIN.<market>.<gateway>.<account_id>``.

  Unlike the public FLAT, ``market``/``gateway``/``account_id`` are **required**
  here: the private subject addresses exactly one worker, so the payload must
  carry the full composite identity. The worker re-validates that all three
  match its own identity before acting (defence-in-depth on top of the subject
  match). ``strategy``/``symbol`` remain optional close filters."""

  strategy: Optional[str] = None
  symbol: Optional[str] = None
  market: str
  gateway: str
  account_id: str
