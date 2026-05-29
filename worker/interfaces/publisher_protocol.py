"""
worker/interfaces/publisher_protocol.py
───────────────────────────────────────
Narrow contract for publishing an outbound message to a subject.

Lets jobs (e.g. PositionCDC) depend on the act of publishing rather than the
concrete :class:`~worker.services.nats_service.NATSPublisher` (Dependency
Inversion / Interface Segregation).
"""

from __future__ import annotations

from typing import Protocol

from worker.schemas.nats_schema import NatsSubjectEnum


class MessagePublisherProtocol(Protocol):
  def publish(self, subject: NatsSubjectEnum, data: str) -> None: ...
