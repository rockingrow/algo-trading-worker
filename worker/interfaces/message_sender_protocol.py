"""
worker/interfaces/message_sender_protocol.py
────────────────────────────────────────────
A single, substitutable contract for anything that sends a notification
message.

Both the direct Telegram sender and the SQLite outbox wrapper conform to it
with the *same* return type (``bool``), so callers can swap one for the other
without surprises (Liskov Substitution). Clients that only need to "send" depend
on this narrow protocol rather than a concrete notifier (Dependency Inversion).
"""

from __future__ import annotations

from typing import Protocol


class MessageSenderProtocol(Protocol):
  def send_message(self, message_text: str) -> bool: ...
