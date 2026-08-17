"""
worker/services/cycle_notification_service.py
─────────────────────────────────────────────
Writer half of the one-message-per-trade Telegram notification.

Before this, every action of a trade produced its own message: an entry, its
TP1, its SL and a FLAT arrived as four unrelated boxes in the channel and the
reader had to stitch the trade back together by eye. A trade now owns a single
message that is **rewritten in place** as it progresses, keyed by the broker's
``signal_uxid`` (see :class:`~worker.schemas.signal_schema.SignalSchema`).

The two halves never call each other:

* :class:`CycleNotifier` — records what happened and renders the new body into
  SQLite. It never touches Telegram, so a Telegram outage can neither delay a
  signal nor lose an action: the timeline is already durable.
* :class:`~worker.jobs.cycle_notification_job.CycleNotificationJob` — drains
  those rows and edits each chat's message, with the outbox's retry/backoff
  semantics.

:meth:`CycleNotifier.record` returns ``False`` whenever it cannot own the
notification — no ``signal_uxid`` (a broker that predates the field), cycles
turned off, or SILENT mode. The caller then falls back to the per-action message
it always sent, so nothing goes unreported.
"""

from __future__ import annotations

from typing import Callable, Iterable, Optional

from worker.gateways.cycle_presenter import format_cycle_message
from worker.logger import get_logger
from worker.schemas.cycle_schema import CycleEventSchema, CycleStatusEnum
from worker.schemas.notification_schema import NotificationModeEnum
from worker.services.notification_service import _box
from worker.settings import settings

log = get_logger("worker.services.cycle_notification_service")


class CycleNotifier:
  """Folds one action into its trade's cycle and re-renders the message body.

  *footer_fn* is a callable rather than a string because the account footer
  carries a live balance: a cycle spanning an entry and a close hours later must
  show the balance as of the latest action, not as of the entry.
  """

  def __init__(
    self,
    db_service,
    chat_ids: Iterable[str],
    settings_dict: Optional[dict] = None,
    footer_fn: Optional[Callable[[], str]] = None,
  ) -> None:
    self._db = db_service
    self._chat_ids = [str(chat_id) for chat_id in chat_ids if chat_id]
    self._settings = settings_dict or {}
    self._footer_fn = footer_fn

  def bind_footer(self, footer_fn: Callable[[], str]) -> None:
    """Attach the account-footer source.

    The footer needs a connected broker to read a balance from, so it only
    becomes available after the processor connects — later than the context that
    builds this notifier. Binding it afterwards keeps the composition root free
    of the market-specific gateway.
    """
    self._footer_fn = footer_fn

  @property
  def enabled(self) -> bool:
    """Whether a cycle message would be delivered at all.

    Mirrors the gates the plain outbox applies to a COMMUNITY message: Telegram
    off, cycles off, no chat to post in, or SILENT mode (which suppresses the
    community channel) all mean the caller should keep its own message — the
    outbox then drops it exactly as it does today.
    """
    if not settings.telegram.enabled or not settings.telegram.cycle_enabled:
      return False
    if not self._chat_ids:
      return False
    return settings.logging.notification_mode.upper() == NotificationModeEnum.VERBOSE

  def record(
    self,
    *,
    signal_uxid: Optional[str],
    strategy: str,
    symbol: str,
    event: CycleEventSchema,
    status: Optional[CycleStatusEnum] = None,
  ) -> bool:
    """Append *event* to the cycle and store the body it renders to.

    Returns True when the cycle now owns reporting this action, False when the
    caller must send its own message instead (see the module docstring).
    """
    if not signal_uxid or not self.enabled:
      return False

    cycle = self._db.append_cycle_event(
      strategy=strategy,
      signal_uxid=signal_uxid,
      symbol=symbol,
      event=event.to_event(),
      status=status,
    )
    if cycle is None:
      # The write failed and was already logged. Report False so the action
      # still reaches the channel through the caller's per-action message.
      return False

    footer = self._footer_fn() if self._footer_fn is not None else ""
    body = _box(
      format_cycle_message(cycle, settings_dict=self._settings, footer=footer)
    )
    self._db.set_cycle_message_text(cycle["id"], body, cycle["revision"])
    self._db.ensure_cycle_chats(cycle["id"], self._chat_ids)
    log.debug(
      "[Cycle] Recorded %s/%s action=%s revision=%s",
      strategy,
      signal_uxid,
      event.action,
      cycle["revision"],
    )
    return True
