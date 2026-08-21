"""
Tests for CycleNotifier — the writer that folds an action into its trade's
cycle.

The contract the whole feature rests on is the return value: True means the
cycle now owns reporting this action, False means the caller must still send
its own message. Getting that wrong either duplicates every trade in the
channel or loses actions entirely.
"""

import pytest

from worker.schemas.cycle_schema import (
  CycleEventSchema,
  CycleOutcomeEnum,
  CycleStatusEnum,
)
from worker.services.cycle_notification_service import CycleNotifier
from worker.settings import MarketTypeEnum, settings

UXID = "9f2c4b7e18a3d605"


class FakeCycleStore:
  """Records what the notifier writes, and can fail on demand."""

  def __init__(self, fail: bool = False):
    self.fail = fail
    self.appended = []
    self.bodies = []
    self.chats = []
    self._revision = 0

  def append_cycle_event(self, *, strategy, signal_uxid, symbol, event, status=None):
    self.appended.append((strategy, signal_uxid, symbol, event, status))
    if self.fail:
      return None
    self._revision += 1
    return {
      "id": 7,
      "signal_uxid": signal_uxid,
      "strategy": strategy,
      "symbol": symbol,
      "status": status.value if status else None,
      "events": [e for (_, _, _, e, _) in self.appended],
      "revision": self._revision,
    }

  def set_cycle_message_text(self, cycle_id, message_text, revision):
    self.bodies.append((cycle_id, message_text, revision))

  def ensure_cycle_chats(self, cycle_id, chat_ids):
    self.chats.append((cycle_id, list(chat_ids)))


@pytest.fixture(autouse=True)
def cycles_on(monkeypatch):
  monkeypatch.setattr(settings.telegram, "enabled", True)
  monkeypatch.setattr(settings.telegram, "cycle_enabled", True)
  monkeypatch.setattr(settings.logging, "notification_mode", "VERBOSE")


def _event(action="LONG", outcome=CycleOutcomeEnum.FILLED, **overrides):
  return CycleEventSchema(action=action, outcome=outcome, **overrides)


def _notifier(store=None, chat_ids=("-1001",), **kwargs):
  return CycleNotifier(
    store if store is not None else FakeCycleStore(),
    chat_ids,
    {"market_type": MarketTypeEnum.FOREX},
    **kwargs,
  )


def _record(notifier, **overrides):
  payload = {
    "signal_uxid": UXID,
    "strategy": "strat-a",
    "symbol": "XAUUSD",
    "event": _event(),
    "status": CycleStatusEnum.OPENED,
  }
  payload.update(overrides)
  return notifier.record(**payload)


# ── When the cycle takes over ─────────────────────────────────────────────── #


def test_recording_an_action_renders_and_stores_the_body():
  store = FakeCycleStore()
  assert _record(_notifier(store)) is True
  cycle_id, body, revision = store.bodies[0]
  assert cycle_id == 7 and revision == 1
  assert body.startswith("<pre>") and body.endswith("</pre>")
  assert "LONG XAUUSD" in body


def test_the_body_is_rendered_with_the_live_account_footer():
  store = FakeCycleStore()
  _record(_notifier(store, footer_fn=lambda: "Balance: 10240"))
  assert "Balance: 10240" in store.bodies[0][1]


def test_every_configured_chat_gets_a_delivery_row():
  store = FakeCycleStore()
  _record(_notifier(store, chat_ids=("-1001", "-1002")))
  assert store.chats == [(7, ["-1001", "-1002"])]


def test_the_status_is_handed_to_the_store_for_merging():
  store = FakeCycleStore()
  _record(_notifier(store), status=CycleStatusEnum.TP1)
  assert store.appended[0][4] is CycleStatusEnum.TP1


def test_later_actions_re_render_the_whole_timeline():
  store = FakeCycleStore()
  notifier = _notifier(store)
  _record(notifier)
  _record(notifier, event=_event(action="TP1"), status=CycleStatusEnum.TP1)
  latest_body = store.bodies[-1][1]
  assert "<b>LONG</b>" in latest_body and "<b>TP1</b>" in latest_body


# ── When the caller keeps its own message ─────────────────────────────────── #


def test_a_signal_without_a_uxid_is_not_a_cycle():
  # A broker that predates signal_uxid has nothing to group actions by.
  store = FakeCycleStore()
  assert _record(_notifier(store), signal_uxid=None) is False
  assert store.appended == []


def test_telegram_disabled_declines_the_action():
  store = FakeCycleStore()
  settings.telegram.enabled = False
  try:
    assert _record(_notifier(store)) is False
  finally:
    settings.telegram.enabled = True
  assert store.appended == []


def test_cycles_can_be_turned_off_without_turning_telegram_off(monkeypatch):
  monkeypatch.setattr(settings.telegram, "cycle_enabled", False)
  store = FakeCycleStore()
  assert _record(_notifier(store)) is False


def test_silent_mode_declines_so_the_outbox_drops_it_as_before(monkeypatch):
  monkeypatch.setattr(settings.logging, "notification_mode", "SILENT")
  assert _record(_notifier()) is False


def test_no_channel_chat_configured_declines():
  assert _record(_notifier(chat_ids=())) is False


def test_a_failed_write_falls_back_instead_of_swallowing_the_action():
  store = FakeCycleStore(fail=True)
  assert _record(_notifier(store)) is False
  assert store.bodies == []


# ── Footer binding ────────────────────────────────────────────────────────── #


def test_footer_can_be_bound_after_construction():
  # The account footer needs a connected broker, so the processor supplies it
  # only once it is up — later than the context that builds the notifier.
  store = FakeCycleStore()
  notifier = _notifier(store)
  notifier.bind_footer(lambda: "Balance: 999")
  _record(notifier)
  assert "Balance: 999" in store.bodies[0][1]
