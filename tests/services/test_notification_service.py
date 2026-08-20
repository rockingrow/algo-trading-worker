from pydantic import SecretStr

import worker.services.notification_service as notification_service
from worker.services.notification_service import (
  ChatTarget,
  EditOutcome,
  Notification,
  OutboxNotifier,
  TelegramCycleNotification,
  TelegramNotification,
  _box,
  parse_chat_targets,
)
from worker.settings import TelegramSettings


def test_box_wraps_in_pre():
  assert _box("  hello  ") == "<pre>hello</pre>"


def test_outbox_notifier_returns_bool_and_enqueues():
  captured = []
  notifier = OutboxNotifier(lambda m: captured.append(m))
  result = notifier.send_message("hi")
  assert result is True  # substitutable with a direct sender (Liskov)
  assert captured == ["hi"]


def test_outbox_notifier_is_a_notification():
  # Conforms to the Notification base / MessageSenderProtocol
  assert isinstance(OutboxNotifier(lambda m: None), Notification)


def test_telegram_disabled_returns_true_without_network():
  notifier = TelegramNotification()
  notifier.enabled = False
  assert notifier.send_message("anything") is True


def test_telegram_missing_token_returns_false():
  notifier = TelegramNotification()
  notifier.enabled = True
  notifier.bot_token = ""
  assert notifier.send_message("anything") is False


# ── chat targets: multiple chats & forum topics ─────────────────────


def test_parse_chat_targets_keeps_a_plain_chat_id_untouched():
  assert parse_chat_targets("-1001111111111") == [ChatTarget("-1001111111111")]


def test_parse_chat_targets_splits_a_topic_suffix():
  # "<chat id>_<topic id>" is what a group with Topics enabled looks like.
  assert parse_chat_targets("-1002173777783_924584") == [
    ChatTarget("-1002173777783", 924584)
  ]


def test_parse_chat_targets_reads_a_comma_separated_list():
  raw = " -1001111111111 ,-1002173777783_924584,, @public_channel "
  assert parse_chat_targets(raw) == [
    ChatTarget("-1001111111111"),
    ChatTarget("-1002173777783", 924584),
    ChatTarget("@public_channel"),
  ]


def test_parse_chat_targets_accepts_an_already_split_list():
  # TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS arrives pre-split by TelegramSettings, and each
  # entry still has to be parsed for a topic suffix.
  assert parse_chat_targets(["-100111", "-1002173777783_924584"]) == [
    ChatTarget("-100111"),
    ChatTarget("-1002173777783", 924584),
  ]
  # …and a list entry that itself holds a comma-separated list still splits.
  assert parse_chat_targets(["-100111,-100222_5"]) == [
    ChatTarget("-100111"),
    ChatTarget("-100222", 5),
  ]


def test_parse_chat_targets_collapses_duplicates():
  raw = "-100111,-100111,-100111_5"
  assert parse_chat_targets(raw) == [ChatTarget("-100111"), ChatTarget("-100111", 5)]


def test_parse_chat_targets_ignores_empty_values():
  assert parse_chat_targets("") == []
  assert parse_chat_targets(None) == []
  assert parse_chat_targets(" , ") == []
  # WorkerContext falls back to [settings_dict.get("telegram_chat_id")], which
  # is [None] when that key is absent — it must not become a "None" chat.
  assert parse_chat_targets([None]) == []


def test_parse_chat_targets_does_not_invent_topics_from_usernames():
  # A username may legitimately contain underscores (and end in digits); only a
  # numeric chat id can carry a topic suffix.
  assert parse_chat_targets("@my_group_2") == [ChatTarget("@my_group_2")]
  assert parse_chat_targets("-100111_abc") == [ChatTarget("-100111_abc")]


def _post_recorder(monkeypatch, status_for=None):
  """Capture every sendMessage payload; optionally fail some of them."""
  sent: list[dict] = []

  class _Response:
    def __init__(self, status_code: int) -> None:
      self.status_code = status_code
      self.text = "error" if status_code != 200 else "ok"

  def fake_post(url, json=None, timeout=None):
    sent.append(json)
    return _Response(status_for(json) if status_for else 200)

  monkeypatch.setattr(notification_service.requests, "post", fake_post)
  return sent


def _notifier(chat_ids) -> TelegramNotification:
  notifier = TelegramNotification(chat_ids=chat_ids, bot_token=SecretStr("tok"))
  notifier.enabled = True
  return notifier


def test_topic_chat_id_sends_message_thread_id(monkeypatch):
  sent = _post_recorder(monkeypatch)

  assert _notifier("-1002173777783_924584").send_message("hi") is True

  assert len(sent) == 1
  assert sent[0]["chat_id"] == "-1002173777783"
  # Bot API type is Integer, and the topic must not leak into the chat id.
  assert sent[0]["message_thread_id"] == 924584


def test_plain_chat_id_omits_message_thread_id(monkeypatch):
  sent = _post_recorder(monkeypatch)

  _notifier("-1001111111111").send_message("hi")

  # Telegram rejects the field on a group without that thread, so it may only
  # be present when a topic was actually configured.
  assert "message_thread_id" not in sent[0]


def test_fans_out_to_every_configured_chat(monkeypatch):
  sent = _post_recorder(monkeypatch)

  notifier = _notifier("-100111,-1002173777783_924584,@chan")
  assert notifier.send_message("hello") is True

  assert [(p["chat_id"], p.get("message_thread_id")) for p in sent] == [
    ("-100111", None),
    ("-1002173777783", 924584),
    ("@chan", None),
  ]
  # One body, delivered to each.
  assert {p["text"] for p in sent} == {"hello"}


def test_one_failing_chat_does_not_stop_the_others(monkeypatch):
  # Middle group answers 400 (bot kicked, topic deleted, …).
  sent = _post_recorder(
    monkeypatch, status_for=lambda p: 400 if p["chat_id"] == "-100222" else 200
  )

  # Reported as a failure, but every other group was still notified.
  assert _notifier("-100111,-100222,-100333").send_message("hi") is False
  assert [p["chat_id"] for p in sent] == ["-100111", "-100222", "-100333"]


def test_chat_id_that_parses_to_nothing_is_a_noop(monkeypatch):
  sent = _post_recorder(monkeypatch)

  assert _notifier(" , ").send_message("hi") is False
  assert sent == []


# ── every chat-id *setting* takes a list, not just the signals channel ──


def test_management_chat_id_setting_takes_a_list(monkeypatch):
  """TELEGRAM_WORKER_LOG_CHAT_IDS — service lifecycle, NATS events, MT5 health."""
  monkeypatch.setattr(
    notification_service.settings.telegram,
    "chat_id",
    "-100111,-1002173777783_924584",
    raising=False,
  )
  sent = _post_recorder(monkeypatch)

  # No chat_ids argument: the notifier falls back to the setting.
  notifier = TelegramNotification(bot_token=SecretStr("tok"))
  notifier.enabled = True
  notifier.send_message("hi")

  assert [(p["chat_id"], p.get("message_thread_id")) for p in sent] == [
    ("-100111", None),
    ("-1002173777783", 924584),
  ]


def test_channel_id_setting_keeps_topics_through_settings_parsing(monkeypatch):
  """TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS — split by the settings validator, topics intact."""
  monkeypatch.setenv("TELEGRAM_ENABLED", "true")
  monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
  monkeypatch.setenv("TELEGRAM_WORKER_LOG_CHAT_IDS", "-100999")
  monkeypatch.setenv(
    "TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS", "-100111, -1002173777783_924584"
  )

  telegram = TelegramSettings()

  # The validator only splits on commas — the topic suffix survives to the
  # notifier, which is the one that knows what to do with it.
  assert telegram.chat_channel_id == ["-100111", "-1002173777783_924584"]
  assert parse_chat_targets(telegram.chat_channel_id) == [
    ChatTarget("-100111"),
    ChatTarget("-1002173777783", 924584),
  ]


def test_log_chat_id_setting_takes_a_list(monkeypatch):
  """TELEGRAM_LOG_CHAT_ID — forwarded ERROR records."""
  monkeypatch.setattr(
    notification_service.settings.telegram,
    "log_chat_id",
    "-100111,-1002173777783_924584",
    raising=False,
  )
  monkeypatch.setattr(
    notification_service.settings.telegram,
    "log_bot_token",
    SecretStr("log-tok"),
    raising=False,
  )
  sent = _post_recorder(monkeypatch)

  notifier = notification_service.TelegramLogNotification()
  notifier.enabled = True
  notifier.send_message("boom")

  assert [(p["chat_id"], p.get("message_thread_id")) for p in sent] == [
    ("-100111", None),
    ("-1002173777783", 924584),
  ]


# ── TelegramCycleNotification (send once, then edit in place) ─────────────── #


class _Response:
  def __init__(self, status_code=200, payload=None, text=""):
    self.status_code = status_code
    self._payload = payload
    self.text = text or ""

  def json(self):
    if self._payload is None:
      raise ValueError("no json")
    return self._payload


def _cycle_sender(monkeypatch, response):
  """A ready sender whose HTTP call returns *response*; records the calls."""
  calls = []

  def fake_post(url, json=None, timeout=None):
    calls.append((url, json))
    return response

  monkeypatch.setattr(notification_service.requests, "post", fake_post)
  sender = TelegramCycleNotification()
  sender.enabled = True
  sender.bot_token = SecretStr("token")
  return sender, calls


def test_cycle_send_returns_the_message_id(monkeypatch):
  sender, calls = _cycle_sender(
    monkeypatch, _Response(payload={"ok": True, "result": {"message_id": 4242}})
  )
  assert sender.send("-1001", "<pre>body</pre>") == "4242"
  url, payload = calls[0]
  assert url.endswith("/sendMessage")
  assert payload["chat_id"] == "-1001"
  assert payload["parse_mode"] == "HTML"


def test_cycle_send_without_a_message_id_is_treated_as_no_message(monkeypatch):
  # Nothing to edit later, so the caller must not record it as delivered.
  sender, _ = _cycle_sender(monkeypatch, _Response(payload={"ok": True, "result": {}}))
  assert sender.send("-1001", "body") is None


def test_cycle_send_reports_an_http_failure(monkeypatch):
  sender, _ = _cycle_sender(monkeypatch, _Response(status_code=429, text="Too Many"))
  assert sender.send("-1001", "body") is None


def test_cycle_send_is_skipped_when_telegram_is_disabled(monkeypatch):
  sender, calls = _cycle_sender(monkeypatch, _Response())
  sender.enabled = False
  assert sender.send("-1001", "body") is None
  assert calls == []


def test_cycle_send_lands_in_the_configured_topic(monkeypatch):
  # A cycle must reach the same forum topic as every other notification, so the
  # stored entry keeps the "<chat>_<topic>" shape and is parsed at send time.
  sender, calls = _cycle_sender(
    monkeypatch, _Response(payload={"ok": True, "result": {"message_id": 7}})
  )
  assert sender.send("-1002173777783_924584", "body") == "7"
  _, payload = calls[0]
  assert payload["chat_id"] == "-1002173777783"
  assert payload["message_thread_id"] == 924584


def test_cycle_send_omits_the_thread_id_for_a_plain_chat(monkeypatch):
  # Telegram rejects the field on a group without that thread.
  sender, calls = _cycle_sender(
    monkeypatch, _Response(payload={"ok": True, "result": {"message_id": 7}})
  )
  sender.send("-1001111111111", "body")
  assert "message_thread_id" not in calls[0][1]


def test_cycle_edit_identifies_the_message_without_a_thread_id(monkeypatch):
  # The topic was fixed when the message was posted; editMessageText takes the
  # chat and message id only.
  sender, calls = _cycle_sender(monkeypatch, _Response(payload={"ok": True}))
  assert sender.edit("-1002173777783_924584", "7", "body") is EditOutcome.OK
  _, payload = calls[0]
  assert payload["chat_id"] == "-1002173777783"
  assert "message_thread_id" not in payload
  assert payload["message_id"] == "7"


def test_cycle_send_with_an_unusable_chat_id_is_a_noop(monkeypatch):
  sender, calls = _cycle_sender(monkeypatch, _Response())
  assert sender.send(" ", "body") is None
  assert calls == []


def test_cycle_edit_targets_the_stored_message(monkeypatch):
  sender, calls = _cycle_sender(monkeypatch, _Response(payload={"ok": True}))
  assert sender.edit("-1001", "4242", "<pre>new</pre>") is EditOutcome.OK
  url, payload = calls[0]
  assert url.endswith("/editMessageText")
  assert payload["message_id"] == "4242"
  assert payload["text"] == "<pre>new</pre>"


def test_cycle_edit_of_an_unchanged_body_counts_as_delivered(monkeypatch):
  # Telegram rejects a no-op edit; calling it a failure would make a redelivered
  # signal re-post the whole cycle.
  sender, _ = _cycle_sender(
    monkeypatch,
    _Response(status_code=400, text="Bad Request: message is not modified"),
  )
  assert sender.edit("-1001", "4242", "body") is EditOutcome.OK


def test_cycle_edit_reports_a_deleted_message_as_missing(monkeypatch):
  sender, _ = _cycle_sender(
    monkeypatch,
    _Response(status_code=400, text="Bad Request: message to edit not found"),
  )
  assert sender.edit("-1001", "4242", "body") is EditOutcome.MISSING


def test_cycle_edit_reports_a_rate_limit_as_a_retryable_failure(monkeypatch):
  sender, _ = _cycle_sender(monkeypatch, _Response(status_code=429, text="Too Many"))
  assert sender.edit("-1001", "4242", "body") is EditOutcome.FAILED


def test_cycle_edit_survives_a_network_exception(monkeypatch):
  def boom(url, json=None, timeout=None):
    raise ConnectionError("down")

  monkeypatch.setattr(notification_service.requests, "post", boom)
  sender = TelegramCycleNotification()
  sender.enabled = True
  sender.bot_token = SecretStr("token")
  assert sender.edit("-1001", "4242", "body") is EditOutcome.FAILED
