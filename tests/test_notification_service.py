from worker.services.notification_service import (
  Notification,
  OutboxNotifier,
  TelegramNotification,
  _box,
)


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
