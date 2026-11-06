# Settings (`worker/settings.py`)

Configuration is grouped into nested `<specific>Settings` sub-models on the main `Settings`, so related options live together:

| Group | Access | Covers |
| --- | --- | --- |
| `LoggingSettings` | `settings.logging` | `notification_mode`, `log_level` |
| `NatsSettings` | `settings.nats` | `url`, `token`, `subjects` |
| `ForexSettings` | `settings.forex` | `platform`, MT5 `server`/`login`/`password`/`path`/`name` |
| `CryptoSettings` | `settings.crypto` | `exchange`, API keys, `hedge_mode`, leverage caps, … |
| `StrategySettings` | `settings.strategy` | `slippage_deviation`, entry-drift limits, `magic_map`, TP1 overrides |
| `RiskSettings` | `settings.risk` | `capital`, `risk_percentage`, `max_open_orders`, … |
| `TelegramSettings` | `settings.telegram` | `enabled`, tokens, chat ids, error-log hook |
| `DatabaseSettings` | `settings.database` | `file` |
| `WebSettings` | `settings.web` | `host`, `port` |
| `BrokerSettings` | `settings.broker` | `api_url`, `api_key` |

`market_type` and the derived `account_id` stay at the top level (`settings.market_type` / `settings.account_id`).

- **Env vars are unchanged.** Each group is its own `BaseSettings` that reads the same flat variable names as before (e.g. `NATS_URL`, `MT5_LOGIN`, `MAX_LEVERAGE_CAP`) via per-field `validation_alias` — `.env` files and the initializer in `.env.example` need no changes.
- **One field reads no env var at all:** `risk.max_open_orders`. The broker owns that value (see [the `settings` ACK section](nats-system.md#worker_connected_ack-payload-broker--worker-reply)), so there is no `MAX_OPEN_ORDERS` variable and a line for it in a `.env` does nothing. The field survives with a default of `5` purely as the backfill until the first handshake lands, and its `validation_alias` points at a name nothing sets — without one, pydantic-settings would match the field *name* case-insensitively and a stale `MAX_OPEN_ORDERS` could quietly cap a worker below what the broker told it.
- **Flat dict for subprocesses.** `Settings.flat_dump()` reproduces the original flat `settings_dict` (legacy keys, with `SecretStr`/enum values intact) that is handed across the multiprocessing fork boundary and consumed by gateways, factories and presenters via `settings_dict.get("<flat_key>")`. Read config off the nested objects (`settings.nats.url`); consume the fork-boundary dict off the flat keys (`settings_dict["nats_url"]`).

## Telegram chat ids: many chats, and group topics

`TELEGRAM_WORKER_LOG_CHAT_IDS`, `TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS` and `TELEGRAM_LOG_CHAT_ID` all run through the same `notification_service.parse_chat_targets()` — none of them is special — so each can reach several chats and can address a **topic** inside a group that has the Topics feature switched on:

```bash
# Two groups + one topic inside a third, all from one setting
TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS="-1001111111111,@public_channel,-1002173777783_924584"
# The same syntax works for the management chat and the error-log chat
TELEGRAM_WORKER_LOG_CHAT_IDS="-1001111111111,-1002173777783_100"
TELEGRAM_LOG_CHAT_ID="-1002173777783_555"
```

| Entry | Delivered to |
| ----- | ------------ |
| `-1001111111111` | The group/channel itself (a group with Topics enabled gets it in **General**) |
| `@public_channel` | Public channel by username |
| `-1002173777783_924584` | Topic `924584` of supergroup `-1002173777783` |

The `_<topic id>` suffix makes the worker add [`message_thread_id`](https://core.telegram.org/bots/api#sendmessage) to the `sendMessage` call — the Bot API's identifier for "the target message thread (topic) of a forum", and the only way a bot can post into a specific topic rather than General. Both numbers are the ones in the topic's own link: `t.me/c/2173777783/924584` → `-1002173777783_924584` (the chat id is the link's first number prefixed with `-100`).

Notes:

- Only a **numeric** chat id may carry a topic suffix — usernames may contain underscores themselves (`@my_group_2`), so those are never split.
- The suffix is sent *only* when configured: Telegram answers `400 Bad Request: message thread not found` if a `message_thread_id` is passed for a chat that has no such topic.
- Each chat gets its own Bot API call, and one failing chat (bot kicked, topic deleted) is logged without stopping the others; `send_message()` returns `False` if any of them failed.
- Whitespace and empty entries are ignored, and a chat listed twice is only notified once.
- An empty `TELEGRAM_LOG_CHAT_ID` still falls back to `TELEGRAM_WORKER_LOG_CHAT_IDS`, list and topics included.
