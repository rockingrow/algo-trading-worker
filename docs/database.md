# SQLite Schema (`worker_data.sqlite`)

Three tables are created on startup by `db_init()` using WAL journal mode. There are no runtime migrations — the schema is created from scratch (drop the sqlite file to recreate).

The `positions` / `position_logs` columns are **gateway-neutral** because the same tables back both markets (FOREX/MT5 and CRYPTO/CEX). `worker/db/repository.py` is the single boundary that maps these physical columns to the application-domain names callers use (and parses the TEXT ref ids ↔ `int`), so the NATS `PositionEvent` contract and consumers are unaffected:

| DB column (generic) | App-domain key | Notes |
| --- | --- | --- |
| `ref_id` | `ticket` | TEXT in DB, `int` in app |
| `ref_source_id` | `source_ticket` | TEXT in DB, `int` in app |
| `strategy_code` | `magic` | INTEGER (MT5 magic; `NULL` for crypto) |
| `gateway_return_code` | `mt5_retcode` | INTEGER broker/exchange status code |
| `gateway_message` | `message` | Raw signal/event JSON |

## `positions` — Live position state

The canonical record for each open trade. `PositionCDC` watches this table for `sync_status = PENDING` rows to publish as NATS `TRADE` events.

| Column | Type | Description |
| --- | --- | --- |
| `id` | INTEGER PK | Auto-increment row ID |
| `ref_source_id` | TEXT UNIQUE | Originating order/position reference at open — never changes across the position's lifetime |
| `ref_id` | TEXT | Current order/deal reference (may differ from `ref_source_id` after a partial close re-tickets) |
| `strategy` | TEXT | Strategy name from signal (e.g. `MT5_GOLD_M5_V1`) |
| `symbol` | TEXT | Instrument symbol (e.g. `XAUUSD`, `BTCUSD`) |
| `action` | TEXT | `long` or `short` |
| `volume` | REAL | Lot/contract size at open |
| `opened_price` | REAL | Fill price at entry |
| `closed_price` | REAL | Fill price at close (null while open) |
| `status` | TEXT | See **Position Status Lifecycle** below |
| `gateway_return_code` | INTEGER | Last broker/exchange return code |
| `comment` | TEXT | Last broker/exchange comment string |
| `gateway_message` | TEXT | Full original signal JSON (used by `PositionCDC` to extract `signal_id`, `sl`, `tp1`, etc.) |
| `strategy_code` | INTEGER | Broker isolation handle (MT5 magic; `NULL` for crypto) |
| `market_type` | TEXT | `FOREX` / `CRYPTO` tag |
| `sync_status` | TEXT | `PENDING` → `PUBLISHED` — drives CDC delivery |
| `sync_time` | DATETIME | Timestamp of last successful publish |
| `created_at` / `updated_at` | DATETIME | Row timestamps; `updated_at` is used as an optimistic-lock key in `mark_position_synced` |

### Constraints

A partial unique index `uidx_positions_one_active_per_signal` enforces at most one `OPENED` or `TP1` row per `(strategy, symbol, COALESCE(signal_uxid, ''))` key — the composite key a tracked position is addressed by. Closed/force-closed rows are unrestricted. The index is created together with the table (`CREATE UNIQUE INDEX IF NOT EXISTS`).

`COALESCE(signal_uxid, '')` rather than the plain column, because SQLite treats NULLs as distinct in a UNIQUE index: folding every uxid-less row onto one key preserves the original one-active-per-`(strategy, symbol)` guarantee for a broker that sends no `signal_uxid`. The key gained its third component when `FOREX_ALLOW_MULTI_POSITIONS_PER_SYMBOL` made several signals of one strategy able to hold the same symbol; a database created before that carries the previous `uidx_positions_one_active_per_strategy_symbol`, which `_create_tables` **drops** before creating the new index, so an existing database upgrades in place (left behind it would keep rejecting the second signal on a symbol).

### Position Status Lifecycle

```text
(entry rejected) ──► REJECTED           (entry guard: open-order cap / symbol already
                                         held / stale signal — never sent to broker)

OPENED ──► TP1 ──► TP2
       │         └──► SL
       │         └──► R_SL
       │         └──► TERMINAL_CLOSED  (MT5EventJob: SL/TP/Stop-Out fired server-side)
       │         └──► FORCED_CLOSED    (new entry signal arrived while position open)
       └──► FLATTED                    (FLAT signal)
```

| Status | Set by | Meaning |
| --- | --- | --- |
| `OPENED` | Entry signal (or `CryptoReconcileJob` manual-position import, `strategy=MANUAL`) | Position is live |
| `TP1` | TP1 signal | Partially closed; runner is still active |
| `TP2` | TP2 signal | Fully closed at take-profit 2 |
| `SL` | SL signal | Fully closed at stop-loss (NATS-triggered) |
| `R_SL` | R_SL signal | Fully closed at revised stop-loss |
| `TERMINAL_CLOSED` | `MT5EventJob` / exchange event / either market's reconcile job | Broker/exchange closed the position (SL/TP/Stop-Out/manual) before a NATS signal arrived, or the reconciler found the position gone from the broker while the row was still open |
| `FORCED_CLOSED` | New entry signal / liquidation | Position was force-closed (opposing/same-direction entry, or a crypto liquidation) |
| `FLATTED` | FLAT signal | Position was closed by an administrative flat command |
| `REJECTED` | `max_open_orders` / open-order-per-symbol / stale-signal guard | Entry blocked by a worker-side policy before it reached the broker — recorded for audit and forwarded to the broker, but no order was placed |

**Only an active row changes state.** `PositionRepository.update_position_status` updates `WHERE ref_source_id = ? AND status IN ('OPENED', 'TP1')`. `ref_source_id` is not unique over the table's history (a `REJECTED` row is keyed by the signal's id), and a close event can arrive after another path already closed the row. A terminal or `REJECTED` row is therefore never rewritten, and so never re-published by `PositionCDC`. An update that matches no active row is logged at `WARNING` and does nothing.

## `position_logs` — Immutable execution audit trail

An append-only log of every signal processed and its execution result. Never updated — only inserted.

| Column | Type | Description |
| --- | --- | --- |
| `strategy` / `symbol` / `action` | TEXT | Signal identity |
| `ref_id` / `ref_source_id` | TEXT | Order/deal references at time of execution (`ticket` / `source_ticket` in the app) |
| `volume` / `price` / `sl` / `tp1` | REAL | Execution parameters |
| `gateway_return_code` | INTEGER | Broker/exchange result code (`10009` = MT5 filled, etc.) |
| `gateway_message` | TEXT | Full signal/event JSON |
| `market_type` | TEXT | `FOREX` / `CRYPTO` tag |
| `author` | TEXT | `broker` (NATS signal), `terminal` (MT5 detection), or `exchange` (CEX event) |
| `timestamp` | DATETIME | Wall-clock time of insertion |

## `notifications` — Telegram outbox (store-and-forward)

A durable queue of pending Telegram messages. `NotificationJob` polls this table every 1 s, sends due rows, and hard-deletes on success. Failed sends increment `attempts` and reschedule via exponential backoff (`5s → 30s → 2m → 10m`). Rows whose `attempts >= max_attempts` become dead-letters and are left in place.

| Column | Type | Description |
| --- | --- | --- |
| `id` | INTEGER PK | Auto-increment row ID |
| `platform` | TEXT | `TELEGRAM` (extensible to Slack/Discord/etc.) |
| `channel` | TEXT | `INDIVIDUAL` (management chat) or `COMMUNITY` (signal channels) |
| `category` | TEXT | Free-form tag (e.g. `TRADE_EVENT`, `MT5_MANAGEMENT`, `NATS_EVENT`) for filtering/analytics |
| `message_text` | TEXT | HTML payload sent to Telegram |
| `mode` | TEXT | `VERBOSE` / `SILENT` / `ERROR` from `NOTIFICATION_MODE` (default `VERBOSE`). `INDIVIDUAL` rows are always enqueued `VERBOSE`; only `COMMUNITY` rows carry the configured mode |
| `attempts` | INTEGER | Number of failed delivery attempts so far |
| `max_attempts` | INTEGER | Cap (default `5`); row becomes dead-letter when reached |
| `last_error` | TEXT | Error string from the last failed attempt |
| `next_attempt_at` | DATETIME | Earliest retry time; `NULL` = ready immediately |
| `created_at` / `updated_at` | DATETIME | Row timestamps |

Indexed on `(next_attempt_at, id)` to make the dispatcher's poll query O(log n).

## `notification_cycles` / `notification_cycle_chats` — One message per trade

Notification state, not trade state: dropping these tables loses chat formatting, never a position. A cycle holds the whole timeline of one trade plus the body last rendered from it; a chat row holds the id of the message being edited in that chat.

**`notification_cycles`**

| Column | Type | Description |
| --- | --- | --- |
| `id` | INTEGER PK | Auto-increment row ID |
| `signal_uxid` | TEXT | The broker's trade-cycle id, shared by the entry and every close that follows |
| `strategy` | TEXT | Part of the cycle key — a uxid is only unique within the strategy that minted it |
| `symbol` | TEXT | Instrument the trade is on |
| `status` | TEXT | Latest position status (`OPENED`/`TP1`/…/`FLATTED`, plus `FAILED`/`REJECTED` when nothing opened) — the message's headline |
| `events` | TEXT | JSON array: the full timeline, one entry per action the worker executed |
| `message_text` | TEXT | The body rendered from `events`, ready to send or edit |
| `revision` | INTEGER | Bumped on every recorded action; drives what each chat still owes |
| `created_at` / `updated_at` | DATETIME | Row timestamps |

Unique on `(strategy, signal_uxid)`.

**`notification_cycle_chats`**

| Column | Type | Description |
| --- | --- | --- |
| `id` | INTEGER PK | Auto-increment row ID |
| `cycle_id` | INTEGER | The cycle this delivery belongs to |
| `chat_id` | TEXT | One entry of `TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS`, kept raw so a `_<topic id>` suffix survives and two topics of one group get their own message |
| `message_id` | TEXT | Telegram's id for the message being edited; `NULL` = nothing posted yet |
| `delivered_revision` | INTEGER | Highest cycle revision this chat has been shown; only ever moves forward |
| `attempts` / `max_attempts` | INTEGER | Retry bookkeeping, dead-letter at the cap (default `5`) |
| `last_error` | TEXT | Error string from the last failed attempt |
| `next_attempt_at` | DATETIME | Earliest retry time; `NULL` = ready immediately |
| `created_at` / `updated_at` | DATETIME | Row timestamps |

Unique on `(cycle_id, chat_id)`, and indexed on `(delivered_revision, next_attempt_at, id)` for the dispatcher's poll.
