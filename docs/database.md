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
| `sync_version` | INTEGER | Bumped by every status change; `mark_position_synced` marks a row published only at the version the CDC read and published |
| `created_at` / `updated_at` | DATETIME | Row timestamps (one-second resolution, so not used as a change marker; see `sync_version`) |

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

## `position_logs` — Execution audit trail and the signal dedup key

A log of every signal processed and its execution result. For a signal the
worker executes, the row is written **before** the broker is called and the
answer is written onto it afterwards; every other writer (reconcile closes,
terminal closes, manual imports, ADMIN flat) inserts a finished row and never
updates it.

| Column | Type | Description |
| --- | --- | --- |
| `strategy` / `symbol` / `action` | TEXT | Signal identity |
| `signal_id` / `signal_uxid` | TEXT | The broker's signal id and trade-cycle id; `signal_id` is the dedup key |
| `ref_id` / `ref_source_id` | TEXT | Order/deal references at time of execution (`ticket` / `source_ticket` in the app) |
| `volume` / `price` / `sl` / `tp1` | REAL | Execution parameters |
| `gateway_return_code` | INTEGER | Broker/exchange result code (`10009` = MT5 filled, etc.) |
| `gateway_message` | TEXT | Full signal/event JSON |
| `market_type` | TEXT | `FOREX` / `CRYPTO` tag |
| `author` | TEXT | `broker` (NATS signal), `terminal` (MT5 detection), or `exchange` (CEX event) |
| `attempt_outcome` | TEXT | How the attempt ended — see below. `EXECUTED` by default |
| `timestamp` | DATETIME | Wall-clock time of insertion |

### `attempt_outcome` — why a signal is or is not re-sent

This is the only thing that decides whether a signal may reach the broker
again, read by both the live signal path and the ACK's `retry_signals` replay
(`PositionRepository.signal_exists`). The row is written before the order, so
its existence proves an attempt was made even if the worker dies mid-order; this
column is the verdict.

| Value | Meaning | Re-sendable? |
| --- | --- | --- |
| `ATTEMPTING` | The broker call is in flight, or the worker died before it could record the answer | **An entry: no.** An exit: yes — see below |
| `EXECUTED` | The broker confirmed | No — a second delivery is a duplicate, for an entry and an exit alike |
| `FAILED` | Definitively refused (a policy rejection or a broker rejection), so nothing is live | **Yes** — this is the gap a replay exists to fill |

`ATTEMPTING` is the only row whose answer depends on the action, because the
asymmetry reverses:

* An **entry** is blocked. The broker may hold a position for this signal, and a
  second order would open another one.
* An **exit** is not. A duplicate close resolves against the tracked row and is
  harmless — and if the first close did go through, the re-sent exit finds
  nothing live and syncs the row closed, which is the self-heal. Refusing to
  retry would instead leave a live position with no stop and no exit anyone will
  attempt again.

So an entry blocked this way stays blocked until someone looks. Each market's
worker scans for such rows at startup, scoped to its own `market_type`, and
alerts with the signals to check on the broker (`stale_signal_attempts`).

The default is `EXECUTED` so that a database upgraded in place keeps its
previous behaviour exactly: before this column, the mere existence of a
`position_logs` row blocked a replay.

## `notifications` — Telegram outbox (store-and-forward)

A durable queue of pending Telegram messages. `NotificationJob` polls this table every 1 s. A due row is **claimed before it is sent** — `attempts` is incremented and `next_attempt_at` pushed out, conditional on the `attempts` value the poll read — and only then handed to Telegram; it is hard-deleted on success, and a failure records `last_error` (the claim already counted the attempt). Backoff is exponential (`5s → 30s → 2m → 10m`) and rows whose `attempts >= max_attempts` become dead-letters, left in place. Claiming first is what makes `max_attempts` bound **sends** rather than failures: the job used to send and then delete, so a crash in between re-sent the message on every later poll.

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
