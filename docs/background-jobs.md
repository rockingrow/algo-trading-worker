# Background Jobs

The daemon threads that run beside the signal path: terminal-close detection, broker-to-DB reconciliation, the NATS `TRADE` publisher and the Telegram dispatchers.

## MT5EventJob — Terminal-Close Polling (`worker/jobs/mt5_event_job.py`)

`MT5EventJob` runs as a daemon thread inside the child process alongside the NATS signal loop and the MT5 health-check thread. Its sole purpose is to detect positions that the MT5 server closed autonomously — without a corresponding NATS signal reaching the pipeline in time.

### How it works

Every 5 seconds the job calls `scan_terminal_closed_positions()` (`worker/gateways/forex/mt5/close_detector.py`), which:

1. Calls `mt5.positions_get()` filtered by **every magic number this worker owns** — resolved **per scan** via the `ForexExecutor.owned_magics` callable, not snapshotted when the job was built, because the broker re-pushes `strategy_magic_map` on every `WORKER_CONNECTED_ACK` (a NATS reconnect re-runs the handshake) → `current_tickets`. An empty result logs one `No owned magics — terminal-close detection is INACTIVE` warning, since it makes the job a silent no-op.
2. Diffs against an internal `seen_tickets` set maintained across polls
3. For each ticket that disappeared, calls `mt5.history_deals_get(position=ticket)` and picks the **newest** closing deal (`DEAL_ENTRY_OUT` / `DEAL_ENTRY_OUT_BY`, by `deal.time`). A position can have several: taking the *first* let our own TP1 partial (`DEAL_REASON_EXPERT`, which maps to nothing) mask a later manual close, silently discarding the whole event.
4. Reads `deal.reason` to classify the closure:

| MT5 `deal.reason` | `TerminalCloseReason` | Action |
| --- | --- | --- |
| `DEAL_REASON_SL` | `SL` | DB updated → `PositionCDC` publishes TRADE event |
| `DEAL_REASON_TP` | `TP` | DB updated → `PositionCDC` publishes TRADE event |
| `DEAL_REASON_SO` | `STOP_OUT` | DB updated → `PositionCDC` publishes TRADE event |
| `DEAL_REASON_CLIENT` / `MOBILE` / `WEB` | `MANUAL` | DB updated → `PositionCDC` publishes TRADE event |
| `DEAL_REASON_EXPERT` | *(skipped)* | none — closed by our own `order_send` (covered by [`ForexReconcileJob`](#forexreconcilejob--missed-close-reconciliation-workergatewaysforexreconcile_jobpy)) |

1. For terminal-initiated closures, emits a `TerminalClosedEvent` dataclass carrying: `source_ticket`, `deal_ticket`, `symbol`, `close_reason`, `close_price`, `close_volume`, `close_time`, `entry_price`, `sl`, `tp`, and an `AccountSnapshot`.

On each event the job then:

- **4.1** Sends a Telegram notification with full position and account context.
- **4.2** Writes a row to `position_logs` with `author = 'terminal'` (vs `'broker'` for NATS-triggered rows) and updates `positions.status → TERMINAL_CLOSED`. `PositionCDC` then picks up the pending row and publishes a `TRADE` event to the Broker via NATS.

### Resilience during MT5 reconnect

| MT5 state | `positions_get()` result | `seen_tickets` | Events fired |
| --- | --- | --- | --- |
| Connected | valid list | updated each scan | normal |
| Disconnected | `None` | **preserved** | none — scan skipped |
| Reconnected | valid list | diff against preserved state | catch-up for any SL/TP that fired during outage |

When `positions_get()` returns `None` (terminal offline), the scan is skipped and `seen_tickets` is left untouched. This prevents false-positive "position closed" events and ensures that once MT5 comes back online, any positions that were closed during the outage are detected on the next scan.

Polling is gated on the **live connection** (`MT5Gateway.is_connected`), not on the FOREX calendar: once the health thread parks the connection for the weekend the job idles rather than logging a "terminal offline" warning every 5 s, but while the terminal is up it keeps scanning — including right through the weekend window, where 24/7 instruments still trade. See [FOREX weekend market-closed handling](forex.md#forex-weekend-market-closed-handling).

The job shares the process-level `stop_event` (`multiprocessing.Event`) with the health-check thread and the NATS loop, so it shuts down cleanly as part of the normal worker lifecycle — no independent restart mechanism is needed.

## Position Reconciliation — the shared broker↔DB safety net (`worker/gateways/reconcile_job.py`)

Every market has a channel that reports broker-side closes (FOREX: `MT5EventJob`; CRYPTO: the user-data websocket) and every one of them can miss one. A missed close leaves the row stuck `OPENED`/`TP1` **forever**, which then blocks later signals (the `max_open_orders` cap, the netting guard) even though nothing is live on the broker.

`BasePositionReconcileJob` is the market-agnostic backstop behind both: a daemon thread that polls the live broker positions and diffs them against the DB's open rows. It owns the poll loop, the two-scan confirmation, both reconcile directions, and the safety rules below; each market subclasses it and supplies only **how a live position correlates with a DB row** (`_live_keys` / `_db_keys` — the same variation point `BaseSignalProcessor._flat_match_key` / `_flat_db_match_keys` solves for ADMIN FLAT).

| Subclass | Thread | Correlates a row with a live position by | Manual-position import |
| --- | --- | --- | --- |
| `ForexReconcileJob` (`worker/gateways/forex/reconcile_job.py`) | `forex-reconcile`, 60 s | Ticket (`ref_id` / `ref_source_id`) **or** its `(resolved symbol, magic)` slot | not wired (a manual MT5 trade carries a foreign magic, so the worker never owns it) |
| `CryptoReconcileJob` (`worker/gateways/crypto/reconcile_job.py`) | `crypto-reconcile`, 45 s | Resolved exchange symbol (the CEX nets one position per symbol) | wired — see below |

Safety properties, identical for both markets:

- **Two-scan confirmation** — a row must be DB-open *and* broker-flat on two consecutive scans before it is reconciled, absorbing the lag between a fill and the position appearing in (or disappearing from) the broker's position list.
- **No empty-fetch mass close** — if the live-position fetch raises or reports "unavailable", the whole scan is skipped and both suspect sets are left untouched. An API blip or an offline terminal is never read as "everything is flat".
- **Never reconcile what cannot be verified** — if a row's match keys cannot be computed (symbol resolution failed, magic unknown), the row is treated as live and left alone.
- **Idempotent** — only `OPENED`/`TP1` rows are ever handled; a closed row drops out of the next scan.

## ForexReconcileJob — Missed-Close Reconciliation (`worker/gateways/forex/reconcile_job.py`)

`MT5EventJob` is the *primary* close detector for FOREX, but by design it only reports closes the **terminal** initiated: a close carrying `DEAL_REASON_EXPERT` is our own `order_send` and is skipped so the pipeline never double-fires. That leaves a real gap this job closes:

- a **TP1 partial close whose volume equals the whole position** — common when the entry was already at the broker's minimum lot (e.g. `0.01`, as auto-sizing from equity often yields): the partial close flattens the position outright, yet the row stays `TP1`, no terminal event will ever arrive for it, and the next entry on that strategy is rejected as "still open";
- a close that landed while the worker was down, or a ticket that vanished during a reconnect gap.

Every 60 s the job calls `ForexExecutor.get_all_open_positions()` (scoped to the magics this worker owns) and marks any DB-open row matching none of them as closed — after the two-scan confirmation above, so a stuck row clears within ~2 minutes.

### Ticket **and** slot matching

A row is matched primarily by ticket (`ref_id` / `ref_source_id`, normalised to `str` since the platform reports `int`). Ticket alone is not enough: some brokers **re-ticket** a position after a partial close, and the new ticket appears in neither DB column. So each row also carries its `(resolved symbol, magic)` **slot** key — while any live position occupies that strategy's slot on that symbol, the row counts as live and is never reconciled.

| Row's keys | Live position | Outcome |
| --- | --- | --- |
| ticket `111` | ticket `111`, magic `101` | live — never reconciled |
| `ref_source_id` `111`, `ref_id` `222` | ticket `222` | live — either ref may match |
| ticket `111`, slot `XAUUSDc:101` | ticket `999`, magic `101`, `XAUUSDc` (re-ticketed) | live — slot matched |
| ticket `111`, slot `XAUUSDc:101` | ticket `999`, magic `202`, `XAUUSDc` (another strategy) | **stale** — magics isolate strategies |
| strategy missing from `strategy_magic_map` | any position on the symbol | live — key degrades to symbol-only |
| any | *(none)* | **stale** → reconciled after the second scan |

The slot key is deliberately conservative: it can only ever *prevent* a reconcile, never cause one — the right trade-off when the alternative is marking a live position closed and losing management of it.

### Connection guards

`MT5Gateway.get_positions` maps `positions_get() → None` (terminal offline) to an empty list, which is indistinguishable from a genuinely flat account and would otherwise reconcile the entire book. Scans are therefore skipped whenever the platform is not connected, **including a re-check before an empty position list is trusted**.

Connectivity — not `is_market_closed()` — is the gate. Scans used to be skipped for the whole weekend window on the assumption that nothing can trade then; 24/7 instruments break that assumption, and a close there went unreconciled all weekend. Only the *cadence* still consults the calendar: the job idles at `MT5_HEALTH_INTERVAL_WEEKEND` when the market is closed **and** the connection has actually been parked, and keeps the normal 60 s cadence otherwise (including a weekday disconnect, so it resumes the moment the health thread reconnects).

### Missed-close handling (`ForexSignalProcessor._on_missed_close`)

The exact close reason/price is unknown (the deal-history event is what carries those), so the row is marked `TERMINAL_CLOSED` — the same status `MT5EventJob` uses for a platform-side close — with a best-effort mid price (`gateway.get_tick`), an author-`terminal` `position_logs` row, and a comment flagging it as reconciled. `PositionCDC` then publishes the status downstream as usual, and a `Position Reconciled — Closed on Broker` Telegram notification is sent.

## CryptoReconcileJob — Missed-Fill & Manual-Position Reconciliation (`worker/gateways/crypto/reconcile_job.py`)

`CryptoReconcileJob` runs as a daemon thread (`crypto-reconcile`, 45 s poll) alongside the Binance user-data websocket stream, `PositionCDC`, and `NotificationJob`. It is the CRYPTO half of the shared reconciler above (`BasePositionReconcileJob`), and the durability backstop behind the user-data stream: the exchange only exposes positions via a single point-in-time snapshot (no `history_deals_get` equivalent), so instead of classifying a close reason it diffs one `get_all_open_positions()` call (Binance `positionRisk`) against the DB's open rows in **both directions**:

1. **Missed closes** — a DB-open row (`OPENED`/`TP1`) whose resolved symbol is no longer live on the exchange. Backstops the user-data stream for a fill it missed (reconnect gap, handler exception, worker downtime during an SL/TP/liquidation).
2. **Manual opens** — a live exchange position whose resolved symbol matches no DB-open row, i.e. opened directly on the exchange (UI / app / a third party) and never routed through a worker signal. This is the opposite drift from (1), and is what lets **manually-opened positions get imported and managed by the worker** even though they never came from a NATS `SIGNAL`.

### Two-scan confirmation

Both directions require the same symbol to be flagged on **two consecutive scans** before acting, mirroring `MT5EventJob`'s use of `seen_tickets` across polls:

| Case | Suspect set (kept across scans) | Confirmed action |
| --- | --- | --- |
| Missed close | `_suspected`, keyed by `ref_source_id` | DB-open + exchange-flat this scan **and** the previous one → `handler(row)` |
| Manual open | `_suspected_manual`, keyed by resolved exchange symbol | Exchange-open + DB-untracked this scan **and** the previous one → `manual_handler(pos)` |

This absorbs the brief lag between a fill and its appearance in (or disappearance from) `positionRisk`: a freshly opened worker position is never mistaken for a missed close, and a worker-opened position still mid-persist (DB insert not yet committed) is never mistaken for a manual one. If the live-position fetch itself raises, the entire scan is skipped and **both** suspect sets are left untouched — a transient API error can never be read as "everything is flat" or "everything is manual".

### Missed-close handling (`CryptoSignalProcessor._on_missed_close`)

The exact close reason/price is unknown (the live user-data event is what normally carries those), so the row is marked `TERMINAL_CLOSED` with a best-effort mark price (`gateway.get_mark_price`) and a comment flagging it as reconciled. `PositionCDC` then publishes the status downstream as usual, and a `Position Reconciled — Closed on Exchange` Telegram notification is sent.

### Manual-position import (`CryptoSignalProcessor._on_manual_position`)

A confirmed manual position is persisted as a brand-new `OPENED` row so the worker manages it exactly like a signal-opened position from this point on — `PositionCDC` publishes it as a `CREATED` TRADE event, and the existing close backstops (user-data stream, missed-close reconciliation above) later flatten it by resolved symbol, same as any other position:

| Field | Value |
| --- | --- |
| `strategy` | Synthetic `MANUAL` strategy — never a real signal strategy, so it can never collide with the one-active-per-`(strategy, symbol, signal_uxid)` invariant |
| `ref_id` / `ref_source_id` | `MANUAL-<symbol>-<uuid8>` — the uuid suffix means a symbol that is manually opened, closed, then re-opened gets a fresh id each time instead of upserting onto the prior, already-closed trade |
| `action` / `volume` / `opened_price` | Read straight off the live `ExchangePosition` (`side`, `volume`, `price_open`) |
| `sl` / `tp1` / `gateway_message` | `None` — there is no originating signal to record |

Idempotency doesn't depend on the generated id: the reconciler only ever imports a symbol the DB isn't already tracking, so the same live position can't be imported twice even though each import mints a fresh id. A `Manual Position Imported` Telegram notification (`CryptoMessagePresenter.position_imported_manual`) is sent per import, with the symbol, side, quantity, entry price, and generated `ref_source_id`.

`manual_handler` is an optional constructor argument on `CryptoReconcileJob` — leaving it unset keeps the job close-only (its original behaviour before manual-position import existed); `CryptoSignalProcessor` wires `_on_manual_position` in when it starts the job.

## PositionCDC — NATS Trade Publisher (`worker/jobs/cdc_job.py`)

`PositionCDC` implements Change Data Capture on the local SQLite `positions` table. It polls every 2 seconds for rows whose `sync_status` is `PENDING` (set automatically on insert or update), serialises them as `PositionEvent` messages, and publishes them to the NATS `TRADE` subject. After a successful publish the row is marked `PUBLISHED`.

### PositionEvent fields

| Field | Source |
| --- | --- |
| `event` | `CREATED` (first sync) or `UPDATED` (subsequent syncs) |
| `account_id` | Worker account id — `MT5_LOGIN` (FOREX) or `CRYPTO_ACCOUNT_ID` (CRYPTO) |
| `account_name` | `MT5_NAME` (FOREX) or `CRYPTO_ACCOUNT_ID` (CRYPTO) |
| `market` | `MARKET_TYPE` from settings (e.g. `forex`, `crypto`) |
| `gateway` | Gateway/platform name (e.g. `MT5`, `BINANCE`) — sent alongside `account_id` because the raw account id is not unique on its own (it can collide across markets/gateways) |
| `account_balance` / `account_leverage` | Snapshot from the gateway's account (`account_info_fn`) at poll time (CRYPTO reports balance only; leverage is `null`) |
| `signal_id`, `sl`, `tp1`, `tp2`, `risk_percent`, `magic` | Extracted from the original signal JSON stored in `positions.gateway_message` |

The Broker handler is expected to be idempotent (upsert by `market + gateway + account_id + ticket`), so at-least-once delivery is safe even if the worker restarts mid-publish.

## NotificationJob — Telegram Outbox Dispatcher (`worker/jobs/notification_job.py`)

`NotificationJob` is the worker side of the notification outbox pattern. It polls the SQLite `notifications` table every 1 second and dispatches due rows via the appropriate Telegram sender, keeping Telegram I/O completely off the NATS event loop.

### Routing

The job is constructed with a `{channel → TelegramNotification}` map built by `WorkerContext`:

| `channel` value | Maps to | Backing chat IDs |
| --- | --- | --- |
| `INDIVIDUAL` | `ctx.direct_notifier` | `TELEGRAM_WORKER_LOG_CHAT_IDS` (management) |
| `COMMUNITY` | `ctx.direct_channel_notifier` | `TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS` (signal channels), falling back to `TELEGRAM_WORKER_LOG_CHAT_IDS` when unset |

Both are comma-separated lists and both accept a `-<chat id>_<topic id>` entry to post into one topic of a group — see [Telegram chat ids](configuration.md#telegram-chat-ids-many-chats-and-group-topics).

In-process callers stay decoupled: they hold an `OutboxNotifier` whose `send_message()` just inserts a row tagged with the right `channel` and `category` — the dispatcher does the routing.

### Retry semantics

| Outcome | Action |
| --- | --- |
| Success (HTTP 200 for every chat ID) | `DELETE` the row (hard-delete; no `status` column required) |
| Partial / total failure | `attempts += 1`, set `next_attempt_at = now + backoff[attempts]`, store `last_error` |
| `attempts >= max_attempts` (default `5`) | Row becomes dead-letter — skipped by the polling query and left in place for inspection |

Backoff schedule (indexed by attempt, capped at last): **5s → 30s → 2m → 10m**. The polling query is `WHERE attempts < max_attempts AND (next_attempt_at IS NULL OR next_attempt_at <= now)` and is served by the `idx_notifications_pending` index.

### Direct-send escape hatches

Two notification paths intentionally bypass the outbox:

1. **Startup / shutdown banners** sent via `ctx.direct_notifier` — must surface even before `NotificationJob` is running or after it has been torn down.
2. **NATS connection events** still go through the outbox via `ctx.nats_enqueue`, but they use the `NATS_EVENT` category so they can be filtered out of analytics if desired.

## CycleNotificationJob — One Telegram message per trade (`worker/jobs/cycle_notification_job.py`)

Every action of one trade folds into a **single** broadcast message that is **rewritten in place** as the trade progresses, instead of an entry, a TP1, a TP2 and a FLAT arriving as four unrelated boxes a reader has to stitch together by eye. A trade is identified by the broker's `signal_uxid` (see [Signal-cycle notifications](signal-execution.md#signal-cycle-notifications)); the write and the send are split across two halves that never call each other:

| Half | Class | Responsibility |
| --- | --- | --- |
| Writer | `CycleNotifier` (`worker/services/cycle_notification_service.py`) | Appends the action to the cycle's timeline, re-renders the whole body, stores it. Never touches Telegram. |
| Reader | `CycleNotificationJob` | Polls every 1 s for chats whose message is behind the cycle, and posts or edits. |

Splitting them is what makes delivery recoverable: the timeline is durable the moment the signal is executed, so a Telegram outage can neither delay a signal nor lose an action.

### Delivery semantics

Unlike the outbox — where a row is sent once and deleted — nothing here is ever deleted, and "delivered" means *this chat has been shown revision N*:

| Chat state | Action |
| --- | --- |
| No `message_id` yet | `sendMessage` (with `message_thread_id` when the entry names a topic), store the id Telegram returns |
| `message_id` present | `editMessageText` on that same message — no thread id, the topic was fixed when it was posted |
| Edit → `message is not modified` | Counted as delivered (a no-op edit; re-posting would duplicate the trade) |
| Edit → message deleted / too old | Forget the `message_id`; the next pass posts a fresh message |
| Anything else failed | `attempts += 1`, back off **5s → 30s → 2m → 10m**, dead-letter at `max_attempts` |

Ordering is guarded by a `revision` counter: recording an action bumps the cycle's `revision`, each chat records the highest `delivered_revision` it has been shown, and `delivered_revision` only ever moves forward. A slow edit therefore can never overwrite a newer body with an older one — a burst of signals on one trade always renders in order.

### Falling back to per-action messages

The cycle takes over reporting only when it can. `CycleNotifier.record()` returns `False` — and the caller sends the standalone message it always sent — when:

- the signal carries **no `signal_uxid`** (a broker that predates the field: there is nothing to group actions by);
- `TELEGRAM_CYCLE_ENABLED=false`, `TELEGRAM_ENABLED=false`, or no broadcast chat resolves (the same list `ctx.channel_notifier` posts to: `TELEGRAM_PRIVATE_BROADCAST_CHAT_IDS`, falling back to `TELEGRAM_WORKER_LOG_CHAT_IDS`);
- `NOTIFICATION_MODE` is not `VERBOSE` (SILENT suppresses the broadcast channel, exactly as for the outbox);
- the cycle write itself failed.

Exactly one of the two fires — never both, never neither.
