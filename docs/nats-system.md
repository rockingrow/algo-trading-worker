# SYSTEM Subject

The `SYSTEM` NATS subject carries the connect handshake and the broker's config pushes. Which of **two paths** a push takes is decided by *when* the config exists, not by what it contains:

| Path | Delivery | Used for |
| --- | --- | --- |
| **Connect-time** | Sections of the `WORKER_CONNECTED_ACK`, on the handshake's private reply inbox | Everything the worker needs before it trades: `settings`, `strategy_magic_map`, `crypto_leverage_init`, `retry_signals` |
| **Runtime** | A standalone action broadcast on the shared `SYSTEM` subject | A setting changed *after* the worker is connected — `WORKER_SETTINGS_UPDATE` (the account's settings, by its owner from the bot or by an admin) and `CRYPTO_LEVERAGE_INIT` |

The split exists because a request reply inbox delivers exactly **one** message (see the handshake section below), so it cannot carry a push that arrives later; the shared subject has no such limit and the worker stays subscribed to it for its whole lifetime, so a broadcast always lands.

Broadcasts are received by `BaseSignalProcessor._handle_system_message`, which validates the common envelope (`action` + `timestamp` + `account_id`), routes by worker identity, then dispatches to the `_handle_system_action` hook. The base handles `WORKER_SETTINGS_UPDATE` itself, because the settings it carries are market-neutral; CRYPTO overrides the hook for `CRYPTO_LEVERAGE_INIT` and delegates anything else back to the base. An action no one understands is logged and ignored, so a `SYSTEM` message meant for another market type is harmless (the worker's own `WORKER_CONNECTED`, fanned back out on the shared subject, lands here too and is ignored).

## WORKER_CONNECTED handshake (request/reply)

Right after connecting to NATS, `BaseSignalProcessor._announce_worker_connected` sends `WORKER_CONNECTED` on `SYSTEM` as a NATS **request** (`nc.request`, not a fire-and-forget publish) so the broker always replies on a private inbox rather than staying silent. The broker's reply is one of these actions:

| Reply action | Meaning | Worker behaviour |
| --- | --- | --- |
| `WORKER_CONNECTED_ACK` | The normal reply — carries **all** of this worker's connect-time config in one payload (`settings`, `strategy_magic_map`, `crypto_leverage_init`, `retry_signals` — all optional) | Applies each section that is present (see [WORKER_CONNECTED_ACK payload](#worker_connected_ack-payload-broker--worker-reply)); handshake complete |
| `WORKER_CONNECTED_ERROR` | Broker received it but couldn't process it (missing settings, invalid leverage config, …) | Logged with `reason` for operator attention; not retried — a config problem on the broker side isn't fixed by resending the same request |

> **One reply per request — why the ACK carries everything.** `nc.request` opens a temporary reply inbox, resolves on the **first** message delivered to it, and unsubscribes immediately. A broker that publishes several messages to that inbox has all but the first silently discarded *by the client library*, below the worker's message handler: nothing is logged on either side, and which message "wins" is decided purely by the broker's send order. Splitting connect-time config across several sends is therefore not a delivery race the worker can retry out of — it is guaranteed loss, so every section rides in the single ACK payload.

If the broker doesn't reply within the request timeout (5s), the worker retries with backoff (5s → 10s → 20s, capped) **indefinitely** — the handshake is idempotent on the broker side and gates trading (crypto specifically needs `default_leverage` before it's safe to size an order), so the worker blocks here rather than falling back to running without config. A random 0–500ms jitter is added before the very first attempt only, to desynchronise a reconnect storm (every connected worker reconnects and re-announces at roughly the same instant after a NATS/broker restart). From the 3rd consecutive timeout onward, the retry log escalates from `WARNING` to `ERROR` so it's forwarded to Telegram (`TelegramLogHandler`), alerting an operator that the broker looks unreachable rather than just transiently slow.

The handshake is **single-flight**. Every publisher reconnect re-announces on a thread of its own, so a reconnect storm used to start one forever-retrying handshake per reconnect. A reconnect that arrives while a handshake is in flight now only marks it to announce **once more** when it finishes, so a broker that restarted mid-handshake is still told. The reply is applied under the processor's `_pipeline_lock`, the same lock every NATS message, and every background handler that writes the positions table, runs under. The ACK can arrive on the reconnect thread while the listener is executing a signal, and its replay places orders, so without the lock the signal dedup and the entry guards were check-then-act races and a signal could be placed twice.

Any other reply action is logged as unexpected and ignored — `WORKER_CONNECTED_ACK` and `WORKER_CONNECTED_ERROR` are the only two this worker has a contract for on the reply inbox. In particular a standalone `CRYPTO_LEVERAGE_INIT` is **not** accepted as a reply: on connect it belongs in the ACK's section, and at runtime it belongs on the shared `SYSTEM` subject.

### WORKER_CONNECTED Payload (worker → broker, request)

```json
{
  "action": "WORKER_CONNECTED",
  "account_id": "CRYPTO-BINANCE-7654321",
  "timestamp": "2026-06-30T00:00:00+00:00",
  "market": "CRYPTO",
  "gateway": "BINANCE"
}
```

### WORKER_CONNECTED_ACK Payload (broker → worker, reply)

The single reply carrying this worker's whole connect-time config. Every section is optional; an ACK with none of them is just "handshake complete, nothing to apply".

| Field | Market | Behaviour when present | Behaviour when omitted (or `null`) |
| --- | --- | --- | --- |
| `settings` | both | The runtime toggles this account was left with in a previous session (currently `signal_blocked`) — each one is re-applied through the very same code path the runtime ADMIN action uses | Every toggle keeps its current value; on a fresh start that is its default (`signal_blocked` → *allowed*) |
| `strategy_magic_map` | both | `{strategy_name: magic_number}` for the strategies this worker subscribes to. Entries for other strategies are ignored; an explicit `{}` **clears** the worker's magics | The worker's current map is left **untouched** — omitting the section means "nothing to push", so a reconnect ACK never wipes the magics the worker is already trading under |
| `crypto_leverage_init` | CRYPTO | `{symbols, default_leverage}` — runs the [per-symbol leverage initialisation](crypto.md#per-symbol-leverage-initialisation-crypto) pass before trading | The connect-time pass is skipped, so a worker that wasn't asked never touches exchange leverage. A change made later arrives instead as a runtime [`CRYPTO_LEVERAGE_INIT`](#action-crypto_leverage_init-crypto-runtime) broadcast |
| `retry_signals` | both | A batch of full `SignalSchema` payloads to replay (deduped + age-gated, see below) | Nothing to replay (`[]` is equivalent) |

Applied by `BaseSignalProcessor._apply_worker_connected_ack`, which restores `settings` first, runs the remaining config sections next, and the **replay last**. That ordering is load-bearing: a replayed FOREX entry is routed by its strategy's magic and a replayed CRYPTO entry is sized against the exchange's leverage, so replaying first would fire orders against config that hadn't been applied yet — and because the replay goes through the same `_process_signal` funnel as live traffic, a `signal_blocked` restored *before* it suppresses the batch, while one restored after would arrive a batch of orders too late.

Merging every section into one payload would normally mean one malformed entry takes the whole ACK down, so `WorkerConnectedAckSchema` (`worker/schemas/inbox_schema.py`) validates each section — and each entry within it — **independently**, and a fault costs only the part that is actually broken:

| Fault | Effect |
| --- | --- |
| One malformed value in `settings` | That setting is left at its current value; the other settings in the same section still apply, as do the other sections |
| A `settings` key this worker has no field for | Ignored and reported (the broker believes it is in force, so the version skew is made visible); the known settings still apply |
| One malformed signal in `retry_signals` | That signal is dropped; the rest of the batch still replays and every config section still applies |
| One non-integer magic in `strategy_magic_map` | That strategy is dropped; the other magics still apply (its own signals are then rejected loudly by the unknown-strategy guard) |
| **Every** magic in `strategy_magic_map` unparseable | The section is skipped and the worker's current map is left alone — never stored as `{}`, because an empty map is the broker's explicit *clear my magics* instruction and a parse failure must not be mistaken for one |
| Malformed `crypto_leverage_init` | The leverage pass is skipped; the magic map and replay in the same payload are unaffected |
| Broken **envelope** (bad `account_id` / `timestamp`) | The whole ACK is dropped — it addresses nobody, so there is nothing to apply it to |

Every discarded entry is logged at `ERROR`, one line each (`WORKER_CONNECTED_ACK dropped retry_signals[0] — timestamp: Field required`), so an operator sees exactly what the worker is missing rather than a silent partial apply.

```json
{
  "action": "WORKER_CONNECTED_ACK",
  "account_id": "FOREX-MT5-1234567",
  "timestamp": "2026-06-30T00:00:00+00:00",
  "settings": { "signal_blocked": true, "max_open_orders": 3 },
  "strategy_magic_map": {
    "MT5_GOLD_M5_V1": 20260409,
    "MT5_FX_M15_V2": 20260410
  },
  "crypto_leverage_init": { "symbols": ["BTC", "ETH"], "default_leverage": 10 },
  "retry_signals": [
    {
      "strategy": "MT5_GOLD_M5_V1",
      "signal_id": "be6aac4d-da0d-41a3-9798-14d0d23d3f63",
      "timestamp": "2026-04-10T22:55:00Z",
      "action": "LONG",
      "symbol": "XAUUSD",
      "price": 4858.50,
      "quantity": 6.0000,
      "sl": 4850.00,
      "tp1": 4865.00,
      "tp2": 4870.00
    }
  ]
}
```

#### Section: `settings`

The **account settings** the broker holds for this worker, replayed so it comes back up in the state an operator left it in. The worker keeps them **in memory only** — so without this section a restart silently drops back to the defaults and resumes trading under a block nobody lifted, or under its own cap rather than the account's. The broker owns the state; this is how it hands it back.

`BaseSignalProcessor._apply_worker_settings` reads each attribute in turn and applies it through the *same helper the runtime path calls*, so restoring a value is indistinguishable from receiving it live: the same already-in-that-state no-op, the same log line, the same Telegram alert. Each field is independently optional — `null` (or absent) means "not pushed, leave this setting alone", which is why `signal_blocked` is a nullable bool: `false` is the broker explicitly saying *unblocked*, not the same statement as saying nothing. Fields are salvaged one at a time, so a malformed value costs only its own setting.

| Field | Behaviour when present | Behaviour when omitted (or `null`) |
| --- | --- | --- |
| `signal_blocked` | `true` re-applies the [`BLOCK_SIGNAL`](nats-admin.md#actions-block_signal--allow_signal) gate (every SIGNAL — live **and** this ACK's `retry_signals` — is skipped); `false` clears it, exactly as `ALLOW_SIGNAL` does | The gate keeps its current value: its default (*allowed*) after a restart, or whatever ADMIN last set on a reconnect that kept the process alive |
| `max_open_orders` | Becomes this worker's open-position cap — see [the exposure-cap guard](design-decisions.md#signal-execution--key-design-decisions). `0` disables the cap and is a real value, not an absence | The cap keeps its current value: the `5` backfill after a restart, or whatever the broker last pushed |

**`max_open_orders` replaces the `MAX_OPEN_ORDERS` .env setting, which no longer exists.** The broker owns the cap: it resolves the account's own value against its own broker-wide default and sends one concrete number, so this worker never has to know a global default exists, and a stale value on a host can no longer disagree with what an owner set. It is stored into the live settings dict under `max_open_orders` — the exact key the old `.env` value populated — so `guard.max_open_orders_rejection` and every other consumer read it unchanged. `worker/settings.py` keeps the field with a default of `5`, deliberately **without** an env var, purely as the backfill for the window before the first handshake and for a broker too old to send the field.

A change made while the worker is connected does not wait for a reconnect: it arrives on the shared `SYSTEM` subject as a [`WORKER_SETTINGS_UPDATE`](#action-worker_settings_update-runtime) carrying the same block.

Adding a setting means adding its field to `WorkerSettingsSchema` (`worker/schemas/system_schema.py`, shared by the ACK section and the runtime broadcast) plus one branch in `_apply_worker_settings` that delegates to the existing setter — that setter stays the single implementation of *what the setting does*, whichever path the value arrives on.

#### Section: `strategy_magic_map`

The **per-strategy MT5 magic-number map**. The broker owns this mapping centrally, so it is **no longer configured per worker in `.env`** — `STRATEGY_MAGIC_MAP` in `.env` is now only an offline/legacy fallback default (empty by default), and a broker push always replaces it. It lands during `connect()`, before `start_market_jobs` builds the CDC and terminal-close jobs, so those jobs read the freshly-stored map.

Handled generically by `BaseSignalProcessor._apply_strategy_magic_map` for every market (CRYPTO stores it too — its `PositionCDC` stamps `strategy_code` from it — but has no executor magic to refresh):

1. **Scope to this worker:** keep only entries whose key is one of the strategies this worker subscribes to (its `NATS_SUBJECTS` minus the control subjects, via `_subscribed_strategies()`). Any entry for a strategy the worker does not trade is ignored with a warning, so the worker never claims a magic it shouldn't own.
2. Store the filtered map into the live settings under `strategy_magic_map` — the exact key the old `.env` value populated, so `PositionCDC` and every other consumer read it unchanged.
3. Refresh the already-built executor via the `_set_executor_magic_map` hook. FOREX calls `ForexExecutor.set_strategy_magic_map`, so `_magic_for` (order stamping) and `owned_magics()` (account-wide queries + `MT5EventJob` terminal-close detection) reflect the new map immediately; CRYPTO no-ops.

#### Section: `crypto_leverage_init`

Runs the [per-symbol leverage initialisation](crypto.md#per-symbol-leverage-initialisation-crypto) pass on connect. The base cannot execute it itself (it imports no exchange SDK), so it hands the parsed section to the `_apply_market_init` market hook, which `CryptoSignalProcessor` overrides to call `_run_leverage_init`. Per-symbol failure isolation and the "never fall back to the cap blindly" guarantee are unchanged; an uncaught crash inside the job is logged and swallowed so it never takes the worker down. The example above shows every section at once as a schema reference — a real ACK carries only the ones that apply to that worker, so a FOREX worker never receives this one.

| Field | Behaviour when present | Behaviour when omitted |
| --- | --- | --- |
| `symbols` | Initialise exactly this list of raw signal symbols | Falls back to `CRYPTO_LEVERAGE_INIT_SYMBOLS` |
| `default_leverage` | Used as the cap — each symbol is set to `min(exchange_max, default_leverage)` | Falls back to `MAX_LEVERAGE_CAP` |

#### Section: `retry_signals`

A replay of recent SIGNALs for the worker's subscribed strategies, so a worker reconnecting after an outage catches up on what it missed. Handled by `BaseSignalProcessor._apply_retry_signals`, identically for FOREX and CRYPTO: each signal is (1) deduped against `position_logs.signal_id` — one we've already processed (successfully or as a REJECT) is skipped — and (2) age-gated against `MAX_RETRY_TIMEOUT` using the signal's own `timestamp`, so a stale entry/exit is dropped rather than fired hours after the market moved. Eligible signals go through the normal `_process_signal` pipeline, so a replayed entry is indistinguishable from a live one, and one failing signal never aborts the rest of the batch. Live signals get a narrower check: a `signal_id` this process already executed **successfully** (live or by replay) is skipped, so it is not placed twice. Failures are not remembered, so a signal that failed can be resent and run.

### WORKER_CONNECTED_ERROR Payload (broker → worker, reply)

```json
{
  "action": "WORKER_CONNECTED_ERROR",
  "account_id": "CRYPTO-BINANCE-7654321",
  "timestamp": "2026-06-30T00:00:00+00:00",
  "reason": "missing leverage settings for account"
}
```

## Action: `WORKER_SETTINGS_UPDATE` (runtime)

The **runtime** counterpart of the ACK's [`settings`](#worker_connected_ack-payload-broker--worker-reply) section: the same block, nested in a `SYSTEM` envelope and broadcast to an **already-connected** worker when the account's owner changes a value from the Telegram bot (`/maxorders`) or an admin changes a broker-wide default (`/admin_max_orders`). It cannot ride in the ACK because the handshake is long over by then and the reply inbox it used is gone — the shared `SYSTEM` subject, which the worker stays subscribed to for its whole lifetime, is the path that still reaches it.

Handled by `BaseSignalProcessor._handle_system_action` — in the **base**, not a market hook, because these settings are market-neutral — which funnels it into the very same `_apply_worker_settings` the ACK section uses. One behaviour, one notification, whichever path a value arrives on — including the log line, whose prefix names the setting's *owner* (`[SYSTEM]` for the cap, `[ADMIN]` for the signal gate) rather than the delivery path, exactly as the ADMIN-owned toggles already do. To tell a connect-time restore from a runtime change, read the line above it: the restore is preceded by `WORKER_CONNECTED_ACK — handshake complete`, the runtime change by `Received action=WORKER_SETTINGS_UPDATE`.

The broker sends the whole block rather than only the field that changed, since the worker applies it through a path that reads every setting it knows. Fields are salvaged one at a time, so a malformed value costs only its own setting; a push with nothing usable in it leaves every setting as it was, which is the same outcome as no push at all.

```json
{
  "action": "WORKER_SETTINGS_UPDATE",
  "account_id": "FOREX-MT5-1234567",
  "timestamp": "2026-06-30T00:05:00+00:00",
  "settings": { "signal_blocked": false, "max_open_orders": 3 }
}
```

`action`, `timestamp` and `account_id` are required; every field inside `settings` is optional, with the same "omitted means leave it alone" rule as the ACK section. `account_id` scopes the broadcast to one worker — every other worker logs a skip and ignores it.

The broker still re-sends the block on every handshake, so this push is a convenience and not the contract: a worker that was offline for the change picks the new value up when it reconnects.

## Action: `CRYPTO_LEVERAGE_INIT` (CRYPTO, runtime)

The **runtime** counterpart of the ACK's `crypto_leverage_init` section: same payload, flattened into a `SYSTEM` envelope and broadcast to an **already-connected** worker when an admin edits a sub-account's leverage cap or onboards new symbols. It cannot ride in the ACK because the handshake is long over by then, and the reply inbox it used is gone — the shared `SYSTEM` subject, which the worker stays subscribed to for its whole lifetime, is the path that still reaches it.

`CryptoSignalProcessor._handle_system_action` parses `SystemCryptoLeverageInitSchema` and funnels it into the same `_leverage_init_from` → `_run_leverage_init` path the ACK section uses, so both deliveries behave identically; only the log line differs (`via SYSTEM` vs `via ACK`), which is what tells an operator whether a pass was a connect-time init or a runtime change.

| Field | Behaviour when present | Behaviour when omitted |
| --- | --- | --- |
| `symbols` | Initialise exactly this list of raw signal symbols | Falls back to `CRYPTO_LEVERAGE_INIT_SYMBOLS` |
| `default_leverage` | Used as the cap — each symbol is set to `min(exchange_max, default_leverage)` | Falls back to `MAX_LEVERAGE_CAP` |

```json
{
  "action": "CRYPTO_LEVERAGE_INIT",
  "account_id": "CRYPTO-BINANCE-7654321",
  "timestamp": "2026-06-30T00:00:00+00:00",
  "symbols": ["BTC", "ETH"],
  "default_leverage": 10
}
```

`action`, `timestamp` and `account_id` are required; `symbols` and `default_leverage` are optional overrides. `account_id` scopes the broadcast to one worker — every other worker logs a skip and ignores it.
