# NATS subjects & example payloads

Every subject this **worker** exchanges messages with the broker on, with a
runnable JSON example for **every action that can occur**. Files are named
`<subject-group>.<action>.json`; open the file linked next to each action.

The subjects are grouped by direction of flow (from the worker's point of view):

- [**Broker → Worker**](#broker--worker) — trade signals, admin directives, and
  configuration the broker pushes down; the worker **subscribes** to these.
- [**Worker → Broker**](#worker--broker) — position events and the connect
  handshake; the worker **publishes** these.

`account_id` appears in two forms across these subjects: the **bare** id
configured on the worker (e.g. `123456` — `MT5_LOGIN` for FOREX,
`CRYPTO_ACCOUNT_ID` for CRYPTO; used on `ADMIN` / `TRADE`) and the **worker id**
`<market>-<gateway>-<account_id>` (e.g. `CRYPTO-BINANCE-7654321`; used on
`SYSTEM`, matching the NATS connection name).

---

# Broker → Worker

## `{strategy}` — trade signals

Each signal is published on the subject **equal to its `strategy` field** (e.g.
`MT5_GOLD_M5_V1`) — there is one subject per strategy, and the worker subscribes
only to the strategies listed in `NATS_SUBJECTS`, so it never sees another
strategy's traffic. Each payload is a `SignalSchema` and is handled by
`BaseSignalProcessor._process_message` → `_process_signal`.

- **Entry / target / stop payloads** carry the full signal: `strategy`,
  `timestamp`, `action`, `symbol`, `signal_id`, `price`, `quantity`, plus the
  optional `sl` / `tp1` / `tp2` / `risk_percent` / `is_running` fields. A
  **scale-in** additionally sets `is_scale_position: true` and a `scaling` block
  (`tp` / `sl` / `quantity`) — the broker has already baked those multipliers
  into the payload's SL/TP/quantity; the worker only re-applies `scaling.quantity`
  when it sizes the entry itself under `VOLUME_DECISION_ENABLED`.
- **The FLAT directive** is a lighter payload carrying only `strategy`,
  `timestamp`, `action`, `symbol` — no price/quantity, because it means "close
  everything on this strategy".

`signal_id` is the de-duplication key: a signal the worker sees live and then
again inside a `WORKER_CONNECTED_ACK.retry_signals` replay is dropped by id
(checked against `position_logs.signal_id`). `action` is one of
`SignalActionEnum`: `LONG`, `SHORT`, `TP1`, `TP2`, `R_SL`, `SL`, `FLAT`.

`signal_uxid` is the **trade-cycle** id: the entry and every follow-up close
(TP1 / TP2 / SL / R_SL / FLAT) of one trade share the same value, so the broker
can gather this worker's executions under one cycle. It is passed through
untouched and never used for dedup — dedup is strictly by `signal_id`.

| Action | Meaning | Example |
| ------ | ------- | ------- |
| `LONG` | Open a long entry | [`entry.long.json`](entry.long.json) |
| `SHORT` | Open a short entry | [`entry.short.json`](entry.short.json) |
| `LONG` (scale-in) | Add to an existing position (`is_scale_position` + `scaling` block) | [`entry.long.scale.json`](entry.long.scale.json) |
| `TP1` | First partial-close target hit | [`close.tp1.json`](close.tp1.json) |
| `TP2` | Second (final) close target hit | [`close.tp2.json`](close.tp2.json) |
| `R_SL` | Runner stop-loss (SL moved into profit) | [`close.r_sl.json`](close.r_sl.json) |
| `SL` | Stop-loss hit | [`close.sl.json`](close.sl.json) |
| `FLAT` | Close-all directive for the strategy (lightweight payload) | [`close.flat.json`](close.flat.json) |

## `ADMIN` / `ADMIN.<market>.<gateway>.<account_id>` — admin directives

Out-of-band administrative commands, handled by
`BaseSignalProcessor._handle_admin_message`. `action` is one of
`AdminActionEnum`: `FLAT`, `BLOCK_SIGNAL`, `ALLOW_SIGNAL`.

`FLAT` reaches the worker on **one of two subjects**, and every worker
subscribes to both:

- **Private** — `ADMIN.<market>.<gateway>.<account_id>` (e.g.
  `ADMIN.FOREX.MT5.123456`), built from the worker's own `MARKET_TYPE`, gateway
  setting, and account id. Only that one worker is subscribed, so the message
  reaches no other worker. Here `market`, `gateway`, and `account_id` are
  **required** (an account id is only unique within a market/gateway pair), and
  the worker re-validates all three against its own identity before acting
  (defence-in-depth against a misrouted publish).
- **Public** — the shared `ADMIN` subject, fanned out to **every** connected
  worker. It carries **no `account_id`** (the public subject is not
  account-scoped); the worker filters for itself client-side on the optional
  `market` / `gateway` dimensions. `strategy` / `symbol` further narrow which
  positions are closed; all four unset closes everything on the worker.

| Action | Scope | Subject | Example |
| ------ | ----- | ------- | ------- |
| `FLAT` | one account | `ADMIN.FOREX.MT5.123456` (private) | [`admin.flat.json`](admin.flat.json) |
| `FLAT` | market / gateway / strategy / symbol | `ADMIN` (broadcast) | [`admin.flat.broadcast.json`](admin.flat.broadcast.json) |
| `FLAT` | everything | `ADMIN` (broadcast) | [`admin.flat.all.json`](admin.flat.all.json) |

`BLOCK_SIGNAL` / `ALLOW_SIGNAL` toggle whether the worker **executes incoming
SIGNALs**. They are account-scoped, so they are accepted on the **private
subject only** (the same action on the public `ADMIN` subject is ignored);
`market` / `gateway` / `account_id` are required and re-validated against the
worker's identity. While blocked, every incoming SIGNAL is skipped (both live
and ACK replays) — open positions are untouched and can still be
closed via an `ADMIN` `FLAT`. The state is in-memory only, so a worker restart
resets it to allowed. Handled by `BaseSignalProcessor._handle_signal_control`
(payload `PrivateAdminSignalControlSchema`).

| Action | Scope | Subject | Example |
| ------ | ----- | ------- | ------- |
| `BLOCK_SIGNAL` | one account | `ADMIN.FOREX.MT5.123456` (private) | [`admin.block_signal.json`](admin.block_signal.json) |
| `ALLOW_SIGNAL` | one account | `ADMIN.FOREX.MT5.123456` (private) | [`admin.allow_signal.json`](admin.allow_signal.json) |

## `SYSTEM` — configuration & handshake replies (broker side)

The broker's outgoing half of the `SYSTEM` conversation, received by
`BaseSignalProcessor._handle_system_message` (or as the direct reply to the
worker's `WORKER_CONNECTED` request). Each payload is a `SystemSchema` subclass
keyed by `action` (`SystemActionEnum`) and addressed to the worker by its worker
id (`account_id` in `<market>-<gateway>-<account_id>` form).

Config reaches the worker on **two** paths, and which one applies is decided by
*when* the config exists, not by what it contains:

- **Connect-time** — the broker's reply to the worker's `WORKER_CONNECTED`
  request, delivered on the request's private **reply inbox**, so it reaches only
  the worker that asked.
- **Runtime** — a broadcast on the shared `SYSTEM` subject, for a setting an
  admin changes *after* the worker is already connected. The worker stays
  subscribed for its whole lifetime, so a broadcast always lands.

> **One reply per request.** A NATS request inbox resolves on the **first**
> message delivered and then unsubscribes, so a broker that publishes several
> messages to it has all but the first dropped by the client library — never
> received, never logged, on either side. Everything the broker pushes *on
> connect* therefore rides inside the single `WORKER_CONNECTED_ACK` payload. The
> shared `SYSTEM` subject has no such limit, which is exactly why runtime changes
> go there instead.

| Action | Sent | Meaning | Example |
| ------ | ---- | ------- | ------- |
| `WORKER_CONNECTED_ACK` | reply inbox | Handshake accepted, carrying all of this worker's connect-time config (see the sections below). Every section is optional — an ACK with none of them means "nothing to push" | [`system.worker_connected_ack.json`](system.worker_connected_ack.json) |
| `WORKER_CONNECTED_ERROR` | reply inbox | Handshake received but the broker could not build the initial config (carries `reason`) | [`system.worker_connected_error.json`](system.worker_connected_error.json) |
| `CRYPTO_LEVERAGE_INIT` | `SYSTEM` broadcast | Re-run the leverage-init pass on a **connected** crypto worker after an admin changes the account's leverage settings. Same `{symbols, default_leverage}` payload as the ACK section, flattened into the envelope | [`system.crypto_leverage_init.json`](system.crypto_leverage_init.json) |

### `WORKER_CONNECTED_ACK` sections

| Field | Market | Meaning |
| ----- | ------ | ------- |
| `strategy_magic_map` | both | The per-strategy MT5 magic-number map (`{strategy: magic}`), scoped to the strategies this worker subscribes to. Replaces the static `STRATEGY_MAGIC_MAP` .env value; the worker keeps only entries for its own `NATS_SUBJECTS` strategies and stores them in settings. **Omitted or `null` leaves the worker's current map untouched; an explicit `{}` clears it.** CRYPTO stores it too (its `PositionCDC` stamps `strategy_code` from it) but has no executor magic to refresh |
| `crypto_leverage_init` | CRYPTO | Re-initialise per-symbol leverage on the exchange before trading: `{symbols, default_leverage}`, each field an optional override of `CRYPTO_LEVERAGE_INIT_SYMBOLS` / `MAX_LEVERAGE_CAP`. `default_leverage` is a **cap** — each symbol is set to `min(exchange_max, default_leverage)`. Omitting the section skips the connect-time pass (there is no startup pass); a change made later is broadcast as a standalone `CRYPTO_LEVERAGE_INIT` instead |
| `retry_signals` | both | Replay of recent SIGNALs for the worker's subscribed strategies so a reconnecting worker catches up; each is deduped by `signal_id` and age-gated against `MAX_RETRY_TIMEOUT`. Omitted, `null` or `[]` all mean "nothing to replay" |

The example file shows every section at once as a schema reference; a real ACK
only carries the ones that apply to that worker.

Config sections are applied **before** `retry_signals`, which always runs last: a
replayed FOREX entry is routed by its strategy's magic and a replayed CRYPTO
entry is sized against the exchange's leverage, so replaying first would fire
orders against config that hadn't been applied yet.

Each section — and each entry within it — is validated **independently**, so a
fault costs only the part that is actually broken: one malformed signal is
dropped while the rest of the batch still replays and the magic map still
applies, and one bad magic costs only that strategy. Every discarded entry is
logged at `ERROR` with its reason. Two exceptions: if *every* magic-map entry is
unparseable the section is skipped rather than stored as `{}` (an empty map means
"clear my magics", and a parse failure must not be mistaken for one), and a
broken **envelope** — bad `account_id` / `timestamp` — drops the whole ACK, since
it addresses nobody.

---

# Worker → Broker

## `TRADE` — position events

Payload is a `PositionEvent`, published by `PositionCDC` (`worker/jobs/cdc_job.py`)
whenever a row in the worker's local `positions` table is inserted
(`event: CREATED`) or updated (`event: UPDATED`). Besides the trade fields
(`symbol`, `action`, `volume`, `opened_price`, `closed_price`, `sl`/`tp1`/`tp2`,
…) it carries an **account snapshot** (`account_id`, `account_name`, `market`,
`gateway`, `account_leverage`, `account_balance`) the broker needs to
create/upsert the trade and address the worker later. `ref_id` is the gateway's
own order/ticket reference; `signal_id` ties the event back to the originating
signal (echoed on every event — the entry's id on `CREATED`, the close
signal's id on `UPDATED`); `signal_uxid` is the trade-cycle id shared by every
event of one trade. Delivery is at-least-once (publish-then-mark), so the
broker upserts idempotently by the composite key
`(market, gateway, account_id, ticket)`.

`status` is `PositionStatusEnum`. A `REJECTED` event is a `CREATED` position a
worker-side policy (e.g. `MAX_OPEN_ORDERS`) blocked before it reached the
gateway — it never opened, and the reason rides in `comment`.

| `event` | `status` | Meaning | Example |
| ------- | -------- | ------- | ------- |
| `CREATED` | `OPENED` | New position opened on the gateway | [`trade.created.opened.json`](trade.created.opened.json) |
| `CREATED` | `REJECTED` | Entry blocked by a worker-side policy; never sent to the gateway (reason in `comment`) | [`trade.created.rejected.json`](trade.created.rejected.json) |
| `UPDATED` | `TP1` | First partial-close target hit | [`trade.updated.tp1.json`](trade.updated.tp1.json) |
| `UPDATED` | `TP2` | Second (final) close target hit | [`trade.updated.tp2.json`](trade.updated.tp2.json) |
| `UPDATED` | `SL` | Stop-loss hit | [`trade.updated.sl.json`](trade.updated.sl.json) |
| `UPDATED` | `R_SL` | Runner stop-loss (SL moved into profit) hit | [`trade.updated.r_sl.json`](trade.updated.r_sl.json) |
| `UPDATED` | `TERMINAL_CLOSED` | Closed directly on the terminal/exchange (detected by reconcile) | [`trade.updated.terminal_closed.json`](trade.updated.terminal_closed.json) |
| `UPDATED` | `FORCED_CLOSED` | Force-closed because a protective SL could not be placed | [`trade.updated.forced_closed.json`](trade.updated.forced_closed.json) |
| `UPDATED` | `FLATTED` | Closed by an `ADMIN` `FLAT` directive | [`trade.updated.flatted.json`](trade.updated.flatted.json) |

## `SYSTEM` — connect announcement (worker side)

The worker's outgoing half of the `SYSTEM` conversation. Right after it connects
to NATS the worker publishes a single `WORKER_CONNECTED` (via NATS `request`)
announcing itself and asking for initial configuration. The broker replies on
the request's reply inbox with exactly one of the `SYSTEM` messages in the
Broker → Worker section above.

| Action | Meaning | Example |
| ------ | ------- | ------- |
| `WORKER_CONNECTED` | Worker announces its `account_id` (worker id), `market`, `gateway`, and the `strategies` it subscribes to; scopes the ACK's `strategy_magic_map` and `retry_signals` replay | [`system.worker_connected.json`](system.worker_connected.json) |
