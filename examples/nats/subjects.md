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
again inside a `SYSTEM.RETRY_SIGNALS` replay is dropped by id (checked against
`position_logs.signal_id`). `action` is one of `SignalActionEnum`: `LONG`,
`SHORT`, `TP1`, `TP2`, `R_SL`, `SL`, `FLAT`.

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

Out-of-band administrative commands. Payload is an `AdminFlatSchema` (public) or
`PrivateAdminFlatSchema` (private), handled by
`BaseSignalProcessor._handle_admin_message`. `action` is one of
`AdminActionEnum`: currently only `FLAT`.

A FLAT reaches the worker on **one of two subjects**, and every worker
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

## `SYSTEM` — configuration & handshake replies (broker side)

The broker's outgoing half of the `SYSTEM` conversation, received by
`BaseSignalProcessor._handle_system_message` (or as the direct reply to the
worker's `WORKER_CONNECTED` request). Each payload is a `SystemSchema` subclass
keyed by `action` (`SystemActionEnum`) and addressed to the worker by its worker
id (`account_id` in `<market>-<gateway>-<account_id>` form). The three handshake
replies below normally arrive on the request's **reply inbox** rather than the
shared `SYSTEM` subject, so they reach only the worker that asked.

| Action | Sent | Meaning | Example |
| ------ | ---- | ------- | ------- |
| `WORKER_CONNECTED_ACK` | reply inbox | Handshake accepted; no extra config needed (e.g. a non-crypto worker) | [`system.worker_connected_ack.json`](system.worker_connected_ack.json) |
| `WORKER_CONNECTED_ERROR` | reply inbox | Handshake received but the broker could not build the initial config (carries `reason`) | [`system.worker_connected_error.json`](system.worker_connected_error.json) |
| `CRYPTO_LEVERAGE_INIT` | reply inbox or `SYSTEM` | Push allowed crypto `symbols` + `default_leverage` to a crypto worker (on connect, or when an admin changes the setting) | [`system.crypto_leverage_init.json`](system.crypto_leverage_init.json) |
| `RETRY_SIGNALS` | reply inbox or `SYSTEM` | Replay of recent SIGNALs for the worker's subscribed strategies so a reconnecting worker catches up; each is deduped by `signal_id` and age-gated against `MAX_RETRY_TIMEOUT` | [`system.retry_signals.json`](system.retry_signals.json) |

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
signal. Delivery is at-least-once (publish-then-mark), so the broker upserts
idempotently by the composite key `(market, gateway, account_id, ticket)`.

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
the request's reply inbox with one of the `SYSTEM` messages in the
Broker → Worker section above.

| Action | Meaning | Example |
| ------ | ------- | ------- |
| `WORKER_CONNECTED` | Worker announces its `account_id` (worker id), `market`, `gateway`, and the `strategies` it subscribes to; drives the `RETRY_SIGNALS` replay | [`system.worker_connected.json`](system.worker_connected.json) |
