# ADMIN Subject

ADMIN carries out-of-band administrative commands that operate outside the normal strategy signal flow, all handled by the shared `BaseSignalProcessor._handle_admin_message`. Messages travel on **one of two subjects**:

| Subject | Name | Scope | Routing fields |
| --- | --- | --- | --- |
| **Public** | `ADMIN` | Fanned out by the broker to every worker | `market` / `gateway` **optional** filters — **no `account_id`** |
| **Private** | `ADMIN.<market>.<gateway>.<account_id>` | Addressed to exactly one worker | `market` / `gateway` / `account_id` **required** |

Every worker subscribes to the public `ADMIN` subject **and** to its own private `ADMIN.<market>.<gateway>.<account_id>` subject (built from `MARKET_TYPE`, the gateway/platform setting, and the account id — `MT5_LOGIN` for FOREX, `CRYPTO_ACCOUNT_ID` for CRYPTO). This lets the broker target a single account precisely without fanning a message out to everyone.

`AdminActionEnum` actions: **`FLAT`** (both subjects — close positions), and **`BLOCK_SIGNAL`** / **`ALLOW_SIGNAL`** (private subject only — toggle whether the worker executes incoming SIGNALs).

## Action: `FLAT`

Closes open positions across one or more strategies/symbols in a single command.

### Public subject (`ADMIN`)

The broker fans a public FLAT out to every worker; each filters only on the **optional** `market`/`gateway` dimensions. A public FLAT carries **no `account_id`** (the public subject is not account-scoped — a stray `account_id` field is ignored).

| Filter | Behaviour when present |
| --- | --- |
| `market` | Silently ignored if it does not match the worker's `MARKET_TYPE`; processed normally if it matches or is absent |
| `gateway` | Silently ignored if it does not match the worker's gateway/platform setting (`MT5_PLATFORM` for FOREX, `CRYPTO_EXCHANGE` for CRYPTO); processed normally if it matches or is absent |
| `strategy` | Restricts close to positions whose `strategy` column equals this value |
| `symbol` | Restricts close to positions for this symbol |

Omitting `market`/`gateway`/`strategy`/`symbol` closes every tracked open position on every worker that receives it.

```json
{
  "action": "FLAT",
  "timestamp": "2026-06-02T08:00:00+00:00",
  "strategy": "my_strategy",
  "symbol": "XAUUSD",
  "market": "FOREX",
  "gateway": "MT5"
}
```

### Private subject (`ADMIN.<market>.<gateway>.<account_id>`)

An account-scoped FLAT is published to a worker's private subject. Here `market`, `gateway`, and `account_id` are **required**: the subject already addresses one worker, and the worker re-validates that all three fields match its own identity before acting (defence-in-depth against a misrouted publish). `account_id` alone is **not** a unique worker identity — the same raw id can collide across markets/gateways (e.g. an email used as `account_id` also matching a CRYPTO gateway name), which is exactly why the full composite `market`/`gateway`/`account_id` is required and re-checked. `strategy`/`symbol` remain optional close filters.

```json
{
  "action": "FLAT",
  "timestamp": "2026-06-02T08:00:00+00:00",
  "strategy": "my_strategy",
  "symbol": "XAUUSD",
  "market": "FOREX",
  "gateway": "MT5",
  "account_id": "123456"
}
```

### Execution flow

The broker/exchange is always the source of truth: live positions are closed **first**, then the DB is reconciled against what actually closed.

1. Parse the envelope (`AdminMessageSchema`); drop silently on validation error, and dispatch by `action` (`FLAT` below; `BLOCK_SIGNAL`/`ALLOW_SIGNAL` handled separately). An unknown action is logged and ignored.
2. Parse the subject-specific payload — `AdminFlatSchema` (public) or `PrivateAdminFlatSchema` (private, which requires `market`/`gateway`/`account_id`); drop silently on validation error.
3. Route to this worker: on the **public** subject, skip unless the optional `market`/`gateway` filters match; on the **private** subject, skip unless `market`/`gateway`/`account_id` all match this worker's identity. Either way, skip is silent (no log noise).
4. Ensure the broker/exchange connection; abort if unreachable.
5. **Close live positions** (`_close_live_positions_for_flat`): fetch live positions — for a given `symbol` via `get_open_positions(symbol, strategy=…)`, otherwise account-wide via `get_all_open_positions(strategy=…)` — and call `close_single_position(pos, reason="FLAT")` on each. Track every key *attempted* and the subset that *closed successfully*.
6. **Reconcile the DB** (`_reconcile_flat_db`) against the open rows matching the filter. For each DB row:
   - **Matched a successful live close** → update `status → FLATTED`, set `closed_price`/return code, send `⚡ Admin FLAT Closed` Telegram notification.
   - **Never seen live on the broker** (not in the attempted set) → already closed externally → mark `FLATTED` to sync the DB (no order sent).
   - **Close was attempted but failed** → the position is **still live**, so the row is left **OPEN** and flagged loudly for manual attention — the DB never falsely reports a position flat.

## Actions: `BLOCK_SIGNAL` / `ALLOW_SIGNAL`

Toggle whether the worker **executes incoming SIGNALs**. `BLOCK_SIGNAL` suspends signal execution; `ALLOW_SIGNAL` resumes it. Both are **account-scoped**, so they are accepted on the **private subject only** (`ADMIN.<market>.<gateway>.<account_id>`) — the same action arriving on the public `ADMIN` subject is ignored. The payload (`PrivateAdminSignalControlSchema`) requires `market` / `gateway` / `account_id`, and the worker re-validates all three against its own identity before applying the toggle.

While blocked (`_signals_blocked = True`), every incoming SIGNAL is skipped at the single execution funnel `_process_signal` — this covers **both** live signals and the ACK's `retry_signals` replay. Open positions are **untouched**: they can still be closed out-of-band via an `ADMIN` `FLAT`, which is the escape hatch even while signals are blocked. A real state change is logged and sends a `🛑 Signals Blocked` / `✅ Signals Allowed` Telegram notification; repeating the current state is a no-op (no duplicate alert).

The flag is **in-memory only** — a worker restart resets it to *allowed*, and there is no DB/schema change. Durability comes from the broker instead of the worker: it owns the state and replays it in the [`settings` section of the `WORKER_CONNECTED_ACK`](nats-system.md#worker_connected_ack-payload-broker--worker-reply) on every handshake, which re-applies the block through this very same setter before the ACK's signal replay runs.

```json
{
  "action": "BLOCK_SIGNAL",
  "timestamp": "2026-06-02T08:00:00+00:00",
  "market": "FOREX",
  "gateway": "MT5",
  "account_id": "123456"
}
```

## Key Design Decisions

- **Broker is the source of truth (close-first, then reconcile):** A DB row is marked `FLATTED` only when its live close actually succeeded *or* the position was never live on the broker. A row whose close was attempted but *failed* is deliberately left OPEN, so the DB can never claim a position is flat while it is still live.
- **Per-market match key, not symbol-only:** The admin FLAT correlates each live position with its DB row via `_flat_match_key` / `_flat_db_match_keys` — FOREX matches by ticket (checking both `ref_id` and `ref_source_id` so re-ticketed positions still match), CRYPTO by resolved exchange symbol. Two FOREX strategies running the same symbol are therefore handled independently.
- **Graceful handling of already-closed positions:** If a position is tracked in SQLite but no longer live on the broker, it is marked `FLATTED` without sending a close order, so the DB stays consistent even after a connectivity gap.
- **`PositionCDC` propagation:** After status updates, the CDC job picks up the `PENDING` rows and publishes `PositionEvent(event=UPDATED, status=FLATTED, …)` to the Broker via the NATS `TRADE` subject — no special handling required.
- **The FLAT never overwrites the stored entry signal:** `positions.gateway_message` holds the *entry* signal JSON, which `PositionCDC` parses for the `signal_id` / `sl` / `tp1` / `tp2` the broker matches a `TRADE` event back to its own order. The ADMIN payload carries none of those, so it is written to the `position_logs` audit trail instead — where per-event history belongs — and the row's entry signal is left intact. (Overwriting it published an update with `signal_id: null` on both the public and the private subject, which the broker could not correlate, so the order was never updated there.)
