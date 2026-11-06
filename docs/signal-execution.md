# Signal Execution Logic

Every incoming signal is parsed into a `SignalSchema` and passed to `SignalHandler.handle()`, which routes it — based on the `action` field — to the correct method on the market-agnostic `BaseMarketStrategy` (`ForexMarket` or `CryptoMarket`). The action-group logic below is identical for both markets; only the concrete executor/gateway underneath differs.

## Action Groups

| Group | Action(s) | Behaviour |
| --- | --- | --- |
| **1 — Entry** | `LONG` / `SHORT` | Force-close any stale position for the same strategy → open a fresh market order with a hard SL set on the broker/exchange server. Gated by the broker-supplied `max_open_orders` cap: an entry that would exceed the open-position cap is rejected (status `REJECTED`) and never sent to the broker. Also rejected when **another** strategy already holds the symbol, unless `FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL` is on (FOREX only), and when **this** strategy already holds it, unless `FOREX_ALLOW_MULTI_POSITIONS_PER_SYMBOL` is on and the entry carries a new `signal_uxid` — see [Key Design Decisions](design-decisions.md#signal-execution--key-design-decisions). |
| **2 — Partial Exit** | `TP1` | Partial close using the resolved TP1 percent of live volume (or signal `quantity` when `VOLUME_DECISION_ENABLED=false`) → optionally move remaining SL to breakeven (`price_open`). TP1 percent and breakeven flag are resolved from the signal (`tp1_percent`, `move_sl_to_be`) or overridden by config (`USE_CUSTOM_POSITION_TP1_PERCENT` / `TP1_MOVE_SL_TO_BREAKEVEN`). |
| **3 — Full Exit** | `TP2` / `SL` / `R_SL` | Close ALL open volume using the **actual live `position.volume`** — signal `quantity` is intentionally ignored |
| **4 — Flat** | `FLAT` | Close all `OPENED`/`TP1` positions for the strategy+symbol at market price, marks status `FLATTED` |

### Entry Sizing Mode — `USE_ACCOUNT_EQUITY` vs `position.use_equity_sizing`

An entry is sized in one of two ways: **equity sizing** (the worker computes the volume itself from risk % / SL against the account's *real* equity, and the payload's `quantity` is ignored) or **payload quantity** (the volume is `signal.quantity` as sent).

Which one applies is resolved per signal, highest priority first:

| Priority | Source | Value | Effect |
| --- | --- | --- | --- |
| 1 | `USE_ACCOUNT_EQUITY` (.env) | `true` | Equity sizing — live account equity is the risk base, `quantity` ignored |
| 1 | `USE_ACCOUNT_EQUITY` (.env) | `false` | Payload quantity — the entry uses `signal.quantity` |
| 2 | `position.use_equity_sizing` (payload) | `true` / `false` | Same meaning as above; consulted **only** when `USE_ACCOUNT_EQUITY` is unset |
| 3 | neither is set | — | Legacy behaviour: `VOLUME_DECISION_ENABLED` decides, and risk sizing uses the fixed `CAPITAL` |

`USE_ACCOUNT_EQUITY` is therefore **tri-state**: leave it out of `.env` (or comment it out) to let each position decide for itself; set it to `true`/`false` to override every incoming signal from the worker side. The startup banner reflects this — `USE_ACCOUNT_EQUITY: PER SIGNAL` when unset, `ENABLED` / `DISABLED` when set.

When equity sizing is on but the account equity cannot be read, the entry falls back to the broker's **minimum** lot/qty rather than silently sizing off `CAPITAL`.

```json
{
  "strategy": "MT5_GOLD_M5_V1",
  "action": "LONG",
  "symbol": "XAUUSD",
  "price": 2000.0,
  "quantity": 1.0,
  "sl": 1990.0,
  "use_equity_sizing": true
}
```

With `USE_ACCOUNT_EQUITY` unset, the payload above ignores `quantity: 1.0` and sizes the lot from the live account equity, the resolved risk percent and the effective SL.

### Scale-In (Averaging) Positions

A signal may carry an optional `is_scale_position` boolean and a nested `scaling` object (`tp`, `sl`, `quantity`). **The broker has already applied these multipliers** — `sl`, `tp1`, `tp2`, and `quantity` arrive as their final, scaled values. The `scaling` object is passed through as metadata only. The worker therefore uses the payload's SL/TP/quantity **verbatim** for order entry, notifications, and persistence; it never re-scales them.

The one exception is risk-based sizing. When the worker sizes the entry itself (see [Entry Sizing Mode](#entry-sizing-mode--use_account_equity-vs-positionuse_equity_sizing)), it computes the entry volume from risk/capital-or-equity/SL and **ignores** `signal.quantity`. To keep a scale-in entry proportional, the executor re-applies `scaling.quantity` to that self-computed volume (sizing is linear in risk, so the lot/qty scales by the same factor). `scaling.tp`/`scaling.sl` are never re-applied.

| Mode | What sizes the entry | Where `scaling.quantity` applies |
| --- | --- | --- |
| Payload-quantity mode | `signal.quantity` (already broker-scaled) | Not re-applied — used as-is |
| Risk sizing (equity or `CAPITAL` base) | risk % / capital-or-equity / SL → computed volume | Re-applied to the computed volume |

A signal without `is_scale_position: true` is passed through untouched (any `scaling` object is ignored), and an absent `scaling.quantity` leaves the computed volume unchanged.

```json
{
  "strategy": "MT5_GOLD_M5_V1",
  "timestamp": "2026-04-18T21:55:00Z",
  "action": "LONG",
  "symbol": "XAUUSD",
  "price": 2000.0,
  "quantity": 1.0,
  "sl": 1791.0,
  "tp1": 2222.0,
  "tp2": 2255.0,
  "is_scale_position": true,
  "scaling": { "tp": 1.1, "sl": 0.9, "quantity": 2.0 }
}
```

With the payload above, the worker executes against the SL/TP/quantity exactly as sent (`tp1=2222.0`, `tp2=2255.0`, `sl=1791.0`, `quantity=1.0`). In payload-quantity mode it enters `1.0`; in risk-sizing mode it computes the lot from risk and then multiplies by `scaling.quantity` (`2.0`).

### FLAT Payload (minimal — no price/quantity required)

```json
{
  "strategy": "MT5_GOLD_M5_V1",
  "signal_uxid": "9f2c4b7e18a3d605",
  "timestamp": "2026-04-18T21:55:00Z",
  "action": "FLAT",
  "symbol": "XAUUSD"
}
```

## Signal-cycle notifications

Two ids travel on every signal and they answer different questions:

| Field | Scope | Used for |
| --- | --- | --- |
| `signal_id` | This one signal — unique per action | **De-duplication**: a signal seen live and again in the ACK replay is recognised by this id |
| `signal_uxid` | The whole trade — the entry and every TP/SL/FLAT that follows share one | **Correlation**: echoed on outbound `TRADE` events so the broker can gather this worker's executions under one cycle, and used locally to fold every action of a trade into a single Telegram message |

`signal_uxid` is optional, so a broker that predates the field keeps working — those signals simply fall back to one message per action. When it is present, the worker reports the whole trade in **one broadcast message that it edits in place**, laid out as three boxes:

```
[⏳ OPENED]                        ← headline: icon + the position's latest status
📈 LONG XAUUSD                     ┐
----------------------------------  │ Box 1 — position: what the trade is, at
Price: 2340.15 | Volume: 0.5 lot ⚙️ │ what price/size, with which levels, and
SL: 2330 | TP1: 2350 | TP2: 2360    │ what the trade has booked so far
Risk: 3% ⚙️ | Ticket: 778899        │ (summed over every close — omitted while
Realized: +154.65 📈                ┘ nothing has closed yet).
----------------------------------
📡 Actions                         ┐
----------------------------------  │ Box 2 — the timeline, one entry per
✅ LONG — Filled                    │ action, appended as the trade moves.
Price: 2340.15 | Volume: 0.5 lot ⚙️ │ The icon is the *outcome*, so a TP1
2026-04-10 22:55:00 (UTC)           │ the broker refused never looks like a
                                    │ TP1 that hit. A close also reports the
🎯 TP1 — Filled                     │ PnL it booked, in the same format the
Price: 2350.4 | Volume: 0.25 lot (TP1 50%)
PnL: +51.25 📈                      │ standalone close message used. Every
2026-04-11 15:05:00 (UTC)          ┘ time carries its zone (see below).
----------------------------------
⚙️ Settings                        ┐
----------------------------------  │ Box 3 — the worker configuration this
VOLUME_DECISION_ENABLED: ENABLED    │ trade ran under (same wording as the
RISK_PERCENTAGE: ENABLED (3.0%)     │ startup banner), which cycle it is,
...                                 │ and the live account footer.
----------------------------------  │
Strategy: MT5_GOLD_M5_V1            │
Signal: 9f2c4b7e18a3d605            ┘
```

The headline status is **the position's**, not the action's: a TP1 the broker refused leaves an `OPENED` position `OPENED`, and once a position is closed (`TP2`/`SL`/`R_SL`/`FLATTED`/…) a late or replayed action can never advertise it as live again. An admin `FLAT` closes out the position's own cycle rather than opening a new message.

Each chat entry keeps its `_<topic id>` suffix, so a cycle lands in the same forum topic as every other notification, and two topics of the same group each own their own message.

Every action line in box 2 ends with the time that action happened **and the zone it is quoted in** — `2026-04-11 15:05:00 (UTC)`. Times are stored in UTC; `TELEGRAM_MESSAGE_TIMEZONE` (a UTC offset in **hours**, default `0`) chooses the zone they are rendered in, and the label follows it: `7` → `2026-04-11 22:05:00 (UTC+7)`, `-4` → `(UTC-4)`, `5.5` → `(UTC+5:30)`. Set it to the zone the broker reports in and both reports of the same trade read off one clock. An offset that is unset, non-numeric or outside ±14h falls back to UTC rather than failing the render, and a stored timestamp that carries no offset is read as UTC — which is what the worker writes.

Delivery, retries and the `TELEGRAM_CYCLE_ENABLED` toggle are covered in [CycleNotificationJob](background-jobs.md#cyclenotificationjob--one-telegram-message-per-trade-workerjobscycle_notification_jobpy).
