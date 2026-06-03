# Changelog

## [Unreleased]

### Fixed

- **Per-strategy position isolation on a shared symbol.** When two strategies traded the same symbol at once (e.g. a Long-only and a Short-only strategy), the live MT5 layer filtered positions only by `magic` + `symbol`, so an entry/exit signal for one strategy would fetch — and force-close — the other strategy's position too. The owning strategy is now stamped into the MT5 position `comment` (`"<strategy>|<action>"`, see `worker/mt5/position_comment.py`) and all position queries/closes (`get_open_positions`, `close_all_positions`, `partial_close_position`, `update_position_sl`) accept a `strategy` filter that `SignalHandler` scopes to `signal.strategy`. A new signal now acts only on its own strategy's position.

---

## [1.0.0] — 2026-06-02

### Overview

First stable release of **Algo Trading Worker** — the execution-end of an event-driven trading system. The worker subscribes to a NATS broker, receives structured trading signals, and executes them directly into a MetaTrader 5 terminal running on Windows.

---

### Architecture

- **Event-driven subprocess model** — All MT5 and NATS blocking I/O runs in an isolated child process spawned by `MT5Manager`. The parent FastAPI process never loads the MT5 C extension and stays fully responsive via a watchdog task that auto-restarts the child on crash.
- **Three NATS subjects** — `SIGNAL` (inbound trading signals), `ADMIN` (out-of-band commands), `TRADE` (outbound position events back to broker).
- **SQLite-backed state** — Three tables: `positions` (live trade state + CDC source), `position_logs` (immutable audit trail), `notifications` (Telegram outbox).

---

### Signal Execution (NATS `SIGNAL` subject)

Every inbound signal is validated into `SignalSchema` and dispatched by `SignalHandler` across four action groups:

| Group | Actions | Behaviour |
| --- | --- | --- |
| Entry | `LONG` / `SHORT` | Force-close any stale position → open market order with hard server-side SL |
| Partial Exit | `TP1` | Close `POSITION_TP1_PERCENT` % of live volume → move remaining SL to breakeven |
| Full Exit | `TP2` / `SL` / `R_SL` | Close all lots at actual MT5 `position.volume` (signal `quantity` ignored) |
| Broker FLAT | `FLAT` | Close all `OPENED`/`TP1` positions for the strategy+symbol pair |

**Key design decisions:**

- Stale cleanup before every entry guarantees a clean slate and prevents accidental hedging.
- SQLite is the source of truth for all exit signals — unknown or already-closed positions are rejected before any MT5 order is sent.
- `source_ticket` is immutable across the full position lifecycle, surviving partial-close re-ticketing.
- Full-exit volume is always read from the live MT5 position to avoid dust-lot rounding errors.

---

### Admin Commands (NATS `ADMIN` subject)

#### `FLAT`

Closes open positions across one or more strategies/symbols via a single administrative command. All filter fields are optional — omitting all three closes every tracked open position on the account.

| Filter | Effect |
| --- | --- |
| `account_id` | Skipped silently if it does not match `MT5_LOGIN` |
| `strategy` | Restricts close to positions for that strategy |
| `symbol` | Restricts close to positions for that symbol |

Positions tracked in SQLite but already gone from MT5 are marked `FLATTED` immediately without sending an order. Each successfully closed position triggers an `⚡ Admin FLAT Closed` Telegram notification and a `PositionCDC` publish to the broker.

---

### Background Jobs (daemon threads inside child process)

| Thread | Interval | Purpose |
| --- | --- | --- |
| `mt5-health` | 15 s | Detects MT5 disconnect; auto-reconnects / restarts `terminal64.exe` |
| `MT5EventJob` | 5 s | Detects server-side closes (SL/TP/Stop-Out) and syncs DB + publishes TRADE event |
| `PositionCDC` | 2 s | Publishes `PENDING` position rows to NATS `TRADE` as `PositionEvent` |
| `NotificationJob` | 1 s | Drains Telegram outbox with exponential-backoff retries (`5s → 30s → 2m → 10m`) |

---

### Notifications (Telegram)

- All in-process notifications go through an SQLite outbox (`OutboxNotifier`) decoupled from the NATS event loop.
- Two channels: `INDIVIDUAL` (operator management) and `COMMUNITY` (signal broadcast channels).
- `SILENT` notification mode suppresses `COMMUNITY` channel only; `INDIVIDUAL` is always delivered.
- Startup/shutdown banners bypass the outbox and are sent directly so they surface even before the dispatcher is running.
- All order-related messages include: Symbol, Strategy, Action, Price, Volume, Ticket, Source Ticket.

---

### Position Status Lifecycle

```text
OPENED ──► TP1 ──► TP2
       │         └──► SL
       │         └──► R_SL
       │         └──► TERMINAL_CLOSED   (MT5EventJob)
       │         └──► FORCED_CLOSED     (new entry signal)
       └──► FLATTED                     (FLAT signal — broker or admin)
```

---

### Risk Management

- `VOLUME_DECISION_ENABLED` — When enabled, lot size is derived from capital + risk % + SL distance instead of signal `quantity`.
- `USE_ACCOUNT_EQUITY` — Optionally uses live account equity as the capital base.
- `POSITION_TP1_PERCENT` — Configurable partial-close percentage for TP1.
- Volume is always normalised to the broker's lot step and clamped to `[min_lot, max_lot]` before any order is sent.
