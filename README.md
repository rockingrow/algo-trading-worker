# 🚀 Algo Trading Worker

This is the execution-end of the Event-Driven trading system. It acts as a NATS subscriber waiting for highly structured trading signals from the central Broker, then executes them on the configured market gateway — **FOREX** via the MetaTrader 5 Terminal (Windows) or **CRYPTO** via a centralized exchange such as Binance (any OS). The market is selected by `MARKET_TYPE`, and the two paths share the same NATS/SQLite/notification core through a factory + Template-Method design.

---

## ⚡ Quick Start

### 1. Requirements

Depends on the market:

- **FOREX (`MARKET_TYPE=FOREX`)** must run on **Windows** — the `MetaTrader5` module and the MT5 terminal are Windows-only.
- **CRYPTO (`MARKET_TYPE=CRYPTO`)** is pure Python (REST + websocket) and runs on **any OS** (Linux, macOS, Windows) — no MT5/MetaTrader5 dependency.

### 2. Setup

Because we orchestrate dependencies via `uv`, setup is instantaneous.

```bash
# Creates a virtual environment and installs all dependencies from pyproject.toml
uv sync
```

### 3. Configure .env

Run the interactive initializer — it creates `.env` from `.env.example` (or updates an existing one in place) and walks you through every setting:

```bash
make init
```

- **Defaults everywhere** — each variable shows a default (your current `.env` value if present, otherwise the `.env.example` default); press Enter to keep it.
- **Market-aware** — pick `MARKET_TYPE` (`FOREX` or `CRYPTO`) and it prompts only for the matching credential group: **MT5 connection** for FOREX, or **Crypto CEX / exchange API keys** for CRYPTO.
- **Re-runnable** — existing values are reused as defaults and edited in place (no duplicates); a timestamped `.env.bak-*` backup is written before each update.

The full list of variables and their defaults lives in [`.env.example`](.env.example).

> **FOREX only:** enable **Allow Algo Trading** under *Options → Expert Advisors* in the MetaTrader 5 Terminal before starting the worker.

### 4. Operation

Start the Worker (from the root directory):

```bash
make start
```

The Worker initialises the `worker_data.sqlite` database, starts the market worker, and subscribes to the configured NATS subjects. The worker runs differently per market:

- **FOREX** — in an isolated child **process** (GIL isolation for the MetaTrader5 C extension), supervised by a watchdog. Daemon threads inside it: MT5 health-check, `MT5EventJob` (terminal-close detection), `PositionCDC`, and `NotificationJob`.
- **CRYPTO** — in a background **thread** (the pure-Python gateway needs no process isolation). Daemon threads: the exchange user-data event stream (Binance websocket), `CryptoReconcileJob` (missed-fill safety net), `PositionCDC`, and `NotificationJob`.

You can monitor the logs printed directly to the screen. Both markets start the same way (`make start`); `MARKET_TYPE` decides which gateway is loaded.

---

## 📖 Key Concepts

### Market

The `MARKET_TYPE` environment variable defines which trading market the worker operates on:

| Value | Description |
| --- | --- |
| `FOREX` | Foreign exchange — instruments like `XAUUSD`, `EURUSD`, etc. Execution goes through the **MT5** gateway (Windows only). |
| `CRYPTO` | Cryptocurrency — perpetual futures on instruments like `BTCUSDT`. Execution goes through a **CEX** gateway (any OS). |

The market type drives the entire execution path: which gateway is loaded, which environment variables are required, and whether the worker runs as a subprocess (FOREX) or a background thread (CRYPTO).

### Gateway

A gateway is the integration layer between this worker and the actual trading platform. Each market has its own gateway implementation:

| Market | Gateway | Technology |
| --- | --- | --- |
| `FOREX` | **MT5** (`worker/gateways/forex/mt5/`) | MetaTrader 5 terminal via the `MetaTrader5` C extension. Windows-only. Managed as an isolated child process due to GIL constraints. |
| `CRYPTO` | **CEX** (`worker/gateways/crypto/`) | Centralized Exchange via REST + WebSocket. Exchange-agnostic by design — the first concrete implementation is **Binance** Futures (`worker/gateways/crypto/binance/`). Adding a new exchange only requires implementing `BaseExchangeGateway` and registering it in `ExchangeFactory`. |

The selected gateway handles all order execution, position queries, and real-time event streaming (MT5 terminal events for FOREX; exchange user-data WebSocket for CRYPTO). Business logic above the gateway layer is market-agnostic.

---

## ⏰ Clock sync / NTP requirement (CRYPTO)

Binance rejects any **signed** request whose timestamp runs more than **1000 ms
ahead** of server time (error `-1021`); `recvWindow` only forgives a clock that
is *behind*, so it cannot rescue a fast clock. The worker self-defends — it
measures the offset against Binance at `connect()` and re-syncs + retries once on
a `-1021` — but that is a safety net, not a substitute for a correct host clock.

**Fix the source: keep the host clock disciplined by NTP.**

- **Linux host (VPS / server):** enable a time daemon —
  `sudo timedatectl set-ntp true` (systemd-timesyncd), or install `chrony`
  (`sudo apt install -y chrony && sudo systemctl enable --now chrony`). Verify
  with `timedatectl` / `chronyc tracking`.
- **Windows host:** make sure the Windows Time service is running and synced —
  `w32tm /resync` (and `w32tm /query /status` to verify). Clocks commonly drift
  after the machine sleeps/hibernates, the usual cause of `-1021` in dev.

**Verify:** the worker logs the measured skew at startup —
`[Binance] Clock offset vs server: <X> ms (rtt=… ms)`. After NTP is healthy this
should be a few tens of ms (≈ rtt), not seconds. A large offset there, or
repeated `-1021 … re-syncing` warnings, means the host clock is not disciplined.

---

## 🏗️ System Architecture

```mermaid
graph TD
    TV[TradingView Alert] -- "POST :8080/webhook" --> Broker
    subgraph "Broker Node"
        Broker[FastAPI Webhook Server]
        DB[(PostgreSQL)]
        NATS["NATS Server :4222"]
        Broker -- "Log Signal" --> DB
        Broker -- "Publish" --> NATS
    end
    NATS -- "Subject: SIGNAL" --> W1
    NATS -- "Subject: SIGNAL" --> W2
    NATS -- "Subject: SIGNAL" --> WN
    subgraph W1["Worker — Forex (MT5)"]
        W1A[Signal Handler] --> W1B[(SQLite)]
    end
    subgraph W2["Worker — Forex (MT5)"]
        W2A[Signal Handler] --> W2B[(SQLite)]
    end
    subgraph WN["Worker — Crypto (CEX, e.g. Binance)"]
        WNA[Signal Handler] --> WNB[(SQLite)]
    end
    W1 -- "Subject: TRADE (PositionEvent)" --> NATS
    W2 -- "Subject: TRADE (PositionEvent)" --> NATS
    WN -- "Subject: TRADE (PositionEvent)" --> NATS
    Binance["Binance User Data Stream (ws)"] -- "fills / SL / TP / liquidation" --> WN
```

### Crypto (CEX) integration

`MARKET_TYPE=CRYPTO` selects the crypto worker instead of the MT5 worker. The
crypto path is fully **exchange-agnostic** via the factory pattern and never
imports MetaTrader5 or any `worker.gateways.forex.mt5.*` module:

| Layer | Type | Role |
| --- | --- | --- |
| `worker/gateways/crypto/base.py` | `BaseExchangeGateway` (ABC) | CEX contract: orders, positions, stops, account |
| `worker/gateways/crypto/factory.py` | `ExchangeFactory` | Builds the configured exchange (first: Binance) |
| `worker/gateways/crypto/binance/gateway.py` | `BinanceFuturesGateway` | Binance USDⓈ-M Futures REST via the official `binance_common` transport (signing, retries, rate-limit errors) |
| `worker/gateways/crypto/binance/user_data_stream.py` | `BinanceUserDataStream` | Official-SDK websocket User Data Stream ingesting fills / SL / TP / liquidation / external manual closes (+ pure parser) |
| `worker/gateways/crypto/executor.py` | `CryptoExecutor` | Implements the generic executor protocol over a gateway |
| `worker/gateways/crypto/signal_processor.py` | `CryptoSignalProcessor` | NATS loop + executor/handler + crypto jobs |
| `worker/gateways/crypto/reconcile_job.py` | `CryptoReconcileJob` | Periodic missed-fill reconciler — diffs DB-open rows against live exchange positions; two-scan confirmation before marking closed |

Adding a new exchange = implement `BaseExchangeGateway` + register it in
`ExchangeFactory`. No business-logic change.

Binance specifics use the **official** `binance-sdk-derivatives-trading-usds-futures`
(and its `binance_common` transport): REST calls go through `binance_common.send_request`
(HMAC signing, timestamps, retries/backoff, typed rate-limit errors) and the User
Data Stream uses the SDK websocket (auto-reconnect). All of this lives only inside
`worker/gateways/crypto/binance/` — the gateway abstraction keeps it out of the
executor/strategy/processor, so it never leaks upward.

Required env when `MARKET_TYPE=CRYPTO` (Binance): `BINANCE_API_KEY`,
`BINANCE_API_SECRET`, `BINANCE_ACCOUNT_ID`, and `CRYPTO_QUOTE_ASSET` (defaults to
`USDT`) — enforced by the settings validator. `CRYPTO_EXCHANGE` defaults to
`BINANCE`; `BINANCE_TESTNET` is optional. MT5 credentials are **not** required.
See `.env.example`.

---

## 🧩 FOREX Gateway Module Flow (`worker/gateways/forex/`)

How the FOREX files wire together at runtime. `ForexSignalProcessor` is the hub: it builds the platform gateway via `PlatformFactory` (today an `MT5Gateway` — the **only** MT5-coupled code), wraps it in a platform-agnostic `ForexExecutor` (with `LotSizer` + `StopValidator`), formats messages via `TradeMessagePresenter`, and starts the background jobs. The `MT5Gateway` owns the terminal connection (`bridge`) and symbol resolution (`SymbolResolver`). The handler and market strategy are market-agnostic and shared with CRYPTO. Solid arrows are the live signal path; dotted arrows are composition (who builds/owns whom).

```mermaid
graph TD
    Manager["mt5/manager.py<br/>MT5Manager (parent process)"]

    subgraph Child["Child process — worker/gateways/forex/"]
        SP["signal_processor.py<br/>ForexSignalProcessor — hub"]
        Factory["factory.py<br/>PlatformFactory"]
        Executor["executor.py<br/>ForexExecutor"]
        Presenter["message_presenter.py<br/>TradeMessagePresenter"]
        LotSizer["lot_sizing.py<br/>LotSizer"]
        StopVal["stop_validator.py<br/>StopValidator"]

        subgraph MT5Pkg["mt5/ — MetaTrader 5 adapter"]
            Gateway["gateway.py<br/>MT5Gateway (BasePlatformGateway)"]
            Bridge["bridge.py<br/>MT5 (terminal connection)"]
            Resolver["symbol_resolver.py<br/>SymbolResolver"]
            CloseDet["close_detector.py<br/>scan_terminal_closed_positions"]
        end
    end

    Manager -. "spawns child (market.forex_worker_main)" .-> SP

    %% Composition — signal_processor builds the stack
    SP -. "builds gateway via" .-> Factory
    Factory -. "creates" .-> Gateway
    SP -. "owns" .-> Executor
    SP -. "owns / formats msgs" .-> Presenter
    Executor -. "delegates platform calls to" .-> Gateway
    Executor -. "sizes lot" .-> LotSizer
    Executor -. "validates SL/TP" .-> StopVal
    Gateway -. "owns lifecycle" .-> Bridge
    Gateway -. "resolves symbol" .-> Resolver

    %% Live signal path
    Sub["services/nats_service.py<br/>NATSSubscriber"] -- "SIGNAL / ADMIN" --> SP
    SP -- "handle(signal)" --> Handler["gateways/signal_handler.py<br/>SignalHandler"]
    Handler -- "routes action" --> Strategy["gateways/market_strategy.py<br/>ForexMarket"]
    Strategy -- "open / close / SL<br/>(scoped by strategy magic)" --> Executor
    Gateway -- "order_send" --> Term[(MetaTrader5 Terminal)]
    Bridge -- "connect / account info" --> Term

    %% Background jobs started by the hub
    SP -. "starts" .-> EventJob["jobs/mt5_event_job.py<br/>MT5EventJob"]
    SP -. "starts" .-> CDC["jobs/cdc_job.py<br/>PositionCDC"]
    EventJob -- "poll owned_magics()" --> CloseDet
    CloseDet -- "positions_get / history" --> Term
    EventJob -- "update positions" --> DB[("SQLite<br/>positions")]
    CDC -- "publish PENDING rows" --> Pub["services/nats_service.py<br/>NATSPublisher → TRADE"]
```

---

## 📂 Project Structure

```text
worker/
├── gateways/            # External trading-platform integrations + shared signal pipeline
│   ├── config.py                 # ExecutionConfig value object (risk/volume params)
│   ├── market_strategy.py        # MarketStrategyFactory + ExecutorBackedMarket / Forex / Crypto
│   ├── processor.py              # BaseSignalProcessor — shared NATS loop (Template Method)
│   ├── signal_handler.py         # Routes signals to the correct execution flow
│   ├── forex/           # FOREX — platform-agnostic layer + per-platform adapters (mt6 slots in beside mt5)
│   │   ├── base.py                   # BasePlatformGateway (ABC) + SymbolSpec / Tick / PlatformPosition
│   │   ├── factory.py                # PlatformFactory — selects the forex platform
│   │   ├── executor.py               # ForexExecutor (TradeExecutorProtocol)
│   │   ├── lot_sizing.py             # LotSizer — risk-based lot-size math
│   │   ├── stop_validator.py         # StopValidator — SL/TP distance validation
│   │   ├── message_presenter.py      # TradeMessagePresenter (Telegram strings)
│   │   ├── signal_processor.py       # ForexSignalProcessor — forex hooks over the base
│   │   └── mt5/             # First concrete platform — MetaTrader 5 (Windows only)
│   │       ├── gateway.py            # MT5Gateway (BasePlatformGateway over the MetaTrader5 C extension)
│   │       ├── bridge.py             # MT5 terminal connection lifecycle (initialize/login/shutdown)
│   │       ├── symbol_resolver.py    # SymbolResolver — base → tradeable platform symbol
│   │       ├── close_detector.py     # Terminal-close event scanner (polling)
│   │       └── manager.py            # MT5Manager (alias of WorkerProcessManager)
│   └── crypto/          # CRYPTO — centralized exchanges (CEX); any OS
│       ├── base.py                   # BaseExchangeGateway (ABC) + ExchangePosition
│       ├── executor.py               # CryptoExecutor (TradeExecutorProtocol)
│       ├── factory.py                # ExchangeFactory — selects the CEX
│       ├── message_presenter.py      # CryptoMessagePresenter
│       ├── reconcile_job.py          # CryptoReconcileJob — missed-fill safety-net (two-scan confirmation)
│       ├── signal_processor.py       # CryptoSignalProcessor — crypto hooks over the base
│       └── binance/                  # First concrete exchange
│           ├── gateway.py            # BinanceFuturesGateway (official binance_common transport)
│           └── user_data_stream.py   # BinanceUserDataStream (official-SDK websocket) + parser
├── interfaces/          # Protocol types for dependency inversion
│   ├── db_protocol.py            # Segregated persistence protocols
│   ├── executor_protocol.py      # TradeExecutorProtocol (broker-neutral)
│   ├── mt5_executor_protocol.py  # MT5ExecutorProtocol (legacy alias)
│   ├── mt5_gateway_protocol.py   # Mt5GatewayProtocol (MetaTrader5 surface, used by MT5Gateway)
│   ├── publisher_protocol.py     # MessagePublisherProtocol
│   ├── message_sender_protocol.py # MessageSenderProtocol (notifier send_message contract)
│   └── trade_presenter_protocol.py # TradePresenterProtocol
├── jobs/                # Background polling jobs (daemon threads)
│   ├── cdc_job.py                # PositionCDC — Change Data Capture to NATS TRADE
│   ├── mt5_event_job.py          # MT5EventJob — terminal-close detection (FOREX)
│   └── notification_job.py       # NotificationJob — outbox dispatcher (Telegram retries)
├── schemas/             # Pydantic / dataclass data schemas
│   ├── admin_schema.py           # AdminActionEnum + AdminSignalSchema (NATS ADMIN subject)
│   ├── job_schema.py             # LogAuthorEnum (broker / terminal / exchange)
│   ├── trade_result.py           # TradeResult value object (ok()/fail() factories)
│   ├── metatrader_schema.py      # Back-compat re-export of TradeResult
│   ├── nats_schema.py            # NatsSubjectEnum (SIGNAL, ADMIN, TRADE)
│   ├── notification_schema.py    # NotificationPlatformEnum / NotificationChannelEnum / NotificationModeEnum
│   ├── position_schema.py        # PositionStatusEnum + PositionEvent / PositionEventType
│   └── signal_schema.py          # Signal validation schemas
├── services/            # Infrastructure services
│   ├── db_service.py             # Persistence facade (positions, logs, outbox)
│   ├── nats_service.py           # NATSSubscriber & NATSPublisher (over the NatsClient lifecycle)
│   └── notification_service.py   # TelegramNotification + OutboxNotifier wrapper
├── db/                  # SQLite persistence layer
│   ├── connection.py             # Connection factory
│   ├── repository.py             # SQL + gateway-neutral column ↔ app-domain mapping
│   └── schema.py                 # Table DDL (gateway-neutral columns)
├── utils/               # Shared utilities
│   └── logging.py                # Structured logging helpers
├── app.py               # Application factory, FastAPI lifespan
├── context.py           # WorkerContext — market-agnostic services (DB, notifiers, outbox)
├── logger.py            # Structured logging configuration
├── main.py              # Application entry point
├── market.py            # Gateway orchestrators (FOREX=process, CRYPTO=thread) + worker entry points + factory
├── nats_client.py       # NatsClient — single NATS connection lifecycle in a daemon thread
├── process_manager.py   # WorkerProcessManager — generic subprocess supervisor
├── worker_runtime.py    # run_worker() — shared child-process bootstrap
└── settings.py          # Environment & app configuration (per-market validation)
```

---

## 🧠 Signal Execution Logic

Every incoming signal is parsed into a `SignalSchema` and passed to `SignalHandler.handle()`, which routes it — based on the `action` field — to the correct method on the market-agnostic `BaseMarketStrategy` (`ForexMarket` or `CryptoMarket`). The action-group logic below is identical for both markets; only the concrete executor/gateway underneath differs.

### Action Groups

| Group | Action(s) | Behaviour |
| --- | --- | --- |
| **1 — Entry** | `LONG` / `SHORT` | Force-close any stale position for the same strategy → open a fresh market order with a hard SL set on the broker/exchange server |
| **2 — Partial Exit** | `TP1` | Partial close using `POSITION_TP1_PERCENT` % of live volume (or signal `quantity` if disabled) → move remaining SL to breakeven (`price_open`), unless `TP1_MOVE_SL_TO_BREAKEVEN=false` (then the original entry SL is kept) |
| **3 — Full Exit** | `TP2` / `SL` / `R_SL` | Close ALL open volume using the **actual live `position.volume`** — signal `quantity` is intentionally ignored |
| **4 — Flat** | `FLAT` | Close all `OPENED`/`TP1` positions for the strategy+symbol at market price, marks status `FLATTED` |

#### FLAT Payload (minimal — no price/quantity required)

```json
{
  "strategy": "MT5_GOLD_M5_V1",
  "timestamp": "2026-04-18T21:55:00Z",
  "action": "FLAT",
  "symbol": "XAUUSD"
}
```

---

## 🛡️ ADMIN Subject

The `ADMIN` NATS subject carries out-of-band administrative commands that operate outside the normal strategy signal flow. Messages are received by `BaseSignalProcessor._handle_admin_message`, which is shared by both markets — only *how a live close maps back to a DB row* varies per market (FOREX matches by ticket, CRYPTO by resolved exchange symbol).

### Action: `FLAT`

Closes open positions across one or more strategies/symbols in a single command. Accepts three **optional** filter attributes — any combination can be specified; omitting all three closes every tracked open position on the account.

| Filter | Behaviour when present |
| --- | --- |
| `account_id` | Silently ignored if it does not match the worker's account id (`MT5_LOGIN` for FOREX, `BINANCE_ACCOUNT_ID` for CRYPTO); processed normally if it matches or is absent |
| `strategy` | Restricts close to positions whose `strategy` column equals this value |
| `symbol` | Restricts close to positions for this symbol |

#### ADMIN FLAT Payload

```json
{
  "action": "FLAT",
  "timestamp": "2026-06-02T08:00:00+00:00",
  "strategy": "my_strategy",
  "symbol": "XAUUSD",
  "account_id": "123456"
}
```

All fields except `action` and `timestamp` are optional.

#### Execution flow

The broker/exchange is always the source of truth: live positions are closed **first**, then the DB is reconciled against what actually closed.

1. Parse `AdminSignalSchema`; drop silently on validation error, and ignore any non-`FLAT` action.
2. If `account_id` is present and does not match the worker's account id → skip (no log noise).
3. Ensure the broker/exchange connection; abort if unreachable.
4. **Close live positions** (`_close_live_positions_for_flat`): fetch live positions — for a given `symbol` via `get_open_positions(symbol, strategy=…)`, otherwise account-wide via `get_all_open_positions(strategy=…)` — and call `close_single_position(pos, reason="FLAT")` on each. Track every key *attempted* and the subset that *closed successfully*.
5. **Reconcile the DB** (`_reconcile_flat_db`) against the open rows matching the filter. For each DB row:
   - **Matched a successful live close** → update `status → FLATTED`, set `closed_price`/return code, send `⚡ Admin FLAT Closed` Telegram notification.
   - **Never seen live on the broker** (not in the attempted set) → already closed externally → mark `FLATTED` to sync the DB (no order sent).
   - **Close was attempted but failed** → the position is **still live**, so the row is left **OPEN** and flagged loudly for manual attention — the DB never falsely reports a position flat.

### Key Design Decisions

- **Broker is the source of truth (close-first, then reconcile):** A DB row is marked `FLATTED` only when its live close actually succeeded *or* the position was never live on the broker. A row whose close was attempted but *failed* is deliberately left OPEN, so the DB can never claim a position is flat while it is still live.
- **Per-market match key, not symbol-only:** The admin FLAT correlates each live position with its DB row via `_flat_match_key` / `_flat_db_match_keys` — FOREX matches by ticket (checking both `ref_id` and `ref_source_id` so re-ticketed positions still match), CRYPTO by resolved exchange symbol. Two FOREX strategies running the same symbol are therefore handled independently.
- **Graceful handling of already-closed positions:** If a position is tracked in SQLite but no longer live on the broker, it is marked `FLATTED` without sending a close order, so the DB stays consistent even after a connectivity gap.
- **`PositionCDC` propagation:** After status updates, the CDC job picks up the `PENDING` rows and publishes `PositionEvent(event=UPDATED, status=FLATTED, …)` to the Broker via the NATS `TRADE` subject — no special handling required.

---

## 🧠 Signal Execution — Key Design Decisions

- **Per-strategy position isolation on a shared symbol:** `get_open_positions`, `close_all_positions`, `partial_close_position`, and `update_position_sl` all accept an optional `strategy` parameter that `SignalHandler` populates from `signal.strategy`. This ensures two strategies trading the same symbol (e.g. a Long-only and a Short-only strategy) cannot accidentally touch each other's positions — every entry, exit, and SL update is scoped to the originating strategy. Isolation differs per gateway:

  - **FOREX (magic-based):** Each strategy is assigned a dedicated MT5 magic number via `STRATEGY_MAGIC_MAP`. `open_position` stamps a new order with `_magic_for(signal.strategy)` and `get_open_positions(symbol, strategy)` filters live MT5 positions by that same magic — native MT5-level isolation, no DB lookup. Every FOREX strategy that trades must be mapped (an unmapped strategy raises). Closing operations stamp the closing deal with `pos.magic`, and `owned_magics()` (all mapped values) defines the magics the worker recognises as its own, used by account-wide queries (`get_all_open_positions`) and terminal-close detection (`MT5EventJob`).
  - **CRYPTO (logical):** A centralized exchange holds a single net position per symbol in one-way mode, so there is no magic equivalent (`strategy_code` is `NULL`). Strategy isolation is logical only — enforced by the one-active-per-`(strategy, symbol)` DB invariant and the `strategy` column.

- **Stale position cleanup (Entry):** Before opening any new LONG/SHORT, the handler queries the broker/exchange for stale positions belonging to the *same strategy* on that symbol and force-closes them — leaving positions from other strategies on the same symbol untouched. It then reconciles **every** active (`OPENED`/`TP1`) DB row for that `(strategy, symbol)` to `FORCED_CLOSED`, independently of whether the broker still reported a live position: a prior position may have been closed externally (exchange SL/liquidation, a manual close, or a missed close event), leaving an orphaned `OPENED` row the broker no longer reports. Clearing it is required — otherwise the fresh entry below would collide with the one-active-per-`(strategy, symbol)` unique index on insert and leave the new trade live but untracked. Only then is the new position opened.

- **Data self-healing on inconsistency:** `SignalHandler._get_db_position` enforces the one-active-position-per-(strategy, symbol) invariant at read time. If more than one `OPENED`/`TP1` row is found (possible after a crash before the unique index existed), the oldest row is kept and all extras are immediately marked `FORCED_CLOSED` with an explanatory comment, so the DB self-heals on the next signal rather than silently producing split-brain state.

- **SQLite as source of truth for exit signals:** Before executing any exit action (`TP1`, `TP2`, `SL`, `R_SL`), `SignalHandler` queries the local SQLite `positions` table for a tracked record matching the signal's `strategy + symbol`. If no record is found the signal is rejected — this prevents acting on untracked or already-closed positions. On success, `source_ticket` in the result is always taken from the DB record (not from the live broker ticket) so `_process_message` always updates the correct DB row, even in edge cases where the broker re-tickets a position after a partial close.

- **`source_ticket` Lifecycle Tracking:** The `source_ticket` acts as the unique identifier for a specific trading *position*. When a new trade is opened (Entry), the broker/exchange assigns an ID which becomes the `source_ticket`. When subsequent signals (`TP1`, `TP2`, `SL`, `R_SL`) arrive, they are resolved against the SQLite record to retrieve the original `source_ticket`. For a given trade, the `source_ticket` remains completely constant across its entire lifecycle. This prevents ambiguity across multiple concurrent active trades on different symbols.

- **Ticket-linked partial close (TP1):** The partial close request always carries the original `position=ticket` so MT5 correctly treats it as a partial close rather than an opposing hedge order.

- **TP1 volume — percent-based or signal quantity:** When `VOLUME_DECISION_ENABLED=true`, TP1 closes `POSITION_TP1_PERCENT` % of the current live position volume instead of using `signal.quantity`. The executor's `normalize_volume()` rounds the result to the broker/exchange's quantity step and clamps it to the `[min, max]` range before sending the order.

- **Actual volume on full close (TP2/SL/R_SL):** The signal `quantity` is **never used** for full exit calculations. The handler reads the live `position.volume` directly from the broker/exchange to avoid dust-lot rounding errors.

- **Breakeven SL after TP1 (configurable):** When `TP1_MOVE_SL_TO_BREAKEVEN=true` (default), after the partial close succeeds the executor's `update_position_sl` moves the server-side SL to `price_open` (entry price), protecting the remaining runner against connectivity loss (a `TRADE_ACTION_SLTP` on MT5, a `STOP_MARKET` on a CEX). If the breakeven SL cannot be placed, the still-open runner is immediately emergency-closed rather than left to run unprotected. When `TP1_MOVE_SL_TO_BREAKEVEN=false`, TP1 is partial-close-only: the original entry SL stays in place and keeps protecting the runner, so no breakeven move (and no emergency-close fallback) is attempted.

- **Local Execution Forensics (`worker_data.sqlite`):** To aid in immediate execution debugging and lifecycle tracking natively on the VPS, every processed signal is persisted to the local `position_logs` SQLite table. This audit trail captures the full original NATS JSON message, the target order reference (`ref_id`), the originating reference (`ref_source_id`), and all execution context mapping directly back to the Broker's state.

- **GIL-isolated subprocess (FOREX only):** All MT5 and NATS blocking code runs in a separate OS process (`market.forex_worker_main`, via the shared `worker_runtime.run_worker`). The parent FastAPI process only manages subprocess lifetime via `GatewayProcessOrchestrator` + the generic `WorkerProcessManager` (`MT5Manager` is a thin alias), keeping the event loop fully responsive. The manager accepts a `worker_fn` parameter so the entry point is injectable and testable. **CRYPTO** skips this entirely: the pure-Python gateway runs in a background thread (`ThreadGatewayOrchestrator`) under the app.

- **Dependency-injection boundary — the gateway is the seam:** The `MetaTrader5` module is a native C extension that exposes a single **process-global** connection — `mt5.initialize()` / `mt5.login()` mutate ambient process state, and every `mt5.*` call implicitly targets that one connection. There is no connection *object* to construct or pass around, and because the calls are GIL-blocking the connection is confined to the child process and its daemon threads (the parent FastAPI process never imports the module at all). This dictates where dependency injection actually pays off:

  - **`ForexExecutor` depends only on a `BasePlatformGateway`** (built by `PlatformFactory`), never on MetaTrader5. The agnostic order logic — translating a signal into an order, lot-size math (`LotSizer`), stop validation (`StopValidator`) — runs against the gateway contract, so the whole order layer is unit-testable with a fake gateway and runs off-Windows. This mirrors `CryptoExecutor` over `BaseExchangeGateway`.
  - **`MT5Gateway` (the concrete adapter) takes an injected `mt5_api: Mt5GatewayProtocol`.** Its order/data methods (`place_order` / `close_position` / `modify_sl` / `get_symbol_spec` / `get_tick` / `get_positions`) hold the only `mt5.*` call-sites in the order path, so production injects the live `MetaTrader5` module while unit tests inject a `FakeMt5`. Symbol resolution (`SymbolResolver`) takes the same injected handle.
  - **`bridge.py` and `close_detector.py` deliberately call the module-global `MetaTrader5` directly.** `bridge` *owns* the singleton's lifecycle (`initialize` / `login` / `shutdown`), and `close_detector` is a free-function scanner that runs **inside the `MT5EventJob` daemon thread**, reading the very same process-global connection. There is no FastAPI-level composition root in those background threads to thread an injected handle down from, and "injecting" a singleton that can only ever have one real instance would be ceremony with no testability payoff.

- **Hard SL vs. NATS SL — callback gap (mitigated by MT5EventJob):** When a LONG/SHORT is opened, the SL is registered directly on the MT5 server (`request["sl"]`), so MT5 will auto-close the position even if the NATS signal pipeline is delayed. If the hard SL fires before the NATS `SL` signal arrives, `_handle_full_close` finds no open position, returns `success=False`, and no event is published. `MT5EventJob` closes this gap by detecting the disappearance independently and updating the SQLite `positions` table, which then triggers `PositionCDC` to publish the `TRADE` event to the Broker.

- **Notification outbox (store-and-forward):** In-process notification calls (`ctx.notifier` and `ctx.channel_notifier`) do **not** hit the Telegram API directly — they enqueue a row in the SQLite `notifications` table via `OutboxNotifier`. A separate `NotificationJob` daemon thread drains the table every 1 s and performs the actual HTTP send, retrying failed messages with exponential backoff (`5s → 30s → 2m → 10m`) up to `max_attempts` (default `5`). This decouples signal handling from Telegram's availability/latency and prevents Telegram outages from blocking the NATS event loop. **Startup/shutdown banners** are sent **directly** via `ctx.direct_notifier` (bypassing the outbox) so the user sees them immediately — even before the DB/notification dispatcher is ready or after they are torn down.

### MT5EventJob — Terminal-Close Polling (`worker/jobs/mt5_event_job.py`)

`MT5EventJob` runs as a daemon thread inside the child process alongside the NATS signal loop and the MT5 health-check thread. Its sole purpose is to detect positions that the MT5 server closed autonomously — without a corresponding NATS signal reaching the pipeline in time.

#### How it works

Every 5 seconds the job calls `scan_terminal_closed_positions()` (`worker/gateways/forex/mt5/close_detector.py`), which:

1. Calls `mt5.positions_get()` filtered by **every magic number this worker owns** (all `STRATEGY_MAGIC_MAP` values, via `ForexExecutor.owned_magics()`) → `current_tickets`
2. Diffs against an internal `seen_tickets` set maintained across polls
3. For each ticket that disappeared, calls `mt5.history_deals_get(position=ticket)` to find the closing deal
4. Reads `deal.reason` to classify the closure:

| MT5 `deal.reason` | `TerminalCloseReason` | Action |
| --- | --- | --- |
| `DEAL_REASON_SL` | `SL` | DB updated → `PositionCDC` publishes TRADE event |
| `DEAL_REASON_TP` | `TP` | DB updated → `PositionCDC` publishes TRADE event |
| `DEAL_REASON_SO` | `STOP_OUT` | DB updated → `PositionCDC` publishes TRADE event |
| `DEAL_REASON_CLIENT` / `MOBILE` / `WEB` | `MANUAL` | Telegram only — no DB update (ambiguous intent) |
| `DEAL_REASON_EXPERT` | *(skipped)* | none — closed by our own `order_send` |

1. For terminal-initiated closures, emits a `TerminalClosedEvent` dataclass carrying: `source_ticket`, `deal_ticket`, `symbol`, `close_reason`, `close_price`, `close_volume`, `close_time`, `entry_price`, `sl`, `tp`, and an `AccountSnapshot`.

On each event the job then:

- **4.1** Sends a Telegram notification with full position and account context.
- **4.2** Writes a row to `position_logs` with `author = 'terminal'` (vs `'broker'` for NATS-triggered rows) and updates `positions.status → TERMINAL_CLOSED`. `PositionCDC` then picks up the pending row and publishes a `TRADE` event to the Broker via NATS.

#### Resilience during MT5 reconnect

| MT5 state | `positions_get()` result | `seen_tickets` | Events fired |
| --- | --- | --- | --- |
| Connected | valid list | updated each scan | normal |
| Disconnected | `None` | **preserved** | none — scan skipped |
| Reconnected | valid list | diff against preserved state | catch-up for any SL/TP that fired during outage |

When `positions_get()` returns `None` (terminal offline), the scan is skipped and `seen_tickets` is left untouched. This prevents false-positive "position closed" events and ensures that once MT5 comes back online, any positions that were closed during the outage are detected on the next scan.

The job shares the process-level `stop_event` (`multiprocessing.Event`) with the health-check thread and the NATS loop, so it shuts down cleanly as part of the normal worker lifecycle — no independent restart mechanism is needed.

### PositionCDC — NATS Trade Publisher (`worker/jobs/cdc_job.py`)

`PositionCDC` implements Change Data Capture on the local SQLite `positions` table. It polls every 2 seconds for rows whose `sync_status` is `PENDING` (set automatically on insert or update), serialises them as `PositionEvent` messages, and publishes them to the NATS `TRADE` subject. After a successful publish the row is marked `PUBLISHED`.

#### PositionEvent fields

| Field | Source |
| --- | --- |
| `event` | `CREATED` (first sync) or `UPDATED` (subsequent syncs) |
| `account_id` | Worker account id — `MT5_LOGIN` (FOREX) or `BINANCE_ACCOUNT_ID` (CRYPTO) |
| `account_name` | `MT5_NAME` (FOREX) or `BINANCE_ACCOUNT_ID` (CRYPTO) |
| `market_type` | `MARKET_TYPE` from settings (e.g. `forex`, `crypto`) |
| `account_balance` / `account_leverage` | Snapshot from the gateway's account (`account_info_fn`) at poll time (CRYPTO reports balance only; leverage is `null`) |
| `signal_id`, `sl`, `tp1`, `tp2`, `risk_percent`, `magic` | Extracted from the original signal JSON stored in `positions.gateway_message` |

The Broker handler is expected to be idempotent (upsert by `account_id + ticket`), so at-least-once delivery is safe even if the worker restarts mid-publish.

### NotificationJob — Telegram Outbox Dispatcher (`worker/jobs/notification_job.py`)

`NotificationJob` is the worker side of the notification outbox pattern. It polls the SQLite `notifications` table every 1 second and dispatches due rows via the appropriate Telegram sender, keeping Telegram I/O completely off the NATS event loop.

#### Routing

The job is constructed with a `{channel → TelegramNotification}` map built by `WorkerContext`:

| `channel` value | Maps to | Backing chat IDs |
| --- | --- | --- |
| `INDIVIDUAL` | `ctx.direct_notifier` | `TELEGRAM_CHAT_ID` (management) |
| `COMMUNITY` | `ctx.direct_channel_notifier` | `TELEGRAM_CHAT_CHANNEL_ID` (comma-separated signal channels) |

In-process callers stay decoupled: they hold an `OutboxNotifier` whose `send_message()` just inserts a row tagged with the right `channel` and `category` — the dispatcher does the routing.

#### Retry semantics

| Outcome | Action |
| --- | --- |
| Success (HTTP 200 for every chat ID) | `DELETE` the row (hard-delete; no `status` column required) |
| Partial / total failure | `attempts += 1`, set `next_attempt_at = now + backoff[attempts]`, store `last_error` |
| `attempts >= max_attempts` (default `5`) | Row becomes dead-letter — skipped by the polling query and left in place for inspection |

Backoff schedule (indexed by attempt, capped at last): **5s → 30s → 2m → 10m**. The polling query is `WHERE attempts < max_attempts AND (next_attempt_at IS NULL OR next_attempt_at <= now)` and is served by the `idx_notifications_pending` index.

#### Direct-send escape hatches

Two notification paths intentionally bypass the outbox:

1. **Startup / shutdown banners** sent via `ctx.direct_notifier` — must surface even before `NotificationJob` is running or after it has been torn down.
2. **NATS connection events** still go through the outbox via `ctx.nats_enqueue`, but they use the `NATS_EVENT` category so they can be filtered out of analytics if desired.

### ForexExecutor Primitives (`worker/gateways/forex/executor.py`)

`ForexExecutor` implements the broker-neutral `TradeExecutorProtocol` (the same surface `CryptoExecutor` provides), delegating every platform call to its injected `BasePlatformGateway`:

| Method | Used By |
| --- | --- |
| `open_position(signal)` | Entry (Group 1) |
| `partial_close_position(symbol, close_volume, position_ticket?, strategy?)` | TP1 (Group 2) |
| `update_position_sl(symbol, new_sl, position_ticket?, strategy?)` | TP1 breakeven update |
| `close_all_positions(symbol, reason, strategy?)` | Full exit (Group 3) & broker FLAT |
| `close_single_position(pos, reason)` | Admin FLAT — closes one position by position object |
| `get_open_positions(symbol, strategy?)` | Pre-flight guard in all groups |
| `get_all_open_positions(strategy?)` | Admin FLAT — all positions, by strategy |
| `convert_quantity_to_lots(symbol, quantity)` | Entry & TP1 volume calc |
| `normalize_volume(symbol, volume)` | Rounds/clamps volume to broker lot step & limits |

Risk-based entry sizing (`calculate_lot_size`) and lot-step rounding live in the `LotSizer` collaborator; `_resolve_entry_volume` invokes it internally during `open_position`.

---

## 🗄️ SQLite Schema (`worker_data.sqlite`)

Three tables are created on startup by `db_init()` using WAL journal mode. There are no runtime migrations — the schema is created from scratch (drop the sqlite file to recreate).

The `positions` / `position_logs` columns are **gateway-neutral** because the same tables back both markets (FOREX/MT5 and CRYPTO/CEX). `worker/db/repository.py` is the single boundary that maps these physical columns to the application-domain names callers use (and parses the TEXT ref ids ↔ `int`), so the NATS `PositionEvent` contract and consumers are unaffected:

| DB column (generic) | App-domain key | Notes |
| --- | --- | --- |
| `ref_id` | `ticket` | TEXT in DB, `int` in app |
| `ref_source_id` | `source_ticket` | TEXT in DB, `int` in app |
| `strategy_code` | `magic` | INTEGER (MT5 magic; `NULL` for crypto) |
| `gateway_return_code` | `mt5_retcode` | INTEGER broker/exchange status code |
| `gateway_message` | `message` | Raw signal/event JSON |

### `positions` — Live position state

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

#### Constraints

A partial unique index `uidx_positions_one_active_per_strategy_symbol` enforces at most one `OPENED` or `TP1` row per `(strategy, symbol)` pair. Closed/force-closed rows are unrestricted. The index is created together with the table (`CREATE UNIQUE INDEX IF NOT EXISTS`).

#### Position Status Lifecycle

```text
OPENED ──► TP1 ──► TP2
       │         └──► SL
       │         └──► R_SL
       │         └──► TERMINAL_CLOSED  (MT5EventJob: SL/TP/Stop-Out fired server-side)
       │         └──► FORCED_CLOSED    (new entry signal arrived while position open)
       └──► FLATTED                    (FLAT signal)
```

| Status | Set by | Meaning |
| --- | --- | --- |
| `OPENED` | Entry signal | Position is live |
| `TP1` | TP1 signal | Partially closed; runner is still active |
| `TP2` | TP2 signal | Fully closed at take-profit 2 |
| `SL` | SL signal | Fully closed at stop-loss (NATS-triggered) |
| `R_SL` | R_SL signal | Fully closed at revised stop-loss |
| `TERMINAL_CLOSED` | `MT5EventJob` / exchange event | Broker/exchange closed the position (SL/TP/Stop-Out/manual) before a NATS signal arrived |
| `FORCED_CLOSED` | New entry signal / liquidation | Position was force-closed (opposing/same-direction entry, or a crypto liquidation) |
| `FLATTED` | FLAT signal | Position was closed by an administrative flat command |

### `position_logs` — Immutable execution audit trail

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

### `notifications` — Telegram outbox (store-and-forward)

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

---

## 🔄 Process Lifecycle & Watchdog

How the worker is hosted depends on the market (`create_market_orchestrator`):

- **FOREX** — a child **process** (the parent FastAPI process never loads the MT5 C extension), supervised by a watchdog:

  ```text
  FastAPI (parent)
    └── GatewayProcessOrchestrator (FOREX)
          ├── WorkerProcessManager.start()  — spawns child process
          ├── Watchdog task                 — checks every 10 s (WATCHDOG_INTERVAL)
          │     └── if child died → WorkerProcessManager.restart()
          └── WorkerProcessManager.stop()   — on FastAPI shutdown
  ```

- **CRYPTO** — no GIL isolation needed (pure-Python gateway), so it runs in a background **thread** under the FastAPI app (`ThreadGatewayOrchestrator`, same watchdog/restart contract as FOREX), launched the same way via `make start`.

Daemon threads running alongside the NATS message loop:

| Thread | Markets | Interval | Purpose |
| --- | --- | --- | --- |
| `forex-health` | FOREX | 15 s (`MT5_HEALTH_INTERVAL`) | Checks MT5 connection; sends Telegram alert on disconnect/reconnect, and relaunches/restarts the terminal when needed |
| `MT5EventJob` | FOREX | 5 s | Detects terminal-side position closes (SL/TP/Stop-Out) |
| `binance-user-stream` | CRYPTO | push | Websocket user-data stream → exchange-side fills / SL / TP / liquidation |
| `crypto-reconcile` | CRYPTO | 45 s | `CryptoReconcileJob` — polls live positions; reconciles DB-open rows that are exchange-flat on two consecutive scans (missed-fill safety net) |
| `PositionCDC` | both | 2 s | Publishes `PENDING` position rows to NATS `TRADE` subject |
| `NotificationJob` | both | 1 s | Drains the `notifications` outbox and dispatches Telegram messages (with exponential-backoff retries) |

All threads share the same `stop_event` — a `multiprocessing.Event` for FOREX, a `threading.Event` for CRYPTO — and exit cleanly when it is set.
