# Changelog

## [1.1.0] — 2026-06-14

### Added

- **Periodic position reconciler for CRYPTO (`CryptoReconcileJob`).** The exchange user-data stream is the primary source of close events, but it can miss a fill — a websocket reconnect gap, a handler exception, or worker downtime while an SL/TP/liquidation triggers — leaving the DB row stuck `OPENED`/`TP1` forever (CRYPTO had no equivalent of the FOREX `close_detector`). A new daemon polls live exchange positions and, when a DB-open position no longer exists on the exchange, marks it `TERMINAL_CLOSED` (best-effort mark price + reconciled comment) and notifies the operator, so a missed event self-heals. Safety: a row must be DB-open *and* exchange-flat on **two consecutive scans** before it reconciles (absorbs the lag between an entry fill and `positionRisk` reflecting it), and a failed live-position fetch skips the scan entirely so an API blip can never be read as "everything is flat".
- **Crypto (CEX) market via the factory pattern — Binance first.** A new `MARKET_TYPE=CRYPTO` runs the worker against a centralized exchange instead of MT5. The integration is exchange-agnostic: business logic depends only on `BaseExchangeGateway` (ABC), and `ExchangeFactory` builds the configured exchange from `CRYPTO_EXCHANGE`. Adding an exchange = implement the gateway + register it; no call-site changes. First adapter: `BinanceFuturesGateway` (USDⓈ-M Futures, HMAC-SHA256 signed REST). Order service via `CryptoExecutor` (implements the broker-neutral `TradeExecutorProtocol`); risk-based or payload-quantity sizing; reduce-only closes; breakeven-after-TP1 as a `STOP_MARKET`.
- **Exchange event ingestion (`BinanceUserDataStream`).** A websocket User Data Stream job (the optimal, push-based mechanism) ingests fills / stop-loss / take-profit / liquidation and reconciles the DB + notifies — the CRYPTO counterpart of `MT5EventJob`. Frame parsing is a pure, unit-tested function (`parse_order_trade_update`); the listenKey is kept alive automatically.
- **`TradeResult` value object** (`worker/schemas/trade_result.py`) with `ok()` / `fail()` factories, replacing the scattered `{"success": ..., "retcode": ..., "comment": ...}` dict literals across both executors, the gateway, strategy, and handler. Keeps dict-style read/write access for backward compatibility; `metatrader_schema.TradeResult` re-exports it.
- **Docker for the crypto worker.** `Dockerfile` (Linux, `python:3.12-slim` + `uv`, non-root) and `docker-compose.yml` (NATS + crypto worker, persistent SQLite volume). The container runs the worker as a **single process** via `python -m worker.crypto_worker` — no FastAPI/uvicorn and no multiprocessing child (see below) — with SIGTERM → graceful shutdown and a heartbeat-file healthcheck. `MetaTrader5` is skipped on Linux via its platform marker, so the image carries no Forex dependency (and pulls no FastAPI/uvicorn at runtime). New `make docker-*` targets. MT5/FOREX is intentionally not containerized (Windows + terminal required).
- **`market_type` column** on `positions` / `position_logs`, and an `exchange` log author for CEX-triggered closes.

### Fixed

- **Stopped rounding persisted prices to 2 decimals.** `PositionRepository` rounded `price`/`opened_price`/`closed_price` to 2 dp (a forex-era assumption), corrupting low-priced crypto (e.g. SHIB ~0.00002345 → `0.00`) in the DB and every downstream `PositionEvent`, and was inconsistent with `sl`/`tp1` (already stored raw). Prices are now persisted as the broker supplies them (already quantized to the instrument tick).

### Removed

- **Dropped the Docker deployment for the crypto worker.** Deleted `Dockerfile`, `docker-compose.yml`, `.dockerignore`, and the `make docker-*` targets. The containerized stack bundled its own NATS, which silently created a *second* NATS server: a local Broker publishing to the host NATS never reached the worker subscribed to the compose-internal `nats://nats:4222`. Both markets now deploy the same way — `make start`, with `MARKET_TYPE` selecting the gateway — so a single NATS is shared by the Broker and all workers.
- **Removed the standalone crypto entry point.** `worker/crypto_worker.py` no longer defines `main()` / `_start_heartbeat()` (the container-only single-process runner + heartbeat-file liveness). It now mirrors `mt5_worker.py` — just `crypto_worker_main`, the binding used by the FastAPI thread orchestrator. Dropped the corresponding heartbeat tests.
- **Deleted `worker/crypto_worker.py` and `worker/forex_worker.py`.** Both were thin shims that only bound their processor to `run_worker`. The functions `crypto_worker_main` and `forex_worker_main` have been consolidated into `worker/market.py` (alongside the orchestrator classes that call them), making the dedicated entry-point files redundant. All internal cross-references (`worker_runtime.py` docstring, `gateways/forex/signal_processor.py` docstring, `README.md`, and tests) updated to reflect the new canonical location.

### Changed

- **Gateway-neutral database columns** (no migration — PROD has no data, the DB is recreated). `positions` / `position_logs` columns renamed so both markets share them: `magic → strategy_code`, `mt5_retcode → gateway_return_code`, `message → gateway_message`, `ticket → ref_id`, `source_ticket → ref_source_id`. `ref_id` / `ref_source_id` are stored as TEXT (any gateway's id format fits); `worker/db/repository.py` is the single boundary that maps columns ↔ application-domain names and parses TEXT↔int. The NATS `PositionEvent` contract and all consumers are unchanged.
- **`BaseSignalProcessor` (Template Method).** Extracted the shared signal-processor skeleton (NATS loop, `connect`/`shutdown`/`run`/`start_market_jobs`, signal persistence, notifications, `PositionCDC` wiring) into `worker/core/base_signal_processor.py`. `Mt5SignalProcessor` and `CryptoSignalProcessor` now implement only broker-specific `@abstractmethod` hooks (`_build_executor`, `_connect_broker`, `_account_footer`, `_magic_for`, `_start_broker_jobs`, `_handle_admin_message`, …).
- **Unified worker bootstrap & orchestration.** A single `run_worker()` (`worker/worker_runtime.py`) drives every worker lifecycle; `mt5_worker_main` / `crypto_worker_main` only bind their processor.
- **CRYPTO runs single-process (no multiprocessing).** The multiprocessing child exists to isolate the GIL-holding MetaTrader5 C extension, which the pure-Python crypto gateway does not need. FOREX keeps the child **process** (`GatewayProcessOrchestrator` + `WorkerProcessManager`); CRYPTO now runs in a background **thread** under the FastAPI app (`ThreadGatewayOrchestrator`) or as a **standalone single process** in the container (`python -m worker.crypto_worker`). This removes a process layer and keeps the crypto container minimal.
- **`MarketStrategyFactory.create(market_type, executor, config)`** now takes `market_type` explicitly (Dependency Injection) instead of reading the global `settings` singleton.
- **Per-market credential validation.** MT5 credentials are required only for FOREX; the exchange API keys only for CRYPTO — so each deployment carries (and initializes) only its own market's dependencies. The CRYPTO path imports no MetaTrader5 / `worker.gateways.mt5.*` (verified).
- **Repository structure.** MT5 and crypto integrations grouped under `worker/gateways/{mt5,crypto}/`; the SQLite layer lives in `worker/db/` (`schema.py`, `repository.py`, `connection.py`).

### Notes

- New settings: `CRYPTO_EXCHANGE`, `CRYPTO_QUOTE_ASSET`, `BINANCE_API_KEY`, `BINANCE_API_SECRET`, `BINANCE_TESTNET`. MT5 settings (`MT5_SERVER` / `MT5_LOGIN` / `MT5_PASSWORD`) are now optional at the field level (validated only for FOREX).
- Removed the standalone `MAGIC_NUMBER` env var from the docs (the worker uses `STRATEGY_MAGIC_MAP` exclusively; every FOREX strategy that trades must be mapped).

---

## [1.0.1] — 2026-06-03

### Added

- **Per-strategy magic numbers (`STRATEGY_MAGIC_MAP`).** A new optional env var maps strategy names to dedicated MT5 magic numbers (JSON object, e.g. `{"SCALP": 20260001, "SWING": 20260002}`). `MT5Executor` resolves orders and position filters through `_magic_for(strategy)`: `open_position` stamps a new order with the strategy's magic, and `get_open_positions(symbol, strategy)` filters by that same magic — giving native MT5-level isolation **without a DB lookup** for any mapped strategy. Strategies absent from the map continue to share the base `MAGIC_NUMBER` and fall back to the DB `strategy`-column filter. Closing deals now inherit `pos.magic`, and `owned_magics()` (base + all mapped values) drives account-wide queries (`get_all_open_positions`) and terminal-close detection (`MT5EventJob` / `scan_terminal_closed_positions` now track the full owned-magic set).

### Fixed

- **Per-strategy position isolation on a shared symbol.** When two strategies traded the same symbol at once (e.g. a Long-only and a Short-only strategy), the live MT5 layer filtered positions only by `magic` + `symbol`, so an entry/exit signal for one strategy would fetch — and force-close — the other strategy's position too. Position queries/closes (`get_open_positions`, `close_all_positions`, `partial_close_position`, `update_position_sl`) now accept a `strategy` filter that `SignalHandler` scopes to `signal.strategy`. Strategy membership is resolved either by the strategy's dedicated magic (when present in `STRATEGY_MAGIC_MAP`) or against the authoritative `strategy` column in the positions table (`get_open_positions_by_strategy`). A new signal now acts only on its own strategy's position.
- **ADMIN FLAT cross-strategy closure bug.** Fixed an issue where executing a `FLAT` action on a specific strategy could accidentally close positions for other strategies if the payload omitted the `symbol` or if a dangerous DB-fallback triggered. `get_all_open_positions` now accepts and strictly enforces a `strategy` filter natively via the MT5 magic number, and the obsolete fallback logic has been removed.

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
