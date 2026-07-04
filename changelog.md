# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.6] - Unreleased

### Added

- `tp1_percent` and `move_sl_to_be` fields on `SignalSchema` — signals can now carry a per-trade TP1 close percentage and a move-SL-to-breakeven flag. The broker encodes both in the payload; the worker reads them at TP1 time and combines them with the config overrides below.
- `USE_CUSTOM_POSITION_TP1_PERCENT` (bool, default `false`) env var — when `true`, always uses `POSITION_TP1_PERCENT` from config regardless of `signal.tp1_percent`; when `false`, prefers `signal.tp1_percent` and falls back to `POSITION_TP1_PERCENT` when the signal omits it.
- `USE_CUSTOM_RISK_PERCENTAGE` (bool, default `false`) env var — when `true`, always uses `RISK_PERCENTAGE` from config; when `false`, prefers `signal.risk_percent` and falls back to the config value when the signal omits or zeroes it.
- `TP1_MOVE_SL_TO_BREAKEVEN` is now optional (previously a required `bool`). When absent (the new default), the signal's own `move_sl_to_be` field governs whether the stop is moved to breakeven after TP1. When explicitly set in config, it overrides the signal.
- Order fill notifications now show: the effective risk percent with a gear icon when the config overrides the signal; the TP1 close-percent appended to the volume/quantity line on TP1 fills; and an override-settings block listing VOLUME_DECISION_ENABLED, RISK_PERCENTAGE, USE_ACCOUNT_EQUITY, POSITION_TP1_PERCENT, and TP1_MOVE_SL_TO_BREAKEVEN states (ENABLED/DISABLED with value) so operators can see, on each trade notification, which modes are active for the connected worker.
- Startup banner now renders all override settings as ENABLED/DISABLED (consistent with the order fill block) instead of raw boolean values.
- CRYPTO: `LeverageInitJob` — one-shot, sequential per-symbol leverage initialisation. For each symbol in `CRYPTO_LEVERAGE_INIT_SYMBOLS` the job calls `gateway.get_max_leverage` (Binance: `GET /fapi/v1/leverageBracket`) to read the account's exchange-side ceiling — which honours sub-account / VIP caps automatically — then calls `gateway.set_leverage` (Binance: `POST /fapi/v1/leverage`) to set the symbol to `min(exchange_max, MAX_LEVERAGE_CAP)`. A sub-account capped at 5x lands on 5; an unrestricted account lands on the configured cap. A failed lookup skips the symbol (never blindly applies the cap), and any crash inside the job is logged and swallowed. The job is not run at worker startup; it runs only on-demand via a `SYSTEM` `CRYPTO_LEVERAGE_INIT` message (see below), which the broker normally sends event-driven right after this worker announces `WORKER_CONNECTED`, and which may override the symbol set (`symbols`) and cap (`default_leverage`, applied as `min(exchange_max, default_leverage)`) for that one run.
- `CRYPTO_LEVERAGE_INIT_SYMBOLS` (comma-separated, default empty) env var — raw signal symbols whose leverage the init job must walk (e.g. `BTCUSDT.P,ETHUSDT.P` or `BTCUSD,ETHUSD`); resolved through the executor's symbol resolver so it mirrors how upstream signals address the symbol. An empty list skips the init entirely.
- `MAX_LEVERAGE_CAP` (int, default `10`) env var — upper bound applied by `LeverageInitJob`. A non-positive cap skips the init with a warning.
- `BaseExchangeGateway.get_max_leverage` / `set_leverage` hooks (default no-op) — added to the agnostic CEX contract so future exchanges plug in without touching the job.
- New `SYSTEM` NATS subject for operational/maintenance commands that drive worker-side actions outside the trade-signal flow (no order placed). Messages are received by `BaseSignalProcessor._handle_system_message`, which validates the common envelope and dispatches to the market-specific `_handle_system_action` hook; an action a market does not understand is logged and ignored. `SYSTEM` is now one of `NATS_REQUIRED_LISTENING_SUBJECTS`, so every worker subscribes to it regardless of `NATS_SUBJECTS`.
- `SystemActionEnum` / `SystemSchema` / `SystemCryptoLeverageInitSchema` (`worker/schemas/system_schema.py`) — the SYSTEM envelope (`action`, a required `account_id` addressing the message to one specific worker, `timestamp` defaulting to now) and the `CRYPTO_LEVERAGE_INIT` payload (`symbols`, `default_leverage`).
- Example payload `examples/signals/admin.crypto_leverage_init.json`.
- Worker → broker handshake on the `SYSTEM` subject: right after a worker connects to NATS it sends a `WORKER_CONNECTED` **request** (`nc.request`, not a fire-and-forget publish) announcing its identity (`account_id`, `market`, `gateway`), so the broker always replies on a private inbox instead of silently dropping the message if it's briefly unreachable. The broker's reply is one of `CRYPTO_LEVERAGE_INIT` (config attached — routed through the normal `_handle_system_action` hook, applied exactly as if received on the subscription), `WORKER_CONNECTED_ACK` (no init needed, handshake complete), or `WORKER_CONNECTED_ERROR` (broker-side problem — logged with `reason` for operator attention, not retried). A request timeout (5s, no reply at all) is retried with backoff (5s → 10s → 20s, capped) indefinitely — the handshake is idempotent on the broker and gates trading (crypto needs `default_leverage` before it's safe to size an order), so the worker blocks here rather than falling back to running without config. The handshake waits for the SYSTEM subscription to be confirmed live first (`NATSSubscriber.wait_subscribed`) because NATS core does not replay: a reply that arrived before we subscribe would be lost. It is re-sent on every NATS reconnect (`NATSPublisher(on_reconnect=…)`, now dispatched off the publisher's event-loop thread since the handshake blocks) so a broker/NATS restart re-triggers init. `SystemActionEnum.WORKER_CONNECTED` / `WORKER_CONNECTED_ACK` / `WORKER_CONNECTED_ERROR` and `SystemWorkerConnectedSchema` / `SystemWorkerConnectedAckSchema` / `SystemWorkerConnectedErrorSchema` added. The worker still keeps its normal `SYSTEM` subscription for `CRYPTO_LEVERAGE_INIT` as a fallback, so an older broker (still fire-and-forget) or an older worker (not yet upgraded) keeps working — the two sides can roll out independently.
- `NATSPublisher.request(subject, data, timeout)` — thread-safe synchronous request/reply on top of the publisher's own event loop (`asyncio.run_coroutine_threadsafe` + `nc.request`), used by the `WORKER_CONNECTED` handshake above.
- Example payloads `examples/signals/system.worker_connected.json`, `examples/signals/system.worker_connected_ack.json`, `examples/signals/system.worker_connected_error.json`.
- CRYPTO: `MIN_LEVERAGE_CAP` (int, default `5`) env var — a last-resort floor for `LeverageInitJob`. When `set_leverage` hits a `-4421` account-level cap but the real ceiling **cannot be parsed** out of the error message (e.g. Binance reworded it and the regex no longer matches), the gateway retries once at `min(MIN_LEVERAGE_CAP, target)` instead of abandoning the symbol at its dangerous exchange default. Threaded through `BaseExchangeGateway.set_leverage(symbol, leverage, min_leverage_cap=None)` and `LeverageInitJob`. A non-positive value disables the fallback. If the account is restricted below the floor, the retry still fails and the symbol is logged for manual fixing.

### Changed

- Worker identity `account_id` format is now `<market>-<gateway>-<account_id>` (was `<market>-<account_id>`) — e.g. `CRYPTO-BINANCE-7654321`, `FOREX-MT5-413652379`. The gateway segment lets the broker route/identify by exchange without a separate lookup. Derived in `Settings._validate_market_requirements`; surfaced as the NATS connection name and as the `SYSTEM` routing key.
- CRYPTO: `BinanceFuturesGateway._leverage_cap_from_error` now (a) logs a warning when a `-4421` is received but its message does not match the cap parser — previously this silently dropped the retry path, hiding a Binance message reword — and (b) clamps the parsed ceiling to the requested leverage, discarding parse anomalies (a `-4421` is by definition a restriction *below* what was requested).
- `BaseSignalProcessor._handle_system_message` now logs the parsed `action` and `account_id` before dispatching, so a received SYSTEM message (including a worker's own `WORKER_CONNECTED` echoed back on its own subscription) is always traceable in the logs even when there is no handler for it.

### Removed

- `NatsSubjectEnum.ACK` — unused NATS subject. `NATSSubscriber`/`NATSPublisher` `publish_subjects` are now `[TRADE, SYSTEM]` (was `[TRADE, ACK]` / `[TRADE, ACK, SYSTEM]`).

### Fixed

- `PositionCDC` was calling `row.get("message")` to extract signal fields for TRADE events, but the DB column is `gateway_message`. As a result, `signal_id`, `sl`, `tp1`, `risk_percent`, and other fields published in the NATS TRADE event were always `null`. Now correctly reads `gateway_message` and forwards it as `message` in the PositionEvent payload.
- TP1 fill notification could show a `(TP1 x%)` close-percent that did not match the volume actually closed: it ignored `VOLUME_DECISION_ENABLED` (appending a percent even when the close was sized from `signal.quantity`) and skipped the config fallback when the signal omitted `tp1_percent`. The suffix now mirrors `_resolve_tp1_params` exactly — dropped when volume-decision sizing is off, and falling back to `POSITION_TP1_PERCENT` when the signal carries no percent.
- Override-settings lines (RISK_PERCENTAGE, POSITION_TP1_PERCENT) rendered a literal `None%` when an override was toggled on but its percent was never configured; they now read `n/a`.
- Worker crashed on startup with `SettingsError: error parsing value for field "crypto_leverage_init_symbols"` whenever `CRYPTO_LEVERAGE_INIT_SYMBOLS` was set to its documented comma form (e.g. `BTCUSDT,ETHUSDT`): pydantic-settings JSON-decodes `list[str]` fields from the dotenv source *before* the `mode="before"` split validator runs, and a bare comma string is not valid JSON. Both `crypto_leverage_init_symbols` and `telegram_chat_channel_id` are now annotated `Annotated[list[str], NoDecode]`, so the raw env string is handed straight to their split validators. `TELEGRAM_CHAT_CHANNEL_ID` previously only survived because its single quoted value happened to be valid JSON; the unquoted multi-channel form documented in `.env` would have hit the same crash.
- `NATSPublisher.__init__` stored the `on_reconnect` constructor argument on `self._on_reconnect`, shadowing the class's own `_on_reconnect` async callback method. Since `NatsClient(reconnected_cb=self._on_reconnect)` is built after that assignment, it picked up the shadowed value instead of the real handler, silently breaking the "re-announce `WORKER_CONNECTED` on NATS reconnect" behaviour. Renamed the stored argument to `self._on_reconnect_callback`.

## [1.1.5] - 2026-06-26

### Added

- **Telegram error-log forwarding** — When `TELEGRAM_ENABLED` and `TELEGRAM_LOG_ERRORS_ENABLED` are both set, log records at `ERROR` level or above are forwarded to Telegram. `TelegramLogHandler` (a `logging.Handler`) formats each record and hands it to a bounded queue drained by a background **thread** that performs the synchronous `send_message`, so `emit` never blocks the caller (FastAPI event loop, FOREX child process, or a daemon thread) or raises. A thread is used rather than an asyncio task because the FOREX processor runs in a child process with no event loop; the handler is process-aware and (re)starts its worker per PID so a forked child forwards through its own thread. Three safeguards keep it production-safe: a filter drops records emitted by the send path itself (no feedback loop), identical messages are suppressed within `TELEGRAM_LOG_DEDUP_WINDOW` seconds (no spam), and the queue is bounded, dropping records under an error storm rather than growing unbounded.
- **Dedicated log bot/chat** — `TELEGRAM_LOG_BOT_TOKEN` and `TELEGRAM_LOG_CHAT_ID` route forwarded error logs through a bot and private chat kept separate from the main signal bot, so an outage or ban on one never affects the other. Both fall back to `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` when left empty.
- Scale-in (averaging) position support on the SIGNAL path. The webhook/NATS SIGNAL payload now carries an `is_scale_position` boolean and a nested `scaling` object (`tp`, `sl`, `quantity`). The broker pre-applies these multipliers, so `sl`/`tp1`/`tp2`/`quantity` arrive already scaled and the worker uses them verbatim for order entry, notifications, and persistence. The only re-derivation is in risk-based sizing: when `VOLUME_DECISION_ENABLED=true` the worker sizes the entry from risk/capital/SL (ignoring `signal.quantity`), so it re-applies `scaling.quantity` to that self-computed volume. `scaling.tp`/`scaling.sl` are never re-applied; non-scale signals and an absent `scaling.quantity` are no-ops.
- Entry notifications now render a "Scaled Position" block (TP1/TP2, plus SL for forex) when a signal is flagged `is_scale_position`, displaying the broker's already-scaled targets.

### Changed

- Refactored the Telegram message presenters to remove duplication. The market-agnostic messages (`signal_rejected`, `position_unprotected_closed`) and the shared divider now live in a new `BaseMessagePresenter` (`worker/gateways/message_presenter.py`) that both the forex and crypto presenters inherit (DRY). Renamed `TradeMessagePresenter` to `ForexMessagePresenter` for naming consistency with `CryptoMessagePresenter`.

### Fixed

- Binance user data stream stopped receiving events after the `listenKey` expired. On keepalive failure the stream only retried every 30s without reconnecting, so once Binance dropped the `listenKey` (~60 min after a sustained failure) no further `ORDER_TRADE_UPDATE` events arrived. Now error `-1125` (listenKey gone) triggers an immediate reconnect, and any other keepalive error reconnects after 5 consecutive failures (~2.5 min) to recover from transient 403s (IP restriction / permission changes).
- CRYPTO: TP2 resting order was never placed on entry — `open_position()` registered the stop-loss but silently skipped the take-profit even when `tp2` was present in the signal. Positions ran without a resting TP target until the next SL move.
- CRYPTO: TP2 resting order was wiped when the stop moved to breakeven — `update_position_sl()` called `cancel_all_orders()` which cleared both the old SL and the still-valid TP2. Now the original TP2 price is recovered from the persisted entry signal and re-placed immediately after the new breakeven stop.
- CRYPTO: A new entry signal could silently cancel another strategy's SL/TP orders for the same symbol — `cancel_all_orders()` is symbol-scoped, not strategy-scoped, so the stale-position cleanup in `_handle_entry()` would wipe a concurrent strategy's resting orders before the netting-conflict guard in the executor had a chance to reject the entry. The guard now runs at the top of `_handle_entry()`, before any exchange call is made.

## [1.1.3] - 2026-06-19

### Fixed

- `strategy_code` published as `"0"` instead of `null` for CRYPTO positions — `_magic_for` in `PositionCDC` returned `0` as default when a strategy had no entry in the map, causing DB-stored `NULL` to be serialised incorrectly.
- CRYPTO gateway ignored `STRATEGY_MAGIC_MAP` setting — `_position_cdc_kwargs` hardcoded `strategy_magic_map={}`, so a configured mapping had no effect on CRYPTO workers. Now reads from settings, consistent with the FOREX gateway.

## [1.1.2] - 2026-06-17

### Fixed

- Added certifi>=2026.0.0 to solve problem related to Windows Server 2022 cannot run normally

### Changed

- Use APP_HOST and APP_PORT from .env instead of hardcoded values
- Auto-quote sensitive env vars in init_env script
- Corrected syntax in chore updates

### Security

- Enhanced environment variable handling for sensitive configuration

## [1.1.1] - Previous Release

## [1.1.0] - Previous Release

## [1.0.1] - Previous Release

## [1.0.0] - Previous Release

[1.1.6]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.5...dev
[1.1.5]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.4...v1.1.5
[1.1.4]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.3...dev
[1.1.3]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/rockingrow/algo-trading-worker/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/rockingrow/algo-trading-worker/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/rockingrow/algo-trading-worker/releases/tag/v1.0.0
