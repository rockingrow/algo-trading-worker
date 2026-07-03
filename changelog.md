# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.5] - Unreleased

### Added

- **Telegram error-log forwarding** — When `TELEGRAM_ENABLED` and `TELEGRAM_LOG_ERRORS_ENABLED` are both set, log records at `ERROR` level or above are forwarded to Telegram. `TelegramLogHandler` (a `logging.Handler`) formats each record and hands it to a bounded queue drained by a background **thread** that performs the synchronous `send_message`, so `emit` never blocks the caller (FastAPI event loop, FOREX child process, or a daemon thread) or raises. A thread is used rather than an asyncio task because the FOREX processor runs in a child process with no event loop; the handler is process-aware and (re)starts its worker per PID so a forked child forwards through its own thread. Three safeguards keep it production-safe: a filter drops records emitted by the send path itself (no feedback loop), identical messages are suppressed within `TELEGRAM_LOG_DEDUP_WINDOW` seconds (no spam), and the queue is bounded, dropping records under an error storm rather than growing unbounded.
- **Dedicated log bot/chat** — `TELEGRAM_LOG_BOT_TOKEN` and `TELEGRAM_LOG_CHAT_ID` route forwarded error logs through a bot and private chat kept separate from the main signal bot, so an outage or ban on one never affects the other. Both fall back to `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` when left empty.
- Scale-in (averaging) position support on the SIGNAL path. The webhook/NATS SIGNAL payload now carries an `is_scale_position` boolean and a nested `scaling` object (`tp`, `sl`, `quantity`). The broker pre-applies these multipliers, so `sl`/`tp1`/`tp2`/`quantity` arrive already scaled and the worker uses them verbatim for order entry, notifications, and persistence. The only re-derivation is in risk-based sizing: when `VOLUME_DECISION_ENABLED=true` the worker sizes the entry from risk/capital/SL (ignoring `signal.quantity`), so it re-applies `scaling.quantity` to that self-computed volume. `scaling.tp`/`scaling.sl` are never re-applied; non-scale signals and an absent `scaling.quantity` are no-ops.
- Entry notifications now render a "Scaled Position" block (TP1/TP2, plus SL for forex) when a signal is flagged `is_scale_position`, displaying the broker's already-scaled targets.

### Changed

- Refactored the Telegram message presenters to remove duplication. The market-agnostic messages (`signal_rejected`, `position_unprotected_closed`) and the shared divider now live in a new `BaseMessagePresenter` (`worker/gateways/message_presenter.py`) that both the forex and crypto presenters inherit (DRY). Renamed `TradeMessagePresenter` to `ForexMessagePresenter` for naming consistency with `CryptoMessagePresenter`.

## [1.1.4] - Unreleased

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

[Unreleased]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.3...dev
[1.1.5]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.4...dev
[1.1.4]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.3...dev
[1.1.3]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/rockingrow/algo-trading-worker/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/rockingrow/algo-trading-worker/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/rockingrow/algo-trading-worker/releases/tag/v1.0.0
