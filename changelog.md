# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.6] - 2026-06-28

### Added

- `tp1_percent` and `move_sl_to_be` fields on `SignalSchema` — signals can now carry a per-trade TP1 close percentage and a move-SL-to-breakeven flag. The broker encodes both in the payload; the worker reads them at TP1 time and combines them with the config overrides below.
- `USE_CUSTOM_POSITION_TP1_PERCENT` (bool, default `false`) env var — when `true`, always uses `POSITION_TP1_PERCENT` from config regardless of `signal.tp1_percent`; when `false`, prefers `signal.tp1_percent` and falls back to `POSITION_TP1_PERCENT` when the signal omits it.
- `USE_CUSTOM_RISK_PERCENTAGE` (bool, default `false`) env var — when `true`, always uses `RISK_PERCENTAGE` from config; when `false`, prefers `signal.risk_percent` and falls back to the config value when the signal omits or zeroes it.
- `TP1_MOVE_SL_TO_BREAKEVEN` is now optional (previously a required `bool`). When absent (the new default), the signal's own `move_sl_to_be` field governs whether the stop is moved to breakeven after TP1. When explicitly set in config, it overrides the signal.
- Order fill notifications now show: the effective risk percent with a gear icon when the config overrides the signal; the TP1 close-percent appended to the volume/quantity line on TP1 fills; and an override-settings block listing VOLUME_DECISION_ENABLED, RISK_PERCENTAGE, USE_ACCOUNT_EQUITY, POSITION_TP1_PERCENT, and TP1_MOVE_SL_TO_BREAKEVEN states (ENABLED/DISABLED with value) so operators can see, on each trade notification, which modes are active for the connected worker.
- Startup banner now renders all override settings as ENABLED/DISABLED (consistent with the order fill block) instead of raw boolean values.

### Fixed

- `PositionCDC` was calling `row.get("message")` to extract signal fields for TRADE events, but the DB column is `gateway_message`. As a result, `signal_id`, `sl`, `tp1`, `risk_percent`, and other fields published in the NATS TRADE event were always `null`. Now correctly reads `gateway_message` and forwards it as `message` in the PositionEvent payload.
- TP1 fill notification could show a `(TP1 x%)` close-percent that did not match the volume actually closed: it ignored `VOLUME_DECISION_ENABLED` (appending a percent even when the close was sized from `signal.quantity`) and skipped the config fallback when the signal omitted `tp1_percent`. The suffix now mirrors `_resolve_tp1_params` exactly — dropped when volume-decision sizing is off, and falling back to `POSITION_TP1_PERCENT` when the signal carries no percent.
- Override-settings lines (RISK_PERCENTAGE, POSITION_TP1_PERCENT) rendered a literal `None%` when an override was toggled on but its percent was never configured; they now read `n/a`.

## [1.1.5] - 2026-06-26

### Added

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
