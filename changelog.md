# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.4] - Unreleased

### Fixed

- Binance user data stream stopped receiving events after the `listenKey` expired. On keepalive failure the stream only retried every 30s without reconnecting, so once Binance dropped the `listenKey` (~60 min after a sustained failure) no further `ORDER_TRADE_UPDATE` events arrived. Now error `-1125` (listenKey gone) triggers an immediate reconnect, and any other keepalive error reconnects after 5 consecutive failures (~2.5 min) to recover from transient 403s (IP restriction / permission changes).

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
[1.1.4]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.3...dev
[1.1.3]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/rockingrow/algo-trading-worker/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/rockingrow/algo-trading-worker/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/rockingrow/algo-trading-worker/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/rockingrow/algo-trading-worker/releases/tag/v1.0.0
