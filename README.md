# 🚀 Algo Trading Worker

The execution end of an event-driven trading system. The worker subscribes to
trading signals from the central Broker over NATS, executes them on a real
trading platform, records every position in a local SQLite database, and
publishes the result back to the Broker.

Two markets share one pipeline, selected by `MARKET_TYPE`:

| Market | Gateway | Runs on |
| --- | --- | --- |
| `FOREX` | MetaTrader 5 terminal (`worker/gateways/forex/mt5/`) | **Windows only**, in its own OS process |
| `CRYPTO` | Binance USDⓈ-M futures (`worker/gateways/crypto/binance/`) | Any OS, in a background thread |

## How it works

1. The Broker publishes a `SIGNAL` (`LONG` / `SHORT` / `TP1` / `TP2` / `SL` / `R_SL` / `FLAT`) on NATS.
2. The worker validates it, applies the entry guards, and executes it on the gateway.
3. The outcome is stored in SQLite, the source of truth for every open position.
4. A change-data-capture job publishes each position change back on the `TRADE` subject, and a
   background job sends the Telegram notifications.

Background safety nets reconcile SQLite against the live broker, so a missed close or fill is
repaired rather than left stuck. See [Architecture](docs/architecture.md) for the full picture.

## Quick start

**Requirements:** Python ≥ 3.10 and [`uv`](https://docs.astral.sh/uv/). FOREX additionally needs
Windows with the MetaTrader 5 terminal installed and **Allow Algo Trading** enabled under
*Options → Expert Advisors*.

```bash
uv sync      # create the virtual environment and install dependencies
make init    # interactive .env bootstrap
make start   # run the worker
```

`make init` creates `.env` from [`.env.example`](.env.example), or updates an existing one in
place. It offers a default for every variable, prompts only for the credentials of the chosen
`MARKET_TYPE`, and writes a timestamped `.env.bak-*` backup before each update.

`make start` initialises `worker_data.sqlite`, starts the market worker and subscribes to the
configured NATS subjects. Logs are printed to the console.

> [!WARNING]
> This worker places **real orders**. Use the Binance testnet (`CRYPTO_TESTNET=true`) or a demo
> MT5 account until you have verified your configuration.

## Documentation

| Topic | Page |
| --- | --- |
| System overview, project structure, process lifecycle and watchdog | [docs/architecture.md](docs/architecture.md) |
| Settings and environment variables | [docs/configuration.md](docs/configuration.md) |
| Signal actions, entry sizing, scale-in, cycle notifications | [docs/signal-execution.md](docs/signal-execution.md) |
| Design decisions: strategy isolation, entry guards, exposure cap | [docs/design-decisions.md](docs/design-decisions.md) |
| `ADMIN` subject: `FLAT`, `BLOCK_SIGNAL` / `ALLOW_SIGNAL` | [docs/nats-admin.md](docs/nats-admin.md) |
| `SYSTEM` subject: `WORKER_CONNECTED` handshake, runtime updates | [docs/nats-system.md](docs/nats-system.md) |
| Background jobs: close detection, reconciliation, CDC, notifications | [docs/background-jobs.md](docs/background-jobs.md) |
| SQLite schema and position lifecycle | [docs/database.md](docs/database.md) |
| FOREX / MetaTrader 5 gateway | [docs/forex.md](docs/forex.md) |
| CRYPTO / Binance gateway, clock sync, position mode, leverage | [docs/crypto.md](docs/crypto.md) |
| Example payload for every NATS subject | [examples/nats/](examples/nats/subjects.md) |
| Release history | [changelog.md](changelog.md) |

## Development

```bash
make install-dev   # uv sync --dev
make test          # uv run pytest
make lint          # uv run ruff check .
make format        # uv run ruff format .
make dev           # run with auto-reload
```

The `MetaTrader5` package is Windows-only. On Linux or macOS, skip the suite that imports it:

```bash
uv run pytest --ignore=tests/gateways/forex/mt5/test_close_detector.py
```

Contributor and coding-agent rules live in [AGENTS.md](AGENTS.md).

## License

[MIT](LICENSE)
