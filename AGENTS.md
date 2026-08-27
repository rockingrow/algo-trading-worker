# Repository Instructions

Shared instructions for every coding agent working in this repository. Codex
reads this file directly; Claude Code reads it through the `@AGENTS.md` import
at the top of `CLAUDE.md`. **Keep shared content here** — anything specific to
one tool goes in that tool's own file, so the two never drift apart.

Follow a more specific `AGENTS.md` in a subdirectory when one exists.

## The project in five lines

Market execution worker. It subscribes to the broker's NATS subjects, executes
`LONG`/`SHORT`/`TP1`/`TP2`/`SL`/`R_SL`/`FLAT` on a real trading platform, records
every position in local SQLite, and publishes the result back on the `TRADE`
subject. Two markets share one pipeline: **FOREX** over MetaTrader 5 (Windows
only, runs in its own OS process) and **CRYPTO** over a CEX (Binance USDⓈ-M
futures, any OS). Python ≥3.10, `uv`, FastAPI host process.

## Rules

Binding on every change.

1. **Commits**: only when the user asks. English, imperative, 72-char subject at
   most. Match the trailer convention of the recent commits on the branch.
2. **English** for comments, docstrings, identifiers, log and exception strings,
   and every committed Markdown file.
3. **Pull requests always target `dev`**, never `master`. Opening the PR is the
   user's call.
4. **A user-facing change updates `changelog.md`** under an unreleased version
   heading, and `README.md` when it changes behaviour the README describes
   (settings, payload fields, execution logic, schema). Both files are part of
   the change, not a follow-up.
5. **A new setting is added in five places**: `worker/settings.py`,
   `.env.example`, `scripts/init_env.py`, the key list in
   `tests/test_settings_validation.py`, and — if execution consumes it —
   `ExecutionConfig` in `worker/gateways/config.py`. A setting missing from any
   of them is a setting that silently does nothing in a child process.
6. **Never commit secrets or local state**: `.env`, credentials, tokens,
   `worker_data.sqlite`, `logs/`. They are git-ignored; keep them that way.

## Working approach

- Read the relevant source, tests, configuration and documentation before
  editing.
- Inspect `git status` first. Preserve every unrelated change and untracked file
  in the working tree.
- Make the smallest coherent change that solves the problem and matches the
  existing architecture.
- Do not add or upgrade production dependencies unless the task requires it; say
  why when you do.

## Commands

```bash
make install-dev    # uv sync --dev
make test           # uv run pytest  (one file: uv run pytest tests/gateways/test_signal_handler.py -q)
make lint           # uv run ruff check .
make format         # uv run ruff format .
make fix            # ruff check --fix
make init           # scripts/init_env.py — interactive .env bootstrap
make start / dev    # run the FastAPI host (needs a real .env; dev adds --reload)
```

`MetaTrader5` is a Windows-only dependency. On Linux/macOS the MT5-importing
suite cannot be collected — run
`uv run pytest --ignore=tests/gateways/forex/mt5/test_close_detector.py -q` and
say so in the report rather than presenting the collection error as a failure.

## Repository navigation

Route the task with the table before searching. Start inside the owning package;
never scan from the repository root.

1. Table below, to find the owning module.
2. `sed -n '1,25p' <file>` — **most modules open with a docstring** stating the
   job and the trade-offs. That usually answers "does this file do X".
3. `rg -n "<symbol>" worker tests` — scope the search.
4. `tests/<mirror of the source path>` — the suite mirrors `worker/` and reads
   as the executable spec for that module.

| Task or concept | Primary location |
| --- | --- |
| Signal payload from the broker | `worker/schemas/signal_schema.py` |
| Position event published back to the broker | `worker/schemas/position_schema.py` |
| ADMIN / SYSTEM / handshake payloads | `worker/schemas/{admin,system,inbox}_schema.py` |
| NATS subjects | `worker/schemas/nats_schema.py`, `examples/nats/subjects.md` |
| Which flow an action takes (the three groups) | `worker/gateways/signal_handler.py` |
| Shared NATS loop, persistence, notification (Template Method) | `worker/gateways/processor.py` |
| Entry policy gates (MAX_OPEN_ORDERS, symbol/strategy conflicts) | `worker/gateways/guard.py` |
| Market abstraction + TP1/exit orchestration | `worker/gateways/market_strategy.py` |
| Execution config value object (risk/volume params) | `worker/gateways/config.py` |
| FOREX order placement, entry sizing | `worker/gateways/forex/executor.py` |
| FOREX lot math / stop distance | `worker/gateways/forex/{lot_sizing,stop_validator}.py` |
| MT5 terminal surface (connection, symbols, deals, closes) | `worker/gateways/forex/mt5/` |
| CRYPTO order placement, sizing, notional/step filters | `worker/gateways/crypto/executor.py` |
| Binance transport and user-data stream | `worker/gateways/crypto/binance/` |
| Missed-close / missed-fill safety nets | `worker/gateways/{reconcile_job,forex/reconcile_job,crypto/reconcile_job}.py` |
| Telegram message strings | `worker/gateways/message_presenter.py` + the per-market ones |
| One-message-per-trade cycle | `worker/gateways/cycle_presenter.py`, `worker/services/cycle_notification_service.py`, `worker/jobs/cycle_notification_job.py` |
| Publishing position rows to NATS (CDC) | `worker/jobs/cdc_job.py` |
| Notification outbox dispatcher | `worker/jobs/notification_job.py`, `worker/services/notification_service.py` |
| SQLite tables, SQL, migrations-by-guarded-ALTER | `worker/db/{schema,repository,connection}.py` |
| Persistence facade used by the pipeline | `worker/services/db_service.py` |
| Settings and environment variables | `worker/settings.py`, `.env.example` |
| Protocol types for dependency inversion | `worker/interfaces/` |
| Process/thread lifecycle, watchdog, composition root | `worker/{market,process_manager,worker_runtime,context,app}.py` |
| Example payloads for every subject | `examples/nats/` |

`README.md` is ~124KB and `changelog.md` ~84KB — never read either whole. Run
`rg -n '^#{1,3} ' README.md` for the section index, then read only that range.

Do not scan `.venv/`, `uv.lock`, caches, or `logs/`.

## Architecture invariants

- **The shared pipeline stays market-neutral.** `processor.py`,
  `signal_handler.py`, `market_strategy.py` and `reconcile_job.py` must not
  learn about MT5 or a CEX; market specifics live behind the market's own
  executor/gateway under `gateways/forex/` or `gateways/crypto/`. A new platform
  is a new adapter under one of those, not a branch in the shared layer.
- **Depend on protocols, not concretions.** Business objects receive their
  dependencies through the constructor (`ExecutionConfig`, the protocols in
  `worker/interfaces/`) rather than reaching for the global `settings` — that is
  what makes them testable without MetaTrader5 or a live exchange.
- **FOREX runs in a child OS process, CRYPTO in a thread.** The MT5 C extension
  holds the GIL, so anything that cannot cross the fork boundary (DB
  connections, notifiers, daemon threads) is built inside the child by
  `WorkerContext`, never in the parent FastAPI process. Settings cross as a flat
  dict.
- **SQLite is the source of truth for exits.** An exit signal resolves against
  the tracked row, and `source_ticket` comes from that row rather than from the
  live broker ticket. Full exits (`TP2`/`SL`/`R_SL`) close the **live**
  `position.volume` — never `signal.quantity`.
- **Position columns are gateway-neutral.** One schema serves FOREX and CRYPTO;
  schema changes are additive and applied through the guarded `ALTER TABLE`
  pattern already in `worker/db/schema.py`, so an existing database upgrades in
  place.
- **Nothing in the signal path calls Telegram directly.** Notifications are
  written to the outbox (or the cycle tables) and dispatched by the background
  job, so a Telegram outage can never delay or lose a trade.
- **Publishing back to the broker goes through the `sync_status` CDC**, not from
  the executor — a row is the record, the event is derived from it.

## Code style

- Python ≥3.10, `uv`. Run Python tooling through `uv run`.
- Ruff rules `E,W,F,I,C,B`, line length 88, **2-space indent**. Run
  `make format` before committing, and format only the files you touched.
- Open a new module with a docstring stating its job and its trade-offs, in the
  style of its neighbours; comment *why*, not *what*.
- Prefer explicit types and domain terminology over clever, compressed code.
- Cover behaviour changes with focused tests, including failure paths and
  boundary cases. Reuse the fakes in `tests/helpers.py` and the fixtures in
  `tests/conftest.py` instead of mocking the platform ad hoc.

## Verification

- Run the narrowest relevant tests while iterating.
- Before handing back a code change: `uv run ruff check .` and the suite (with
  the MT5 ignore above on a non-Windows machine).
- The suite has pre-existing failures on some checkouts. Compare your failure
  list against the same command on the unmodified tree and report the
  difference, not the raw count.
- Report every command you ran and every failure or skipped check. Never claim a
  check passed without running it.

## Trading and destructive operations

- This worker places **real orders**. Do not point it at a live account, change
  credentials, disable a guard (`MAX_OPEN_ORDERS`, the entry conflict checks,
  stop validation) or widen a risk setting without an explicit request and
  confirmation of the target environment.
- `FLAT` and the ADMIN flat actions close live positions, and a `DB_FILE` delete
  destroys the only record of them. Verify the exact target and get explicit
  approval immediately before running anything of that kind.
- Prefer the testnet (`CRYPTO_TESTNET=true`) and a demo MT5 account for anything
  exercised end to end, and record the settings a result depended on.
