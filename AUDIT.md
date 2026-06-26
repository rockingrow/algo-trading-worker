# 🔍 Code Audit — Algo Trading Worker

**Date:** 2026-06-26
**Scope:** Full `worker/` package + test suite (`tests/`)
**Method:** Static reading of every module on the execution path, `ruff` lint,
`pytest` run, and two targeted runtime probes to confirm the highest-severity
findings empirically.

> **TL;DR.** The architecture is strong — clean Template-Method / factory / DI
> seams, good separation between market-agnostic logic and broker adapters, and
> careful safety reasoning around unprotected positions. But there is **one
> critical data-loss bug** (every NATS `TRADE` event is published without
> `sl`/`tp1`/`tp2`/`signal_id`/`message`), **one high-severity outbox bug**
> (`NOTIFICATION_MODE=SILENT|ERROR` silently drops *and* leaks community
> notifications), and the **test suite is currently red (7 failures)**. Details,
> evidence, and fixes below.

---

## Severity legend

| Level | Meaning |
| --- | --- |
| 🔴 Critical | Silent data loss / incorrect trading-relevant data reaching the broker |
| 🟠 High | Functional break or unbounded resource growth under a supported config |
| 🟡 Medium | Reliability gap, broken CI, or a real edge-case defect |
| 🟢 Low / Enhancement | Best-practice / maintainability / design improvement |

---

# Part A — Bugs

## 🔴 A1 — `PositionCDC` publishes TRADE events without `message`, `sl`, `tp1`, `tp2`, `signal_id`, `risk_percent`

**Files:** `worker/jobs/cdc_job.py:28-47`, `:119-120`, `:170-186`;
`worker/db/schema.py:46`; `worker/schemas/position_schema.py:54-67`

The `positions` table column is **`gateway_message`** (`schema.py:46`), but
`PositionCDC` reads the key **`message`**:

```python
_EVENT_FIELDS = { ... "comment", "message", "strategy_code", ... }   # cdc_job.py:28-47
payload = {k: row[k] for k in _EVENT_FIELDS if k in row}             # ":119
payload.update(self._extract_signal_fields(row.get("message")))     # ":120
```

`get_pending_sync_positions()` does `SELECT *` and `dict(row)`, so the row dict
has the **physical** key `gateway_message`, never `message`. Therefore:

* `"message" not in row` → the raw signal JSON is **never copied** into
  `PositionEvent.message`.
* `row.get("message")` is `None` → `_extract_signal_fields(None)` returns `{}`,
  so `signal_id`, `sl`, `tp1`, `tp2`, `risk_percent` are **never populated**.

`PositionEvent`'s docstring states these are *"needed for broker upsert the first
time a position is seen"* — so the broker receives every position with null
SL/TP and no original payload. **Affects both FOREX and CRYPTO** (CDC is shared)
and every event type (CREATED/UPDATED).

### Evidence (runtime probe)

Inserted a position with `message={"signal_id":"abc123","sl":1791,"tp1":2222,"tp2":2255,"risk_percent":1.5}`, then replicated the CDC extraction exactly:

```
ROW KEYS: [... 'gateway_message', 'gateway_return_code', ...]   # no 'message'
has 'message' key? False | has 'gateway_message'? True
EXTRACTED signal fields: {}
=> message in TRADE event: None
=> sl/tp1/tp2/signal_id in TRADE event: None None None None
```

### Why it slipped through

The "gateway-neutral columns" refactor renamed the column to `gateway_message`,
but `cdc_job.py` was not updated, and **there is no test for `PositionCDC`** (no
`tests/jobs/test_cdc_job.py`). Note the README's column table even claims the
repository maps `gateway_message → message` — it does **not** (`_row_to_dict` is
a bare `dict(row)`), so the contract is only documented, never implemented.

### Fix (minimal)

```python
payload = {k: row[k] for k in _EVENT_FIELDS if k in row}
raw_msg = row.get("gateway_message")
payload["message"] = raw_msg
payload.update(self._extract_signal_fields(raw_msg))
```

Better: make `worker/db/repository.py` the *real* mapping boundary it claims to
be (rename `gateway_message → message` on read), so CDC and every other reader
speak one vocabulary. Then add a `PositionCDC` test asserting the published
`PositionEvent` carries `sl/tp1/tp2/signal_id/message`.

---

## 🟠 A2 — `NOTIFICATION_MODE=SILENT` / `ERROR` silently drops *and* leaks COMMUNITY notifications

**Files:** `worker/db/repository.py:296-320` (`get_due_notifications`);
`worker/context.py:65-82`; `worker/db/schema.py:84`

The dispatcher poll query hard-filters on mode:

```sql
SELECT * FROM notifications WHERE mode = 'VERBOSE' AND attempts < max_attempts AND ...
```

But `WorkerContext._build_enqueue` enqueues COMMUNITY rows with the **configured**
mode (`context.py:73`): under `NOTIFICATION_MODE=SILENT` they get `mode='SILENT'`,
under `ERROR` they get `mode='ERROR'`. Those rows therefore:

1. are **never selected** → never sent (intended suppression for `SILENT`?), **and**
2. are **never deleted and never dead-lettered** (`attempts` stays `0`) → they
   accumulate in the `notifications` table **forever** (unbounded growth).

Worse, **`ERROR` mode suppresses error notifications too** — `order_failed` is a
COMMUNITY message, so in `ERROR` mode it is also stamped `ERROR` and dropped. The
one mode whose name promises "only errors" delivers nothing.

`mode` has **no other consumer** anywhere in the code (verified by grep) — it is
purely this delivery gate. The README documents the poll query *without* the
`mode = 'VERBOSE'` clause and describes `mode` as a free-form analytics tag, so
this filter is undocumented behaviour.

### Evidence (runtime probe)

```
rows due for dispatch: 0
=> SILENT/ERROR community notifications are dispatched? False
=> they remain stuck in the table forever (never sent, never deleted)
```

### Fix

Decide the intended semantics and make them explicit:

* If `SILENT`/`ERROR` mean "do not send community messages": **don't enqueue
  them** in `_build_enqueue` (return early), so nothing leaks.
* If `mode` is meant as a Telegram silent-push flag: drop the `mode='VERBOSE'`
  filter from the query and pass `disable_notification` to the Telegram payload
  based on the row's mode.
* For `ERROR`: enqueue only error-category messages, and still deliver them.

Either way, remove the dead-letter leak.

---

## 🟡 A3 — `PositionCDC` claims at-least-once delivery but is effectively at-most-once

**Files:** `worker/jobs/cdc_job.py:143-151`; `worker/services/nats_service.py:188-190`

```python
self._publisher.publish(NatsSubjectEnum.TRADE, event_json)   # cdc_job.py:143
marked = self._db.mark_position_synced(row["id"], row["updated_at"])  # ":146
```

`NATSPublisher.publish()` only does `self._send_queue.put(...)` — an **in-memory,
fire-and-forget enqueue**. It returns before the async loop has actually sent the
frame to NATS (and `nc.publish` failures are logged and **dropped**, never
requeued — `nats_service.py:179-183`). The CDC then immediately marks the row
`PUBLISHED`. So a crash, NATS disconnect, or publish error between enqueue and
flush loses the `TRADE` event while the DB believes it was delivered — the exact
opposite of the *"Publish-then-mark gives at-least-once delivery"* comment.

### Fix

Make `publish()` confirmable (e.g. `run_coroutine_threadsafe(...).result(timeout)`
so a publish failure raises) and only `mark_position_synced` when it succeeds; or
have the publisher requeue on failure. The row staying `PENDING` on failure is
already handled correctly by the optimistic-lock check — the gap is purely that
the local enqueue can't fail.

---

## 🟡 A4 — Test suite is red: 7 failing tests (test/code drift + non-hermetic tests)

`uv run pytest -q` → **7 failed**. None are environment noise; each is a real
divergence:

| Test | Root cause |
| --- | --- |
| `test_exchange_factory.py::test_factory_builds_binance_gateway` / `::test_factory_accepts_string_exchange` | `ExchangeFactory._build_binance` calls `settings_dict["binance_api_key"].get_secret_value()` (`factory.py:28`). The factory **assumes `SecretStr`** and raises `AttributeError` on a plain `str`. Brittle: it works in prod (where `settings.model_dump()` keeps `SecretStr`) but the contract is implicit and untested for the str case. |
| `test_message_presenter.py::test_order_filled_shows_scale_position_block` | Presenter renders `"Scaled Position"` (`message_presenter.py:105`); test asserts `"Scale Position"` — a one-character string mismatch. |
| `test_crypto_signal_processor.py::test_account_id_*` (×3) | Tests expect `_account_id` to fall back to the exchange name (`"BINANCE"`) / `"CRYPTO"` when unset; implementation returns `self.settings.get("binance_account_id")` → `None` (`signal_processor.py:70-72`). Stale tests vs. current contract. |
| `test_settings_validation.py::test_crypto_ok_with_keys_and_no_mt5` | Sets `BINANCE_API_KEY`/`SECRET` but not `BINANCE_ACCOUNT_ID`, which the validator now requires (`settings.py:157`). The test isn't hermetic — it only passes if `BINANCE_ACCOUNT_ID` happens to be in the ambient environment. |

Two systemic issues underneath: **(a)** code and tests drifted with no CI gate
catching it; **(b)** `Settings` tests depend on ambient env (`Settings(_env_file=None)`
still reads `os.environ`), so results vary by machine. The `_account_id == None`
case is also a latent bug: `PositionEvent.account_id` is a required `str`, so a
`None` here would raise `ValidationError` in the CDC loop and wedge publishing —
it's saved in prod only because settings validation forbids a missing
`BINANCE_ACCOUNT_ID`.

### Fix

Reconcile each test with the intended contract (update the test or restore the
behaviour), make the factory accept both `SecretStr` and `str`, and give the
settings tests a hermetic fixture that clears all `BINANCE_*`/`MT5_*` env first.

---

## 🟡 A5 — CRYPTO worker restart spawns **duplicate** daemon jobs (double Telegram sends + double TRADE publishes)

**Files:** `worker/market.py:118-129` (`ThreadGatewayOrchestrator._watchdog`);
`worker/worker_runtime.py:42-61`

`run_worker` starts `NotificationJob` and (via `start_market_jobs`) `PositionCDC`
+ the user-data stream + reconciler — all as daemon threads bound to the shared
`stop_event`. If the crypto worker thread exits **without** `stop_event` being set
(e.g. `processor.connect()` returns `False`, so `run_worker` returns early —
`worker_runtime.py:52-54`), those daemon threads keep running. The thread
watchdog then sees a dead worker and `_spawn`s a fresh `run_worker`, which starts
**another** `NotificationJob`/`PositionCDC`. Now two dispatchers poll the same
SQLite outbox and two CDC loops poll the same `positions` table → duplicate
Telegram messages and duplicate `TRADE` publishes (and the count grows with each
restart).

FOREX is immune: it restarts a whole **child process**, so the old daemon threads
die with it. The bug is specific to the thread orchestrator (CRYPTO).

### Fix

On restart, set the old `stop_event` and re-create a fresh one before re-spawning
(so the previous generation's jobs exit), or move job startup so it cannot run
twice against one shared event. Add a guard so `start()` is idempotent.

---

## 🟢 A6 — Smaller correctness/robustness items

* **`datetime.utcnow()` deprecation** (`worker/jobs/notification_job.py:33`) —
  deprecated in 3.12+. Use `datetime.now(timezone.utc)`. (Values still compare
  correctly against SQLite `CURRENT_TIMESTAMP`, which is UTC.)
* **`NATSSubscriber.listen` reaches into `self._client._stop_event`** (private,
  `nats_service.py:121`) — encapsulation leak; expose a public predicate on
  `NatsClient`.
* **Unbounded in-memory queues** — `NATSPublisher._send_queue` and
  `NATSSubscriber._msg_queue` are unbounded; a prolonged NATS outage or a
  consumer slower than the producer grows memory without backpressure.
* **`get_due_notifications` ignores `platform`** — harmless today (only Telegram),
  but the column exists and the dispatcher map is keyed only by `channel`.

---

# Part B — Design / best-practice enhancements

### B1 — Make the repository the *real* gateway-neutral mapping boundary
The README says `worker/db/repository.py` maps physical columns
(`gateway_message`, `ref_id`, …) to app-domain names, but `_row_to_dict` is a bare
`dict(row)`. The mapping lives implicitly in scattered readers (and is wrong in
CDC — see A1). Centralise it: a single `_row_to_domain()` that renames on read so
every consumer sees `message`/`ticket`/`source_ticket`/`magic`. This kills A1 by
construction and makes the "single boundary" claim true.

### B2 — Replace `dict`-typed `TradeResult` with a typed structure
`TradeResult` flows everywhere as a loosely-typed `dict[str, Any]`
(`result.get("success")`, `result.get("retcode")`, ad-hoc keys like
`sl_failsafe_close`, `forced_closed`, `position_unprotected`). A `TypedDict` (or
frozen dataclass) would give the type checker and readers a contract, prevent
silent typos (`retcode` vs `ret_code`), and document the optional keys. The
factories `TradeResult.ok()/fail()` are already the natural place to anchor it.

### B3 — Collapse the per-method SQLite `try/conn/finally` boilerplate
Every repository method repeats the same `conn = None … finally: conn.close()`
shape (~12×). A small `@contextmanager def _cursor(commit=False)` (or a decorator)
removes the duplication and guarantees the pattern is identical everywhere. It
also fixes a latent inconsistency: some methods swallow exceptions and return
`[]`/`None`, others re-`raise` — that policy should be explicit and uniform.

### B4 — Connection-per-call vs. pooling
`_get_conn()` opens and closes a fresh SQLite connection on **every** operation
(`connection.py:9`). With four daemon threads polling every 1–2 s plus the signal
path, that's a lot of open/close churn. WAL makes it safe, but a per-thread
connection (thread-local) or a tiny pool would cut overhead. Low priority, but
worth noting given the polling cadence.

### B5 — Outbox lifecycle: dead-letter cleanup + observability
Dead-lettered rows (`attempts >= max_attempts`) and (today) all SILENT/ERROR rows
live forever. Add a retention/cleanup job and a metric/log of outbox depth so a
stuck queue is visible. Pairs with the A2 fix.

### B6 — Isolate the Binance global monkey-patch
`gateway.py:72` rebinds `binance_common.utils.get_timestamp` at **import time**
and stores the offset in a module global (`_TIME_OFFSET_MS`). It's clever and
documented, but it's a process-wide side effect on a third-party module that
fires merely by importing the gateway (e.g. in tests). Prefer patching inside the
gateway instance lifecycle (or via the SDK's config if a hook exists) so importing
the module has no global effect.

### B7 — `_pick` / position-selection duplication across executors
`ForexExecutor._pick` (`executor.py:290-297`) and the inline
`next((p for p in positions if p.ticket == ticket), positions[0])` in
`CryptoExecutor` (`executor.py:353-357`, `:382-386`) are the same logic three
times. Hoist to one shared helper (both executors already share the
`TradeExecutorProtocol` surface).

### B8 — Tests: close the coverage gaps the bugs exposed
* Add `PositionCDC` tests asserting the published `PositionEvent` carries the
  signal-derived fields (would have caught A1).
* Add a `NotificationJob`/outbox dispatch test across all three modes (A2).
* Make `Settings` tests hermetic (clear ambient `BINANCE_*`/`MT5_*`), and assert
  the factory accepts both `SecretStr` and `str` (A4).
* Consider a tiny integration test that round-trips insert → CDC → captured
  publish, since the per-unit fakes currently hide the column-name mismatch.

### B9 — Minor naming / docs drift
The README's `positions` column-mapping table and the documented
`NotificationJob` poll query are both out of sync with the code (A1, A2). Once the
fixes land, refresh those sections so the docs match the implementation.

---

# Appendix — what's already good

* **Clean architecture**: Template-Method (`BaseSignalProcessor`), factories
  (`MarketStrategyFactory`, `ExchangeFactory`, `PlatformFactory`), and a real DI
  seam at the gateway boundary — the order layer is unit-testable off-Windows.
* **Disciplined safety reasoning**: breakeven-SL failure → emergency close →
  loud escalation; entry SL-placement failure → rollback; "broker is source of
  truth" reconcile for ADMIN FLAT; two-scan confirmation in the crypto
  reconciler; `seen_tickets` preserved across MT5 disconnects.
* **Careful external-API handling**: Binance `-1021` clock re-sync + retry,
  tick-size snapping (`-1111`), `avgPrice==0` fallback via `cumQuote`, algo-order
  endpoint for conditional stops, worker-tagged client order ids to skip the
  worker's own fills.
* **`ruff` is clean** (`All checks passed!`) and modules are thoroughly
  docstring'd.

The findings above are concentrated in two seams — the **CDC field mapping** and
the **outbox mode gate** — plus test hygiene. Fixing A1 and A2 (and greening the
suite) addresses the material risk; Part B is incremental hardening.
