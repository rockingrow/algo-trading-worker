# CRYPTO (CEX / Binance)

The CRYPTO gateway: host clock requirements, the CEX integration, Binance position mode and per-symbol leverage initialisation.

## Clock sync / NTP requirement (CRYPTO)

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

## Crypto (CEX) integration

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
| `worker/gateways/crypto/reconcile_job.py` | `CryptoReconcileJob` | Periodic reconciler — diffs DB-open rows against live exchange positions in both directions: marks missed closes, and imports untracked live positions as `MANUAL`-strategy DB rows; two-scan confirmation either way |

Adding a new exchange = implement `BaseExchangeGateway` + register it in
`ExchangeFactory`. No business-logic change.

Binance specifics use the **official** `binance-sdk-derivatives-trading-usds-futures`
(and its `binance_common` transport): REST calls go through `binance_common.send_request`
(HMAC signing, timestamps, retries/backoff, typed rate-limit errors) and the User
Data Stream uses the SDK websocket (auto-reconnect). All of this lives only inside
`worker/gateways/crypto/binance/` — the gateway abstraction keeps it out of the
executor/strategy/processor, so it never leaks upward.

Required env when `MARKET_TYPE=CRYPTO` (Binance): `CRYPTO_API_KEY`,
`CRYPTO_API_SECRET`, `CRYPTO_ACCOUNT_ID`, and `CRYPTO_QUOTE_ASSET` (defaults to
`USDT`) — enforced by the settings validator. `CRYPTO_EXCHANGE` defaults to
`BINANCE`; `CRYPTO_TESTNET` is optional, and `CRYPTO_HEDGE_MODE` (default
`true`) is the Position Mode the worker enforces on the account at startup (see
below). MT5 credentials are **not** required. See `.env.example`.

## Position Mode — Hedge vs One-way (CRYPTO / Binance)

Binance Futures accounts run in one of two **Position Modes**. Rather than
relying on the operator to set it on the exchange (Binance app/web → Futures →
Preferences → Position Mode), the worker **reconciles** the account into
`CRYPTO_HEDGE_MODE` right after connecting — the generic
`BaseExchangeGateway.set_position_mode()` hook, implemented by the Binance
gateway via `POST /fapi/v1/positionSide/dual`. So `CRYPTO_HEDGE_MODE` is the
mode the worker *drives the account into*, and it selects the order payload:

| `CRYPTO_HEDGE_MODE` | Exchange mode | Order payload |
| --- | --- | --- |
| `true` (default) | Hedge | Every order (market open/close, stop-loss, take-profit) carries an explicit `positionSide` (`LONG`/`SHORT`); `reduceOnly` is omitted entirely — Binance rejects it in Hedge Mode. |
| `false` | One-way | No `positionSide` sent; `reduceOnly` marks closing orders instead. Binance infers direction from `side` alone. |

The switch is best-effort: Binance returns `-4059` ("*No need to change
position side.*") when the account is already in the requested mode (treated as
success), and can reject the switch (e.g. `-4068`) when open positions or orders
exist — that is logged and the worker proceeds on the account's current mode, in
which case a mismatch still surfaces as Binance error `-4061` ("*Order's
position side does not match user's setting.*") on the first order.

The worker still only ever tracks **one net position per symbol** regardless
of this setting: it does not open or manage simultaneous LONG and SHORT
positions on the same symbol even when the account is in Hedge Mode. Strategy
isolation on a shared symbol remains logical-only, as described in [Signal
Execution — Key Design Decisions](design-decisions.md#signal-execution--key-design-decisions).

## Per-symbol leverage initialisation (CRYPTO)

USDⓈ-M futures leverage is **sticky on the exchange**: whatever value was set
the last time on a symbol (manually in the UI or by a previous worker run) is
what the next order is sized against. Without an init pass, a sub-account
capped at 5x can silently leave a symbol at its old 20x setting (or vice
versa), and an order can fail with `-2019 Margin is insufficient` even when
risk-sizing math is correct.

`CryptoSignalProcessor` runs `LeverageInitJob` on demand, never at startup. Two
things trigger it, both from the broker (see
[SYSTEM Subject](nats-system.md#system-subject)): the `crypto_leverage_init` section of the
`WORKER_CONNECTED_ACK` on connect, and a runtime `CRYPTO_LEVERAGE_INIT` broadcast
when an admin changes the account's leverage settings later. Neither one arriving
means no leverage is touched. For each symbol in `CRYPTO_LEVERAGE_INIT_SYMBOLS`:

1. `gateway.get_max_leverage(symbol)` (Binance: `GET /fapi/v1/leverageBracket`)
   returns the account-side ceiling. Sub-account / VIP caps are reflected here
   automatically — a sub-account limited to 5x returns 5; an unrestricted
   account returns the exchange-wide max (e.g. 125 for BTCUSDT).
2. The target is `min(exchange_max, MAX_LEVERAGE_CAP)`.
3. `gateway.set_leverage(symbol, target)` (Binance: `POST /fapi/v1/leverage`)
   applies it.

Some account-level caps (sub-account / VIP tier) are **not** exposed by
`leverageBracket` and surface only when `set_leverage` is POSTed, as a `-4421`
rejection whose message names the real ceiling (e.g. *"…greater than 5x"*). The
gateway parses that ceiling and retries once at it, so the symbol still lands on
its true limit instead of being abandoned. If Binance ever rewords the message
so the parser misses it, `set_leverage` retries one last time at
`min(MIN_LEVERAGE_CAP, target)` — a known-safe floor — rather than leaving the
symbol at its dangerous default. If the account is restricted below that floor,
the retry also fails and the symbol is logged for manual fixing.

| Env var | Default | Purpose |
| --- | --- | --- |
| `CRYPTO_LEVERAGE_INIT_SYMBOLS` | empty (skip) | Comma-separated raw signal symbols to initialise (`BTCUSDT.P,ETHUSDT.P` or `BTCUSD,ETHUSD`). Resolved through the executor's symbol resolver, so the form mirrors how upstream signals address the symbol. |
| `MAX_LEVERAGE_CAP` | `10` | Upper bound applied per symbol. A sub-account at 5x lands on 5; an unrestricted account lands on this cap. |
| `MIN_LEVERAGE_CAP` | `5` | Last-resort floor used only when a `-4421` account cap cannot be parsed from the error message. Set to the lowest leverage your sub-accounts can take; if an account is restricted below it, the retry still fails and the symbol is left for manual fixing. |

Failure modes are isolated: a failed `get_max_leverage` (network blip, symbol
typo) skips that symbol with a warning and **never falls back to the cap**
blindly — picking the cap for an account that is actually limited to 5x is
the failure mode this job exists to prevent. Any uncaught crash inside the
job is logged and swallowed so the worker keeps running; symbols are left at
their current exchange-side leverage until the next trigger.

The pass can be re-triggered at any time — without restarting the worker — via a
[`CRYPTO_LEVERAGE_INIT`](nats-system.md#action-crypto_leverage_init-crypto-runtime) broadcast,
which may also override the symbol set and cap for that one run. It also re-runs
on every reconnect handshake whose ACK carries the section, since the worker
re-announces `WORKER_CONNECTED` after a broker/NATS restart.
