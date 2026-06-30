"""
worker/gateways/crypto/leverage_init_job.py
───────────────────────────────────────────
One-shot leverage initialisation for crypto exchanges.

Runs once at worker startup, after the gateway connection is verified, to bring
every configured symbol's leverage into a known state. Per-symbol leverage on
USDⓈ-M futures is sticky on the exchange — whatever value was set the last
time (manually or by a prior worker) is what the next order is sized against —
so without an init pass a sub-account capped at 5x can silently leave a symbol
at its old 20x setting (or vice versa) and an order can fail with ``-2019
Margin is insufficient``.

Algorithm (per symbol, sequential — leverage endpoints are cheap and we want
deterministic ordering in the logs):

1. ``gateway.get_max_leverage(symbol)`` → the exchange-side maximum for this
   *account* (sub-account caps are reflected here automatically).
2. ``target = min(max_leverage, MAX_LEVERAGE_CAP)``.
3. ``gateway.set_leverage(symbol, target)``.

If max-leverage lookup fails (network blip, symbol typo) the symbol is skipped
with a warning — never falls back to the cap blindly, because picking the cap
for a sub-account that is actually limited to 5x is exactly the failure mode
this job exists to prevent.
"""

from __future__ import annotations

from typing import Callable, Iterable, Optional

from worker.gateways.crypto.base import BaseExchangeGateway
from worker.logger import get_logger

log = get_logger("worker.gateways.crypto.leverage_init_job")


class LeverageInitJob:
  """Initialise per-symbol leverage to ``min(exchange_max, cap)``."""

  def __init__(
    self,
    gateway: BaseExchangeGateway,
    symbols: Iterable[str],
    max_leverage_cap: int,
    resolve_symbol: Optional[Callable[[str], str]] = None,
  ) -> None:
    self._gateway = gateway
    # Deduplicate while preserving the .env declaration order, so the log lines
    # reflect what the operator wrote rather than a hash-set order.
    self._symbols: list[str] = list(dict.fromkeys(s for s in symbols if s))
    self._cap = int(max_leverage_cap)
    self._resolve = resolve_symbol or (lambda s: s)

  def run(self) -> None:
    if not self._symbols:
      log.info("[LeverageInit] No symbols configured — skipping.")
      return
    if self._cap <= 0:
      log.warning(
        "[LeverageInit] MAX_LEVERAGE_CAP=%s is non-positive — skipping init.",
        self._cap,
      )
      return

    log.info(
      "[LeverageInit] Initialising leverage for %d symbol(s) (cap=%s)…",
      len(self._symbols), self._cap,
    )
    for raw in self._symbols:
      self._init_one(raw)

  def _init_one(self, raw_symbol: str) -> None:
    resolved = self._resolve(raw_symbol)
    max_lev = self._gateway.get_max_leverage(resolved)
    if max_lev is None or max_lev <= 0:
      log.warning(
        "[LeverageInit] %s (%s): max leverage unavailable — skipped.",
        raw_symbol, resolved,
      )
      return

    target = min(max_lev, self._cap)
    ok = self._gateway.set_leverage(resolved, target)
    if ok:
      log.info(
        "[LeverageInit] %s (%s): exchange_max=%s cap=%s → set leverage=%s",
        raw_symbol, resolved, max_lev, self._cap, target,
      )
    else:
      log.error(
        "[LeverageInit] %s (%s): set_leverage(%s) failed — symbol left at its "
        "current exchange setting.",
        raw_symbol, resolved, target,
      )
