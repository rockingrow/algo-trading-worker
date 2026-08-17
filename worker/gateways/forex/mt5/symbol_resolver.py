"""
worker/gateways/forex/mt5/symbol_resolver.py
────────────────────────────────────────────
Resolves a base symbol (e.g. ``XAUUSD``) to the broker's actual tradeable
symbol name (e.g. ``XAUUSDc``) and caches the result.

Extracted from ``MT5Executor`` so symbol resolution has a single reason to
change (Single Responsibility) and can be reused/mocked independently.
"""

from __future__ import annotations

import time

from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger

logger = get_logger("worker.gateways.forex.mt5.symbol_resolver")

# How long to wait for the terminal's first quote after subscribing a symbol.
_TICK_WAIT_TIMEOUT = 2.0
_TICK_WAIT_INTERVAL = 0.1


class SymbolResolver:
  """Maps base symbols to tradeable broker symbols, with an in-memory cache."""

  def __init__(
    self,
    mt5_api: Mt5GatewayProtocol,
    tick_wait_timeout: float = _TICK_WAIT_TIMEOUT,
    tick_wait_interval: float = _TICK_WAIT_INTERVAL,
  ) -> None:
    self._mt5 = mt5_api
    self._cache: dict[str, str] = {}
    self._tick_wait_timeout = tick_wait_timeout
    self._tick_wait_interval = tick_wait_interval

  def get_symbol(self, base_symbol: str) -> str:
    """Dynamically find the tradeable symbol name (e.g., XAUUSD -> XAUUSDc)."""
    if base_symbol in self._cache:
      return self._cache[base_symbol]

    symbols = self._mt5.symbols_get(group=f"*{base_symbol}*")
    if not symbols:
      logger.warning(f"No symbols found matching {base_symbol}")
      return base_symbol

    for sym in symbols:
      # Check if name starts with base_symbol and is tradeable
      if (
        sym.name.startswith(base_symbol)
        and sym.trade_mode != self._mt5.SYMBOL_TRADE_MODE_DISABLED
      ):
        if not sym.visible:
          self._mt5.symbol_select(sym.name, True)
          self._await_first_tick(sym.name)

        logger.info(f"Resolved symbol: {base_symbol} -> {sym.name}")
        self._cache[base_symbol] = sym.name
        return sym.name

    return base_symbol

  def _await_first_tick(self, name: str) -> None:
    """Block briefly until a symbol just added to Market Watch has a live quote.

    ``symbol_select`` only *asks* the terminal to subscribe; the first tick lands
    a moment later. Reading ``symbol_info_tick`` inside that window yields a tick
    with ``bid == ask == 0.0``, so the very first signal for a symbol would be
    priced off zero and rejected by the broker. Poll until the quote is real, or
    give up after the timeout — the caller's own tick check then fails the trade
    cleanly instead of sending a bogus order.
    """
    deadline = time.monotonic() + self._tick_wait_timeout
    while True:
      tick = self._mt5.symbol_info_tick(name)
      if tick and tick.bid > 0 and tick.ask > 0:
        return
      if time.monotonic() >= deadline:
        logger.warning(
          f"No quote for {name} after waiting {self._tick_wait_timeout}s — "
          "symbol may be outside its trading session."
        )
        return
      time.sleep(self._tick_wait_interval)
