"""
worker/gateways/forex/mt5/symbol_resolver.py
────────────────────────────────────────────
Resolves a base symbol (e.g. ``XAUUSD``) to the broker's actual tradeable
symbol name (e.g. ``XAUUSDc``) and caches the result.

Extracted from ``MT5Executor`` so symbol resolution has a single reason to
change (Single Responsibility) and can be reused/mocked independently.
"""

from __future__ import annotations

from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger

logger = get_logger("worker.gateways.forex.mt5.symbol_resolver")


class SymbolResolver:
  """Maps base symbols to tradeable broker symbols, with an in-memory cache."""

  def __init__(self, mt5_api: Mt5GatewayProtocol) -> None:
    self._mt5 = mt5_api
    self._cache: dict[str, str] = {}

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

        logger.info(f"Resolved symbol: {base_symbol} -> {sym.name}")
        self._cache[base_symbol] = sym.name
        return sym.name

    return base_symbol

  def base_for(self, resolved_symbol: str) -> str:
    """Best-effort inverse of :meth:`get_symbol`: map a tradeable broker symbol
    (e.g. ``XAUUSDc``) back to the base symbol upstream signals use (``XAUUSD``).

    Uses the resolve cache, so it only knows symbols this worker has already
    resolved. When the mapping is unknown (a symbol never traded/queried) it
    returns *resolved_symbol* unchanged — for brokers without a suffix that is
    already the base symbol, so the common case is always correct.
    """
    for base, resolved in self._cache.items():
      if resolved == resolved_symbol:
        return base
    return resolved_symbol
