"""
worker/interfaces/mt5_gateway_protocol.py
─────────────────────────────────────────
Structural interface over the MetaTrader5 C extension.

The real ``MetaTrader5`` module satisfies this protocol structurally, so
production code injects the module itself. Tests inject a lightweight fake that
exposes the same handful of functions and constants — without this seam the
executor could only be imported (let alone exercised) on Windows where the
native ``MetaTrader5`` package is installable.

Only the surface actually used by :class:`~worker.gateways.mt5.executor.MT5Executor` and
its collaborators is declared here (Interface Segregation).
"""

from __future__ import annotations

from typing import Any, List, Optional, Protocol


class Mt5GatewayProtocol(Protocol):
  # ── Functions ──────────────────────────────────────────────────────────── #
  def symbols_get(self, group: str = ...) -> Optional[List[Any]]: ...
  def symbol_select(self, name: str, enable: bool) -> bool: ...
  def symbol_info(self, name: str) -> Optional[Any]: ...
  def symbol_info_tick(self, name: str) -> Optional[Any]: ...
  def account_info(self) -> Optional[Any]: ...
  def positions_get(self, symbol: str = ...) -> Optional[List[Any]]: ...
  def order_send(self, request: dict) -> Optional[Any]: ...
  def last_error(self) -> Any: ...

  # ── Constants (declared so static checkers see them on the protocol) ────── #
  SYMBOL_TRADE_MODE_DISABLED: int
  ORDER_TYPE_BUY: int
  ORDER_TYPE_SELL: int
  TRADE_ACTION_DEAL: int
  TRADE_ACTION_SLTP: int
  ORDER_TIME_GTC: int
  ORDER_FILLING_IOC: int
  TRADE_RETCODE_DONE: int
