"""
worker/interfaces/executor_protocol.py
──────────────────────────────────────
Market-agnostic execution contract.

Both the MT5 executor (FOREX) and the crypto executor (CEX) satisfy this
structurally, so :class:`~worker.gateways.market_strategy.ExecutorBackedMarket`
depends only on this protocol and never on a concrete broker. The legacy
:class:`~worker.interfaces.mt5_executor_protocol.MT5ExecutorProtocol` is kept as
an alias for backward compatibility.
"""

from typing import Any, List, Optional, Protocol

from worker.schemas.signal_schema import SignalSchema
from worker.schemas.trade_result import TradeResult


class TradeExecutorProtocol(Protocol):
  """The order-execution surface a market strategy needs from any broker."""

  def open_position(self, signal: SignalSchema) -> TradeResult: ...
  def get_entry_price(self, signal: SignalSchema) -> Optional[float]: ...
  def partial_close_position(
    self,
    symbol: str,
    close_volume: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
  ) -> TradeResult: ...
  def update_position_sl(
    self,
    symbol: str,
    new_sl: float,
    position_ticket: Optional[int] = None,
    strategy: Optional[str] = None,
  ) -> TradeResult: ...
  def close_all_positions(
    self, symbol: str, reason: str = "CLOSE", strategy: Optional[str] = None
  ) -> TradeResult: ...
  def get_open_positions(
    self, symbol: str, strategy: Optional[str] = None
  ) -> List[Any]: ...
  def get_all_open_positions(self, strategy: Optional[str] = None) -> List[Any]: ...
  def close_single_position(self, pos: Any, reason: str = "FLAT") -> TradeResult: ...
  def normalize_volume(self, symbol: str, volume: float) -> float: ...
  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float: ...
