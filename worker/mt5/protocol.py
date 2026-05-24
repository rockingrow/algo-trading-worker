from typing import Any, List, Optional, Protocol, runtime_checkable

from worker.schemas.broker_schema import SignalSchema
from worker.schemas.trade_result import TradeResult


@runtime_checkable
class MT5ExecutorProtocol(Protocol):
  def open_position(self, signal: SignalSchema) -> TradeResult: ...
  def partial_close_position(
    self,
    symbol: str,
    close_volume: float,
    position_ticket: Optional[int] = None,
  ) -> TradeResult: ...
  def update_position_sl(
    self,
    symbol: str,
    new_sl: float,
    position_ticket: Optional[int] = None,
  ) -> TradeResult: ...
  def close_all_positions(self, symbol: str, reason: str = "CLOSE") -> TradeResult: ...
  def get_open_positions(self, symbol: str) -> List[Any]: ...
  def normalize_volume(self, symbol: str, volume: float) -> float: ...
  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float: ...
