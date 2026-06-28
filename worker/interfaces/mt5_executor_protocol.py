from typing import Any, List, Optional, Protocol

from worker.schemas.trade_result import TradeResult
from worker.schemas.signal_schema import SignalSchema


class MT5ExecutorProtocol(Protocol):
  def open_position(self, signal: SignalSchema) -> TradeResult: ...
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
  def normalize_volume(self, symbol: str, volume: float) -> float: ...
  def convert_quantity_to_lots(self, symbol: str, quantity: float) -> float: ...
