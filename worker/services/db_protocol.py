from typing import Any, Dict, List, Optional, Protocol

from worker.schemas.position_schema import PositionStatusEnum


class DBServiceProtocol(Protocol):
  def get_open_positions_by_strategy(self, strategy: str, symbol: str) -> List[Dict[str, Any]]: ...
  def update_position_status(
    self,
    source_ticket: int,
    status: PositionStatusEnum,
    new_ticket: Optional[int] = None,
    closed_price: Optional[float] = None,
    mt5_retcode: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ) -> None: ...
