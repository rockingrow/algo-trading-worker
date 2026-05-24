from typing import Any, TypedDict


class _TradeResultRequired(TypedDict):
  success: bool
  retcode: int


class TradeResult(_TradeResultRequired, total=False):
  comment: str
  ticket: int
  price: float
  volume: float
  source_ticket: int
  new_sl: float
  sl_update: Any  # nested TradeResult from SL update step
