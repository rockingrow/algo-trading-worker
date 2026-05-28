from typing import Any, TypedDict


class _TradeResultRequired(TypedDict):
  """Required fields present on every TradeResult."""

  success: bool
  retcode: int


class TradeResult(_TradeResultRequired, total=False):
  """Return value from all MT5Executor trade operations; optional fields are populated only when the order fills."""
  comment: str
  ticket: int
  price: float
  volume: float
  source_ticket: int
  new_sl: float
  sl_update: Any  # nested TradeResult from SL update step
