from typing import Optional

from worker.db import get_open_positions_by_strategy, get_position, init_db, insert_position, log_position, update_position_status
from worker.schemas.position_schema import PositionStatusEnum


class DBService:
  """Service layer for database operations to decouple core DB logic from other services."""

  def __init__(self):
    pass

  def initialize(self):
    """Initialize the database schema."""
    init_db()

  def log_position(
    self,
    strategy: str,
    ticket: Optional[int],
    source_ticket: Optional[int],
    symbol: str,
    action: str,
    volume: float,
    price: float,
    sl: Optional[float],
    tp1: Optional[float],
    mt5_retcode: int,
    comment: str = "",
    message: Optional[str] = None,
    author: str = "broker",
  ):
    """Log order execution result to the database."""
    log_position(
      strategy=strategy,
      ticket=ticket,
      source_ticket=source_ticket,
      symbol=symbol,
      action=action,
      volume=volume,
      price=price,
      sl=sl,
      tp1=tp1,
      mt5_retcode=mt5_retcode,
      comment=comment,
      message=message,
      author=author,
    )

  def insert_position(
    self,
    ticket: int,
    strategy: str,
    symbol: str,
    action: str,
    volume: float,
    opened_price: float,
    mt5_retcode: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ):
    """Insert a successfully opened position."""
    insert_position(
      ticket=ticket,
      strategy=strategy,
      symbol=symbol,
      action=action,
      volume=volume,
      opened_price=opened_price,
      mt5_retcode=mt5_retcode,
      comment=comment,
      message=message,
    )

  def update_position_status(
    self,
    source_ticket: int,
    status: PositionStatusEnum,
    new_ticket: Optional[int] = None,
    closed_price: Optional[float] = None,
    mt5_retcode: Optional[int] = None,
    comment: Optional[str] = None,
    message: Optional[str] = None,
  ):
    """Update a position's status (TP1, TP2, SL, R_SL, TERMINAL_CLOSED, FORCED_CLOSED, FLATTED)."""
    update_position_status(
      source_ticket=source_ticket,
      status=status.value,
      new_ticket=new_ticket,
      closed_price=closed_price,
      mt5_retcode=mt5_retcode,
      comment=comment,
      message=message,
    )

  def get_position(self, source_ticket: int) -> Optional[dict]:
    """Fetch a position by source_ticket."""
    return get_position(source_ticket=source_ticket)

  def get_open_positions_by_strategy(self, strategy: str, symbol: str) -> list:
    """Fetch all OPENED or TP1 positions for a given strategy+symbol."""
    return get_open_positions_by_strategy(strategy=strategy, symbol=symbol)
