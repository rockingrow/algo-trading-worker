from typing import Optional

from worker.db import init_db, log_order


class DBService:
  """Service layer for database operations to decouple core DB logic from other services."""

  def __init__(self):
    pass

  def initialize(self):
    """Initialize the database schema."""
    init_db()

  def log_order(
    self,
    ticket: Optional[int],
    symbol: str,
    action: str,
    volume: float,
    price: float,
    sl: Optional[float],
    tp1: Optional[float],
    mt5_retcode: int,
    comment: str = "",
  ):
    """Log order execution result to the database."""
    log_order(
      ticket=ticket,
      symbol=symbol,
      action=action,
      volume=volume,
      price=price,
      sl=sl,
      tp1=tp1,
      mt5_retcode=mt5_retcode,
      comment=comment,
    )
