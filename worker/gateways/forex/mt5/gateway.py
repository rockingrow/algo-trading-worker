"""
worker/gateways/forex/mt5/gateway.py
────────────────────────────────────
MetaTrader 5 adapter — concrete :class:`~worker.gateways.forex.base.BasePlatformGateway`.

This is the **only** MT5-coupled code in the order path. Every ``mt5.*`` call that
used to live in ``MT5Executor`` + its helpers is here: order_send for
open/close/SL, plus ``symbol_info`` → :class:`SymbolSpec`, ``symbol_info_tick`` →
:class:`Tick`, and ``positions_get`` → :class:`PlatformPosition` mapping. The
agnostic :class:`~worker.gateways.forex.executor.ForexExecutor` above depends only
on the contract, exactly as ``CryptoExecutor`` depends on ``BaseExchangeGateway``.

Connection lifecycle (initialize/login/reconnect/restart/shutdown) is delegated to
the existing :class:`~worker.gateways.forex.mt5.bridge.MT5` wrapper; symbol
resolution to :class:`~worker.gateways.forex.mt5.symbol_resolver.SymbolResolver`.
The raw MetaTrader5 module is injected as ``mt5_api`` so order/data methods are
unit-testable off-Windows with a fake.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from worker.gateways.forex.base import (
  SIDE_LONG,
  SIDE_SHORT,
  BasePlatformGateway,
  PlatformPosition,
  SymbolSpec,
  Tick,
)
from worker.gateways.forex.mt5.bridge import MT5
from worker.gateways.forex.mt5.symbol_resolver import SymbolResolver
from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger
from worker.schemas.trade_result import TradeResult

logger = get_logger("worker.gateways.forex.mt5.gateway")

_MT5_COMMENT_MAX = 31


def _mt5_error_code(err) -> int:
  """mt5.last_error() returns (code, description); extract just the int."""
  return err[0] if isinstance(err, tuple) else int(err)


class MT5Gateway(BasePlatformGateway):
  """MetaTrader 5 implementation of :class:`BasePlatformGateway`."""

  name = "MT5"

  def __init__(
    self,
    server: str,
    login: int,
    password: str,
    path: Optional[str] = None,
    slippage_deviation: int = 20,
    mt5_api: Optional[Mt5GatewayProtocol] = None,
  ) -> None:
    if mt5_api is None:
      import MetaTrader5 as mt5_api  # lazy: native extension only needed at runtime

    self._mt5 = mt5_api
    self._deviation = slippage_deviation
    self._bridge = MT5(server=server, login=login, password=password, path=path)
    self._resolver = SymbolResolver(mt5_api)

  # ── Lifecycle (delegated to the bridge) ───────────────────────────────── #

  def connect(self) -> bool:
    return self._bridge.connect()

  def close(self) -> None:
    self._bridge.shutdown()

  def is_connected(self) -> bool:
    return self._bridge.is_connected()

  def reconnect(self, max_attempts: int = 0, delay_seconds: float = 10.0) -> bool:
    return self._bridge.reconnect(
      max_attempts=max_attempts, delay_seconds=delay_seconds
    )

  def restart_terminal(self, startup_wait: float = 15.0) -> bool:
    return self._bridge.restart_terminal(startup_wait=startup_wait)

  # ── Account ───────────────────────────────────────────────────────────── #

  def get_account(self) -> Optional[Dict[str, Any]]:
    return self._bridge.get_account_status()

  def get_account_footer(self) -> str:
    return self._bridge.get_account_footer()

  # ── Market data / rules ───────────────────────────────────────────────── #

  def resolve_symbol(self, base_symbol: str) -> str:
    return self._resolver.get_symbol(base_symbol)

  def get_symbol_spec(self, symbol: str) -> Optional[SymbolSpec]:
    si = self._mt5.symbol_info(symbol)
    if not si:
      logger.error(f"Cannot find symbol {symbol}")
      return None
    return SymbolSpec(
      resolved_name=si.name,
      point=si.point,
      digits=si.digits,
      volume_min=si.volume_min,
      volume_step=si.volume_step,
      volume_max=si.volume_max,
      tick_value=si.trade_tick_value,
      tick_size=si.trade_tick_size,
      contract_size=si.trade_contract_size,
      stops_level=si.trade_stops_level,
    )

  def get_tick(self, symbol: str) -> Optional[Tick]:
    t = self._mt5.symbol_info_tick(symbol)
    if not t:
      return None
    return Tick(bid=t.bid, ask=t.ask)

  # ── Positions ─────────────────────────────────────────────────────────── #

  def get_positions(self, symbol: Optional[str] = None) -> List[PlatformPosition]:
    raw = (
      self._mt5.positions_get(symbol=symbol)
      if symbol is not None
      else self._mt5.positions_get()
    )
    if raw is None:
      return []
    return [
      PlatformPosition(
        ticket=p.ticket,
        magic=p.magic,
        side=SIDE_LONG if p.type == self._mt5.ORDER_TYPE_BUY else SIDE_SHORT,
        volume=p.volume,
        price_open=p.price_open,
        sl=getattr(p, "sl", 0.0),
        tp=getattr(p, "tp", 0.0),
        symbol=p.symbol,
      )
      for p in raw
    ]

  # ── Orders ────────────────────────────────────────────────────────────── #

  def place_order(
    self,
    symbol: str,
    side: str,
    volume: float,
    price: float,
    sl: Optional[float],
    tp: Optional[float],
    magic: int,
    comment: str,
  ) -> TradeResult:
    order_type = (
      self._mt5.ORDER_TYPE_BUY if side == SIDE_LONG else self._mt5.ORDER_TYPE_SELL
    )
    request: Dict[str, Any] = {
      "action": self._mt5.TRADE_ACTION_DEAL,
      "symbol": symbol,
      "volume": float(volume),
      "type": order_type,
      "price": float(price),
      "deviation": self._deviation,
      "magic": magic,
      "comment": comment[: _MT5_COMMENT_MAX - 1],
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }
    if sl is not None:
      request["sl"] = float(sl)
    if tp is not None:
      request["tp"] = float(tp)

    logger.info(f"[place_order] Sending Order: {request}")
    result = self._mt5.order_send(request)

    if result is None:
      logger.error(f"order_send failed. error code: {self._mt5.last_error()}")
      return TradeResult.fail(
        "Send Failed", retcode=_mt5_error_code(self._mt5.last_error())
      )

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"Order rejected, retcode={result.retcode}, comment: {result.comment}"
      )
      return TradeResult.fail(result.comment, retcode=result.retcode)

    logger.info(
      f"[place_order] Filled! Ticket: {result.order}, Price: {result.price}, Vol: {result.volume}"
    )
    return TradeResult.ok(
      retcode=result.retcode,
      ticket=str(result.order),
      price=result.price,
      volume=result.volume,
    )

  def close_position(
    self,
    position: PlatformPosition,
    volume: Optional[float] = None,
    comment: str = "Close",
  ) -> TradeResult:
    close_type = (
      self._mt5.ORDER_TYPE_SELL
      if position.side == SIDE_LONG
      else self._mt5.ORDER_TYPE_BUY
    )
    tick = self.get_tick(position.symbol)
    if tick is None:
      return TradeResult.fail("No tick / market data unavailable")
    price = tick.bid if close_type == self._mt5.ORDER_TYPE_SELL else tick.ask
    close_volume = position.volume if volume is None else volume

    request: Dict[str, Any] = {
      "action": self._mt5.TRADE_ACTION_DEAL,
      "symbol": position.symbol,
      "volume": float(close_volume),
      "type": close_type,
      "position": position.ticket,  # CRITICAL: links close to the specific ticket
      "price": float(price),
      "deviation": self._deviation,
      "magic": position.magic,  # close deal inherits the position's own magic
      "comment": comment,
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }

    logger.info(
      f"[close_position] Closing ticket {position.ticket}, vol={close_volume}"
    )
    result = self._mt5.order_send(request)

    if result is None:
      logger.error(
        f"[close_position] order_send failed. error: {self._mt5.last_error()}"
      )
      return TradeResult.fail(
        "Close Failed", retcode=_mt5_error_code(self._mt5.last_error())
      )

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"[close_position] Rejected, retcode={result.retcode}, comment: {result.comment}"
      )
      return TradeResult.fail(result.comment, retcode=result.retcode)

    logger.info(f"[close_position] Closed ticket {position.ticket} successfully")
    return TradeResult.ok(
      retcode=result.retcode,
      ticket=str(result.order),
      price=result.price,
      volume=result.volume,
    )

  def modify_sl(self, position: PlatformPosition, new_sl: float) -> TradeResult:
    request: Dict[str, Any] = {
      "action": self._mt5.TRADE_ACTION_SLTP,
      "symbol": position.symbol,
      "position": position.ticket,
      "sl": float(new_sl),
      "tp": float(position.tp),  # preserve existing TP
    }

    logger.info(f"[modify_sl] Updating SL for ticket {position.ticket} → {new_sl}")
    result = self._mt5.order_send(request)

    if result is None:
      logger.error(f"modify_sl order_send failed. error: {self._mt5.last_error()}")
      return TradeResult.fail(
        "SL Update Failed", retcode=_mt5_error_code(self._mt5.last_error())
      )

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      logger.error(
        f"SL update rejected, retcode={result.retcode}, comment: {result.comment}"
      )
      return TradeResult.fail(result.comment, retcode=result.retcode)

    logger.info(f"[modify_sl] SL updated successfully for ticket {position.ticket}")
    return TradeResult.ok(
      retcode=result.retcode,
      ticket=str(position.ticket),
      new_sl=new_sl,
    )

  # ── Event ingestion ───────────────────────────────────────────────────── #

  def create_close_detection_job(self, magic_numbers, db_service, notifier):
    from worker.jobs.mt5_event_job import MT5EventJob

    return MT5EventJob(
      magic_numbers=magic_numbers,
      db_service=db_service,
      notifier=notifier,
      # Poll only while the terminal is actually reachable — the job must keep
      # scanning through the FOREX weekend for 24/7 symbols, so the connection
      # (not the calendar) is what tells it there is nothing to read.
      is_connected=self.is_connected,
    )
