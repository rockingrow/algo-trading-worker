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

import time
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
from worker.gateways.forex.mt5.deal_pnl import deals_realized_pnl
from worker.gateways.forex.mt5.symbol_resolver import SymbolResolver
from worker.interfaces.mt5_gateway_protocol import Mt5GatewayProtocol
from worker.logger import get_logger
from worker.schemas.trade_result import TradeResult

logger = get_logger("worker.gateways.forex.mt5.gateway")

_MT5_COMMENT_MAX = 31

# A deal is registered on the server before ``order_send`` returns its ticket,
# but the terminal's local history can trail that by a few milliseconds. Retry
# the lookup a couple of times before giving up, so a close reports its PnL
# instead of "n/a" for want of a moment's patience — bounded tightly because the
# close itself has already succeeded and nothing downstream waits on this.
_DEAL_LOOKUP_ATTEMPTS = 3
_DEAL_LOOKUP_DELAY = 0.15  # seconds between attempts


def _mt5_error_code(err) -> int:
  """mt5.last_error() returns (code, description); extract just the int."""
  return err[0] if isinstance(err, tuple) else int(err)


def _as_ticket_id(ticket: Any) -> Optional[int]:
  """Coerce *ticket* to the ``int`` the MT5 history API requires, or ``None``.

  ``history_deals_get`` is a native extension: its ``ticket`` / ``position``
  arguments are unsigned integers and anything else raises rather than searching.
  Tickets the worker holds from its own ``order_send`` are already ints, but the
  reconciler reads its ticket out of the DB — where every reference is stored as
  TEXT (see ``worker/db/schema.py``) — so ``"798267992"`` reached the terminal as
  a string, the call raised, and the reconcile notification reported
  ``PnL (position total): n/a`` for every position it ever synced.

  Normalising here rather than at each call site keeps the one place that talks
  to the extension responsible for the extension's types.
  """
  try:
    return int(str(ticket).strip())
  except (TypeError, ValueError):
    return None


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
    bid, ask = t.bid, t.ask
    # ``symbol_info_tick`` returns a namedtuple, which is truthy even when every
    # field is zero — and that is exactly what the terminal reports for a symbol
    # it holds no quote for (just added to Market Watch, or outside its trading
    # session). Such a tick must not reach the pricing/stop math: an entry priced
    # off 0.0 turns SL/TP into nonsense the broker rejects with retcode 10016
    # (TRADE_RETCODE_INVALID_STOPS). Report "no data" so callers fail fast.
    if bid <= 0 or ask <= 0:
      logger.warning(
        f"[get_tick] {symbol} has no live quote (bid={bid} ask={ask}) — "
        "symbol not yet synced in Market Watch or market closed."
      )
      return None
    return Tick(bid=bid, ask=ask)

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
      f"[place_order] Filled! Ticket: {result.order}, Price: {result.price}, "
      f"Vol: {result.volume}, SL: {sl}, TP: {tp}"
    )
    # Report the stops as *sent*, not as the signal asked for them: the caller
    # already ran them through StopValidator, so these are what the position
    # actually carries and what the DB/notification must record.
    return TradeResult.ok(
      retcode=result.retcode,
      ticket=str(result.order),
      price=result.price,
      volume=result.volume,
      sl=sl,
      tp=tp,
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
      profit=self._closing_deal_pnl(result),
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

  # ── Realized PnL ──────────────────────────────────────────────────────── #

  def _closing_deal_pnl(self, order_result: Any) -> Optional[float]:
    """Realized PnL of the deals a successful close just produced, or ``None``.

    ``order_send`` reports *what happened to the order* — it carries no money
    figure — so the amount is read back off the deals it created. Those deals are
    the single authority on what the close booked: MT5 has already netted the
    position's entry price against the fill and charged the account's
    commission/swap, none of which the worker can derive itself from lots and
    prices (it would need the symbol's contract size and the account-currency
    conversion, and would still miss the costs).

    The lookup goes through the **order** ticket, not ``OrderSendResult.deal``:
    ``history_deals_get(ticket=…)`` filters on ``DEAL_ORDER`` — the order a deal
    came *from* — and never on a deal's own ticket, so passing the deal ticket
    matched no deal at all and every worker-placed close reported ``PnL: n/a``.
    Going through the order is also what makes a close that the broker filled in
    several deals report their sum rather than one slice of itself.

    Best-effort throughout: the close has already succeeded by the time this
    runs, so a terminal that cannot answer costs a missing PnL line, never the
    trade result.
    """
    order_ticket = getattr(order_result, "order", None)
    if not order_ticket:
      logger.debug(
        "[close_position] order_send reported no order ticket — PnL unknown."
      )
      return None
    return self.get_order_realized_pnl(order_ticket)

  def get_order_realized_pnl(self, order_ticket: Any) -> Optional[float]:
    """Net realized PnL of every deal *order_ticket* produced, or ``None``."""
    return self._history_pnl("ticket", order_ticket, retry=True)

  def get_position_realized_pnl(self, position_ticket: Any) -> Optional[float]:
    """Net realized PnL of a whole position, summed over all of its deals.

    Used by the reconciler, which learns a position is gone long after the fact
    and has no closing deal in hand — only the ticket. Every deal the position
    ever produced is summed, so the figure covers the entry's commission and any
    earlier partial close as well as the final exit: a *position total* rather
    than the result of one close, which is why the caller labels it as such.
    """
    return self._history_pnl("position", position_ticket, retry=False)

  def _history_pnl(self, key: str, ticket: Any, *, retry: bool) -> Optional[float]:
    """Sum the realized PnL of the deals matching ``{key}=ticket`` in history.

    *key* selects the ``history_deals_get`` filter — ``ticket`` for every deal one
    *order* produced (``DEAL_ORDER``), ``position`` for every deal of one position
    (``DEAL_POSITION_ID``). Neither filter matches a deal's own ticket. *retry*
    re-reads a moment later when the history came back empty, which is worth it
    for a deal that was created milliseconds ago and pointless for a position
    that closed long enough ago for the reconciler to notice.
    """
    reader = getattr(self._mt5, "history_deals_get", None)
    if reader is None:  # pragma: no cover - the real module always has it
      logger.debug("[pnl] history_deals_get unavailable — PnL unknown.")
      return None

    ticket_id = _as_ticket_id(ticket)
    if ticket_id is None:
      logger.warning("[pnl] %s=%r is not a numeric ticket — PnL unknown.", key, ticket)
      return None

    attempts = _DEAL_LOOKUP_ATTEMPTS if retry else 1
    for attempt in range(attempts):
      try:
        deals = reader(**{key: ticket_id})
      except Exception as exc:
        logger.warning("[pnl] history_deals_get(%s=%s) failed: %s", key, ticket, exc)
        return None
      pnl = deals_realized_pnl(deals)
      if pnl is not None:
        return pnl
      if attempt < attempts - 1:
        time.sleep(_DEAL_LOOKUP_DELAY)

    logger.warning(
      "[pnl] No deal history for %s=%s — PnL unknown for this close.", key, ticket
    )
    return None

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
