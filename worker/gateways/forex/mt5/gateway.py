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

import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

from worker.gateways.errors import PositionsUnavailableError
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
from worker.schemas.trade_result import TradeFailureEnum, TradeResult

logger = get_logger("worker.gateways.forex.mt5.gateway")

# Longest a caller waits for another thread's session change (reconnect, restart,
# weekend park) before giving up. The listener holds the pipeline lock while it
# waits, so this bounds how long a health-thread reconnect can stall it.
_SESSION_LOCK_WAIT = 20.0  # seconds

# Longest order comment the MetaTrader5 Python wrapper accepts. Measured against
# 5.0.5735: a request whose comment reaches 30 characters is refused client-side
# with ``(-2, 'Invalid "comment" argument')`` before the terminal ever sees it,
# so one character too many costs the whole order, not just its label. The MQL5
# struct's own 31 is not the binding limit here — the wrapper's check is.
_MT5_COMMENT_MAX = 29

# A deal is registered on the server before ``order_send`` returns its ticket,
# but the terminal's own history trails that — by measurably more than the few
# milliseconds first assumed. A live close that read 0.3 s later still found
# nothing, while the reconciler reading the *same position* a minute on returned
# its full history, so the window is widened to ~1.25 s before giving up. It stays
# bounded because the close has already succeeded: everything after it (the DB
# status write, the notification) waits on this read, so the budget buys a PnL
# line at the cost of delaying them, and must never grow into a stall.
_DEAL_LOOKUP_ATTEMPTS = 6
_DEAL_LOOKUP_DELAY = 0.25  # seconds between attempts

# "Trade context is busy": the terminal serialises every trade request through a
# single trade context, so a request that arrives while another one is still in
# flight is refused outright rather than queued. Two signals landing together (a
# TP1 partial and the SL move that follows it, or two symbols filling on the same
# tick) hit it routinely, and the refusal says nothing about the order itself —
# the very same request succeeds once the context frees up. So it is retried with
# a short sleep between attempts instead of being reported as a failed trade.
#
# The budget stays small on purpose: an entry is a market order priced off the
# live quote, so seconds of retrying would fill it at a price the signal never
# meant. Four attempts at a doubling delay spend ~2.1 s at worst, then the
# refusal is surfaced as the failure it now really is.
_BUSY_SEND_ATTEMPTS = 4
_BUSY_SEND_DELAY = 0.3  # seconds before the first retry, doubling after that
# TRADE_RETCODE_TOO_MANY_REQUESTS — the server-side sibling of a busy context:
# same transient refusal, same remedy. Named rather than read off the module so a
# terminal build that omits the constant cannot break the check.
_BUSY_RETCODES = frozenset({10024})

# Outcomes that leave it open whether the trade server executed the request.
# TRADE_RETCODE_TIMEOUT (10012) is "request cancelled by timeout": the terminal
# stopped waiting, which says nothing about what the server did with the order.
# Reporting that as a plain failure is a guess — if it did fill, the position is
# live with no stop and no DB row, which is the same harm the Binance side was
# fixed for. Named rather than read off the module so a terminal build that omits
# the constant cannot break the check.
_UNKNOWN_OUTCOME_RETCODES = frozenset({10012})
# Client-side ``last_error`` codes that leave the same question open when
# ``order_send`` returns no result at all: RES_E_INTERNAL_FAIL_RECEIVE (-10002),
# the request went out and the answer was lost, and
# RES_E_INTERNAL_FAIL_TIMEOUT (-10005). Deliberately narrow — _SEND (-10001),
# _CONNECT (-10004) and an argument the wrapper itself refused (-2) all mean the
# request never reached the trade server, so those outcomes are known, and
# treating them as unknown would block replays of signals that never executed.
_UNKNOWN_OUTCOME_ERRORS = frozenset({-10002, -10005})

# Pauses while re-reading the position book to see whether an unknown-outcome
# order actually landed. Same shape and budget as the deal lookup above: the
# position appears on the server before the terminal's own book catches up.
_ORDER_LOOKUP_ATTEMPTS = 6
_ORDER_LOOKUP_DELAY = 0.25  # seconds between attempts


def _fit_comment(comment: str) -> str:
  """Cut *comment* down to what the wrapper accepts (see ``_MT5_COMMENT_MAX``).

  Every ``order_send`` goes through this: the caller composes the comment for a
  reader, not for the wrapper's limit, and a comment that overflows is not a
  cosmetic problem — the order is never sent. Trailing whitespace goes too, so a
  cut that lands on a separator does not leave a dangling gap in the terminal.
  """
  return comment[:_MT5_COMMENT_MAX].rstrip()


def _mt5_error_code(err) -> int:
  """mt5.last_error() returns (code, description); extract just the int."""
  return err[0] if isinstance(err, tuple) else int(err)


def _is_trade_context_busy(result: Any, error: Any) -> bool:
  """Is this rejected ``order_send`` outcome the busy trade context?

  Matched on the text as well as on the retcode because the condition reaches the
  worker by two different routes: a rejected order carries it in ``comment``,
  while a request the terminal refused to send at all returns ``None`` and leaves
  it in ``last_error()``. The text is the only thing common to both, and the
  wording ("Trade context is busy") is the terminal's own.
  """
  if result is None:
    return "busy" in str(error).lower()
  if getattr(result, "retcode", None) in _BUSY_RETCODES:
    return True
  return "busy" in str(getattr(result, "comment", "") or "").lower()


def _is_outcome_unknown(result: Any, error: Any) -> bool:
  """Could the trade server have executed this request anyway?

  Two routes, mirroring :func:`_is_trade_context_busy`: a request the terminal
  gave up waiting for comes back with a timeout retcode, while one it could not
  complete at all returns ``None`` and leaves the reason in ``last_error()``.
  Only the connection/timeout errors qualify there — an argument the wrapper
  rejected (``-2``) never reached the server, so that outcome is known.
  """
  if result is None:
    return _mt5_error_code(error) in _UNKNOWN_OUTCOME_ERRORS
  return getattr(result, "retcode", None) in _UNKNOWN_OUTCOME_RETCODES


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
  order_comment_max = _MT5_COMMENT_MAX

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
    # One session change at a time: the listener (before a message) and the
    # health thread both reconnect, and the health thread also restarts and
    # parks the terminal — interleaved, they tear down each other's session.
    self._reconnect_lock = threading.Lock()

  # ── Lifecycle (delegated to the bridge) ───────────────────────────────── #

  def connect(self) -> bool:
    return self._bridge.connect()

  def close(self) -> None:
    with self._session_change() as owned:
      if owned:
        self._bridge.shutdown()

  def is_connected(self) -> bool:
    return self._bridge.is_connected()

  def reconnect(self, max_attempts: int = 0, delay_seconds: float = 10.0) -> bool:
    with self._session_change() as owned:
      # Either the other thread restored the session while this one waited, or
      # it is still busy and this caller gives up rather than queue behind it.
      if not owned or self._bridge.is_connected():
        return self._bridge.is_connected()
      return self._bridge.reconnect(
        max_attempts=max_attempts, delay_seconds=delay_seconds
      )

  def restart_terminal(self, startup_wait: float = 15.0) -> bool:
    with self._session_change() as owned:
      if not owned:
        return False
      return self._bridge.restart_terminal(startup_wait=startup_wait)

  @contextmanager
  def _session_change(self) -> Iterator[bool]:
    """Hold the session lock, waiting at most :data:`_SESSION_LOCK_WAIT`.

    Yields whether the lock was taken. Bounded so the listener — which holds the
    pipeline lock — never waits out the health thread's long reconnect run (15
    attempts, ~150 s); the health thread, which has no such constraint, simply
    tries again on its next check.
    """
    owned = self._reconnect_lock.acquire(timeout=_SESSION_LOCK_WAIT)
    try:
      yield owned
    finally:
      if owned:
        self._reconnect_lock.release()

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
      # positions_get() returns None on an error (terminal offline, IPC lost) and
      # an empty tuple for a flat book. Mapping None to [] made every caller read
      # an outage as "no positions" — an ADMIN FLAT then marked every tracked row
      # FLATTED while the positions were still live.
      raise PositionsUnavailableError(
        f"MT5 positions_get failed: {self._mt5.last_error()}"
      )
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

  def _position_tickets(self, symbol: str, magic: int) -> Optional[Dict[Any, Any]]:
    """This strategy's live positions on *symbol*, keyed by ticket.

    ``None`` when the book could not be read, which is kept apart from an empty
    book: a snapshot that failed cannot be compared against a later one, and
    guessing either way is what this whole path exists to avoid.
    """
    try:
      return {p.ticket: p for p in self.get_positions(symbol) if p.magic == magic}
    except Exception as exc:
      logger.warning(
        "[place_order] Could not read the %s book (magic=%s): %s", symbol, magic, exc
      )
      return None

  def _resolve_unknown_entry(
    self,
    symbol: str,
    magic: int,
    before: Optional[Dict[Any, Any]],
    cause: str,
  ) -> TradeResult:
    """Settle an entry whose ``order_send`` ended without a definitive answer.

    Reporting it as failed would be a guess: if the trade server did execute it,
    the position is live with no stop attached and no DB row tracking it. So the
    position book is re-read a few times and compared against the snapshot taken
    before the send. Exactly one new ticket for this strategy on this symbol is
    the order; anything else — nothing new, or more than one — stays a failure
    flagged ``OUTCOME_UNKNOWN``, which the signal path records as an unresolved
    attempt rather than a refusal, so the signal is neither replayed nor written
    off while a position may be live.
    """
    logger.error(
      "[place_order] %s outcome unknown (%s) — checking whether it landed.",
      symbol,
      cause,
    )
    if before is None:
      logger.critical(
        "[place_order] %s outcome UNKNOWN (%s) and the book was unreadable "
        "before the send, so nothing can be compared. A position may be open "
        "without a stop — check the terminal.",
        symbol,
        cause,
      )
      return TradeResult.fail(
        f"Order outcome unknown ({cause}) — check the terminal",
        failure=TradeFailureEnum.OUTCOME_UNKNOWN,
      )

    for attempt in range(_ORDER_LOOKUP_ATTEMPTS):
      after = self._position_tickets(symbol, magic)
      if after is not None:
        new = [p for ticket, p in after.items() if ticket not in before]
        if len(new) == 1:
          pos = new[0]
          logger.warning(
            "[place_order] %s was executed despite the lost answer (%s) — "
            "ticket %s, vol %s.",
            symbol,
            cause,
            pos.ticket,
            pos.volume,
          )
          return TradeResult.ok(
            ticket=str(pos.ticket),
            price=pos.price_open,
            volume=pos.volume,
            sl=pos.sl or None,
            tp=pos.tp or None,
          )
        if len(new) > 1:
          # Two positions appeared where one order was sent. Which is ours is
          # unanswerable, and picking one could attach a stop to the wrong
          # ticket, so this is reported rather than resolved.
          logger.critical(
            "[place_order] %s outcome UNKNOWN (%s): %d new positions appeared "
            "(%s) — cannot tell which is this order. Check the terminal.",
            symbol,
            cause,
            len(new),
            ", ".join(str(p.ticket) for p in new),
          )
          return TradeResult.fail(
            f"Order outcome unknown ({cause}) — {len(new)} new positions",
            failure=TradeFailureEnum.OUTCOME_UNKNOWN,
          )
      if attempt < _ORDER_LOOKUP_ATTEMPTS - 1:
        time.sleep(_ORDER_LOOKUP_DELAY)

    # Nothing appeared in the window. The order most likely never reached the
    # server — but "most likely" is not knowledge, and a late fill would leave
    # an unprotected position, so it stays unknown and loud. A missed trade with
    # an alert beats a live position nobody is tracking.
    logger.critical(
      "[place_order] %s outcome STILL UNKNOWN (%s) after %.2fs — no new position "
      "appeared, but a late fill would be unprotected. Check the terminal.",
      symbol,
      cause,
      (_ORDER_LOOKUP_ATTEMPTS - 1) * _ORDER_LOOKUP_DELAY,
    )
    return TradeResult.fail(
      f"Order outcome unknown ({cause}) — check the terminal",
      failure=TradeFailureEnum.OUTCOME_UNKNOWN,
    )

  def _send_order_request(self, request: Dict[str, Any], label: str) -> Any:
    """``order_send``, retried while the terminal reports a busy trade context.

    Every order the worker places goes through here, so open/close/SL all get the
    same treatment: a busy context is a scheduling collision, not a verdict on the
    request. Anything else — a real rejection, a fill — is returned untouched on
    the first attempt, and the caller keeps owning the interpretation.
    """
    for attempt in range(1, _BUSY_SEND_ATTEMPTS + 1):
      result = self._mt5.order_send(request)
      if result is not None and result.retcode == self._mt5.TRADE_RETCODE_DONE:
        return result

      error = self._mt5.last_error() if result is None else None
      if not _is_trade_context_busy(result, error):
        return result

      if attempt == _BUSY_SEND_ATTEMPTS:
        logger.error(
          f"[{label}] Trade context still busy after {attempt} attempts — "
          f"giving up on {request.get('symbol')}."
        )
        return result

      delay = _BUSY_SEND_DELAY * (2 ** (attempt - 1))
      logger.warning(
        f"[{label}] Trade context is busy (attempt {attempt}/"
        f"{_BUSY_SEND_ATTEMPTS}) — retrying in {delay:.2f}s."
      )
      time.sleep(delay)

    # Unreachable: the loop returns on its final attempt.
    return None

  def calc_margin(
    self, symbol: str, side: str, volume: float, price: float
  ) -> Optional[float]:
    """Margin the terminal quotes for *volume* lots of *symbol* at *price*.

    ``order_calc_margin`` asks the server the same question the trade server
    asks itself when the order arrives, so the figure includes the symbol's own
    leverage (metals and indices are commonly capped well below the account's
    headline leverage). ``None`` on any failure — the pre-flight is a safety net,
    never a reason to block an entry the terminal simply could not price.
    """
    order_type = (
      self._mt5.ORDER_TYPE_BUY if side == SIDE_LONG else self._mt5.ORDER_TYPE_SELL
    )
    try:
      margin = self._mt5.order_calc_margin(
        order_type, symbol, float(volume), float(price)
      )
    except Exception as e:  # the native extension raises on an unknown symbol
      logger.warning(f"[calc_margin] order_calc_margin failed for {symbol}: {e}")
      return None

    if margin is None:
      logger.warning(
        f"[calc_margin] Terminal priced no margin for {symbol} vol={volume}: "
        f"{self._mt5.last_error()}"
      )
      return None
    return float(margin)

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
      "comment": _fit_comment(comment),
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }
    if sl is not None:
      request["sl"] = float(sl)
    if tp is not None:
      request["tp"] = float(tp)

    logger.info(f"[place_order] Sending Order: {request}")
    # The book as it stands before the order, so an outcome the terminal cannot
    # report can still be settled by what appeared. MT5 accepts no client-side
    # order id, so there is nothing to look the order up by afterwards — this
    # snapshot is the only handle on "did my order land".
    before = self._position_tickets(symbol, magic)
    result = self._send_order_request(request, "place_order")

    if result is None:
      error = self._mt5.last_error()
      logger.error(f"order_send failed. error code: {error}")
      if _is_outcome_unknown(result, error):
        return self._resolve_unknown_entry(symbol, magic, before, str(error))
      return TradeResult.fail("Send Failed", retcode=_mt5_error_code(error))

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      if _is_outcome_unknown(result, None):
        logger.error(
          f"[place_order] Outcome unknown, retcode={result.retcode}, "
          f"comment: {result.comment} (symbol={symbol} volume={volume})"
        )
        return self._resolve_unknown_entry(
          symbol, magic, before, f"retcode={result.retcode} {result.comment}"
        )
      logger.error(
        f"Order rejected, retcode={result.retcode}, comment: {result.comment} "
        f"(symbol={symbol} volume={volume})"
      )
      # Carry the refused volume: a rejection is usually *about* the volume
      # (10019 "No money", 10014 "Invalid volume"), and the handler's failure
      # line is all the operator gets.
      return TradeResult.fail(
        result.comment, retcode=result.retcode, volume=float(volume)
      )

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
      "comment": _fit_comment(comment),
      "type_time": self._mt5.ORDER_TIME_GTC,
      "type_filling": self._mt5.ORDER_FILLING_IOC,
    }

    logger.info(
      f"[close_position] Closing ticket {position.ticket}, vol={close_volume}"
    )
    result = self._send_order_request(request, "close_position")

    if result is None:
      error = self._mt5.last_error()
      logger.error(f"[close_position] order_send failed. error: {error}")
      if _is_outcome_unknown(result, error):
        return self._resolve_unknown_close(position, close_volume, str(error))
      return TradeResult.fail("Close Failed", retcode=_mt5_error_code(error))

    if result.retcode != self._mt5.TRADE_RETCODE_DONE:
      if _is_outcome_unknown(result, None):
        logger.error(
          f"[close_position] Outcome unknown, retcode={result.retcode}, "
          f"comment: {result.comment}"
        )
        return self._resolve_unknown_close(
          position, close_volume, f"retcode={result.retcode} {result.comment}"
        )
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
      profit=self._closing_deal_pnl(result, position.ticket),
    )

  def _resolve_unknown_close(
    self, position: PlatformPosition, close_volume: float, cause: str
  ) -> TradeResult:
    """Settle a close whose ``order_send`` ended without a definitive answer.

    Easier than the entry case: the request named the ticket, so the question is
    just what happened to that one position. Gone, or smaller by the volume
    asked for, means the close went through. Unchanged after the window means it
    very likely did not — but a late fill would leave the DB row open against a
    flat broker, so it is reported ``OUTCOME_UNKNOWN`` rather than as a refusal
    the exit logic would act on.
    """
    logger.error(
      "[close_position] ticket %s outcome unknown (%s) — checking whether it closed.",
      position.ticket,
      cause,
    )
    for attempt in range(_ORDER_LOOKUP_ATTEMPTS):
      live = self._position_tickets(position.symbol, position.magic)
      if live is not None:
        still = live.get(position.ticket)
        if still is None:
          logger.warning(
            "[close_position] ticket %s did close despite the lost answer (%s).",
            position.ticket,
            cause,
          )
          return TradeResult.ok(
            ticket=str(position.ticket),
            volume=close_volume,
            profit=self.get_position_realized_pnl(position.ticket),
          )
        if still.volume < position.volume:
          # A partial close landed. Report the volume that actually went, not
          # the volume asked for, so the tracked row stays true to the broker.
          closed = position.volume - still.volume
          logger.warning(
            "[close_position] ticket %s partially closed despite the lost "
            "answer (%s): %s of %s.",
            position.ticket,
            cause,
            closed,
            close_volume,
          )
          return TradeResult.ok(
            ticket=str(position.ticket),
            volume=closed,
            profit=self.get_position_realized_pnl(position.ticket),
          )
      if attempt < _ORDER_LOOKUP_ATTEMPTS - 1:
        time.sleep(_ORDER_LOOKUP_DELAY)

    logger.critical(
      "[close_position] ticket %s outcome STILL UNKNOWN (%s) after %.2fs — it is "
      "still live, but a late fill would leave the tracked row open against a "
      "flat broker. Check the terminal.",
      position.ticket,
      cause,
      (_ORDER_LOOKUP_ATTEMPTS - 1) * _ORDER_LOOKUP_DELAY,
    )
    return TradeResult.fail(
      f"Close outcome unknown ({cause}) — check the terminal",
      failure=TradeFailureEnum.OUTCOME_UNKNOWN,
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
    result = self._send_order_request(request, "modify_sl")

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

  def _closing_deal_pnl(
    self, order_result: Any, position_ticket: Any
  ) -> Optional[float]:
    """Realized PnL of the deals a successful close just produced, or ``None``.

    ``order_send`` reports *what happened to the order* — it carries no money
    figure — so the amount is read back out of deal history. Those deals are the
    single authority on what the close booked: MT5 has already netted the
    position's entry price against the fill and charged the account's
    commission/swap, none of which the worker can derive itself from lots and
    prices (it would need the symbol's contract size and the account-currency
    conversion, and would still miss the costs).

    The read goes through the **position** (``DEAL_POSITION_ID``) and the deals it
    returns are narrowed *here* to the ones this close's order produced
    (``DEAL_ORDER``). Asking the terminal to do that narrowing — the more direct
    ``history_deals_get(ticket=<order>)`` — returned nothing on a live account,
    for a position whose history the reconciler read in full a minute later
    through the position filter. So the query uses the filter with evidence behind
    it and the narrowing is done in Python, which also keeps a partial close from
    reporting the position's running total: an entry deal (and any earlier TP1)
    sits in the same result and belongs to a different order.

    Best-effort throughout: the close has already succeeded by the time this
    runs, so a terminal that cannot answer costs a missing PnL line, never the
    trade result.
    """
    order_id = _as_ticket_id(getattr(order_result, "order", None))
    if not order_id:
      logger.debug(
        "[close_position] order_send reported no order ticket — PnL unknown."
      )
      return None

    deals: List[Any] = []
    for attempt in range(_DEAL_LOOKUP_ATTEMPTS):
      found = self._history_deals("position", position_ticket)
      if found is None:
        return None  # the terminal refused the read outright; it already logged
      deals = found
      mine = [d for d in deals if getattr(d, "order", None) == order_id]
      if mine:
        return deals_realized_pnl(mine)
      if attempt < _DEAL_LOOKUP_ATTEMPTS - 1:
        time.sleep(_DEAL_LOOKUP_DELAY)

    # Deliberately detailed: it separates "history is unreadable" from "history
    # is readable but this close is not in it yet", which is the one thing a
    # missing PnL line cannot tell an operator by itself.
    logger.warning(
      "[pnl] Position %s holds %d deal(s), none from close order %s after %.2fs "
      "— PnL unknown for this close.",
      position_ticket,
      len(deals),
      order_id,
      (_DEAL_LOOKUP_ATTEMPTS - 1) * _DEAL_LOOKUP_DELAY,
    )
    return None

  def get_position_realized_pnl(self, position_ticket: Any) -> Optional[float]:
    """Net realized PnL of a whole position, summed over all of its deals.

    Used by the reconciler, which learns a position is gone long after the fact
    and has no closing deal in hand — only the ticket. Every deal the position
    ever produced is summed, so the figure covers the entry's commission and any
    earlier partial close as well as the final exit: a *position total* rather
    than the result of one close, which is why the caller labels it as such.

    No retry: by the time the reconciler notices, the close is at least a poll
    interval old, so an empty history means unreadable rather than not-yet-landed.
    """
    pnl = deals_realized_pnl(self._history_deals("position", position_ticket))
    if pnl is None:
      logger.warning(
        "[pnl] No deal history for position=%s — PnL unknown.", position_ticket
      )
    return pnl

  def _history_deals(self, key: str, ticket: Any) -> Optional[List[Any]]:
    """Deals matching ``{key}=ticket``, or ``None`` when the read itself failed.

    *key* selects the ``history_deals_get`` filter — ``position`` for every deal
    of one position (``DEAL_POSITION_ID``), ``ticket`` for the deals of one order
    (``DEAL_ORDER``). Neither matches a deal by its own ticket.

    An empty list and ``None`` are kept apart: the first means the terminal
    answered and holds nothing yet (worth retrying), the second that it could not
    answer at all (retrying is pointless).
    """
    reader = getattr(self._mt5, "history_deals_get", None)
    if reader is None:  # pragma: no cover - the real module always has it
      logger.debug("[pnl] history_deals_get unavailable — PnL unknown.")
      return None

    ticket_id = _as_ticket_id(ticket)
    if ticket_id is None:
      logger.warning("[pnl] %s=%r is not a numeric ticket — PnL unknown.", key, ticket)
      return None

    try:
      return list(reader(**{key: ticket_id}) or [])
    except Exception as exc:
      logger.warning("[pnl] history_deals_get(%s=%s) failed: %s", key, ticket, exc)
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
