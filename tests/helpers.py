"""
tests/helpers.py
────────────────
Shared builder functions and fake objects for unit tests.
Imported directly by test files; fixtures are in conftest.py.
"""

import os
from types import SimpleNamespace
from typing import List, Optional

os.environ.setdefault("NATS_URL", "nats://localhost:4222")
os.environ.setdefault("NATS_SUBJECTS", "signal.test")
os.environ.setdefault("MT5_SERVER", "TestServer")
os.environ.setdefault("MT5_LOGIN", "1000")
os.environ.setdefault("MT5_PASSWORD", "pw")
os.environ.setdefault("TELEGRAM_ENABLED", "false")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("TELEGRAM_CHAT_ID", "123")
os.environ.setdefault("BROKER_API_URL", "http://localhost")
os.environ.setdefault("BROKER_API_KEY", "key")

from worker.gateways.crypto.base import (
  SIDE_LONG as CRYPTO_SIDE_LONG,
)
from worker.gateways.crypto.base import (
  BaseExchangeGateway,
  ExchangePosition,
  SymbolFilter,
)
from worker.gateways.forex.base import (
  SIDE_LONG,
  PlatformPosition,
  SymbolSpec,
  Tick,
)
from worker.schemas.signal_schema import SignalActionEnum, SignalSchema
from worker.schemas.trade_result import TradeResult


def make_symbol_info(**overrides):
  defaults = {
    "name": "XAUUSDc",
    "point": 0.01,
    "digits": 2,
    "volume_min": 0.01,
    "volume_max": 100.0,
    "volume_step": 0.01,
    "trade_tick_value": 1.0,
    "trade_tick_size": 0.01,
    "trade_contract_size": 100.0,
    "trade_stops_level": 10,
  }
  defaults.update(overrides)
  return SimpleNamespace(**defaults)


def make_tick(ask=2000.0, bid=1999.5):
  return SimpleNamespace(ask=ask, bid=bid)


def make_position(
  ticket=111,
  type_=0,
  volume=1.0,
  price_open=2000.0,
  tp=2050.0,
  magic=42,
  symbol="XAUUSDc",
):
  return SimpleNamespace(
    ticket=ticket,
    type=type_,
    volume=volume,
    price_open=price_open,
    tp=tp,
    magic=magic,
    symbol=symbol,
  )


def make_order_result(retcode=10009, order=999, price=2000.0, volume=1.0, comment="ok"):
  return SimpleNamespace(
    retcode=retcode, order=order, price=price, volume=volume, comment=comment
  )


def make_signal(action=SignalActionEnum.LONG, **overrides):
  defaults = {
    "strategy": "strat-1",
    "timestamp": "2026-05-29T10:00:00",
    "action": action,
    "symbol": "XAUUSD",
    "quantity": 100,
    "price": 2000.0,
    "sl": 1990.0,
    "tp1": 2020.0,
    "tp2": 2050.0,
  }
  defaults.update(overrides)
  return SignalSchema(**defaults)


class FakeMt5:
  """In-memory stand-in for the MetaTrader5 module (Mt5GatewayProtocol)."""

  SYMBOL_TRADE_MODE_DISABLED = 0
  ORDER_TYPE_BUY = 0
  ORDER_TYPE_SELL = 1
  TRADE_ACTION_DEAL = 1
  TRADE_ACTION_SLTP = 2
  ORDER_TIME_GTC = 0
  ORDER_FILLING_IOC = 1
  TRADE_RETCODE_DONE = 10009

  def __init__(
    self,
    symbol_info=None,
    tick=None,
    positions: Optional[List] = None,
    account=None,
    order_results: Optional[List] = None,
    symbols=None,
  ):
    self._symbol_info = symbol_info if symbol_info is not None else make_symbol_info()
    self._tick = tick if tick is not None else make_tick()
    self._positions = list(positions or [])
    self._account = account if account is not None else SimpleNamespace(equity=10000.0)
    self._order_results = list(order_results or [])
    self._symbols = (
      symbols
      if symbols is not None
      else [SimpleNamespace(name="XAUUSDc", trade_mode=4, visible=True)]
    )
    self.sent_requests: List[dict] = []
    self.selected: List[tuple] = []

  def symbols_get(self, group=None):
    return self._symbols

  def symbol_select(self, name, enable):
    self.selected.append((name, enable))
    return True

  def symbol_info(self, name):
    return self._symbol_info

  def symbol_info_tick(self, name):
    return self._tick

  def account_info(self):
    return self._account

  def positions_get(self, symbol=None):
    return list(self._positions)

  def order_send(self, request):
    self.sent_requests.append(request)
    if self._order_results:
      return self._order_results.pop(0)
    return make_order_result(volume=request.get("volume", 1.0))

  def last_error(self):
    return (1, "fake error")


# ── FOREX platform-gateway fakes (agnostic layer) ──────────────────────────── #


def make_symbol_spec(**overrides):
  defaults = {
    "resolved_name": "XAUUSDc",
    "point": 0.01,
    "digits": 2,
    "volume_min": 0.01,
    "volume_step": 0.01,
    "volume_max": 100.0,
    "tick_value": 1.0,
    "tick_size": 0.01,
    "contract_size": 100.0,
    "stops_level": 10,
  }
  defaults.update(overrides)
  return SymbolSpec(**defaults)


def make_platform_position(
  ticket=111,
  side=SIDE_LONG,
  volume=1.0,
  price_open=2000.0,
  tp=2050.0,
  sl=0.0,
  magic=42,
  symbol="XAUUSDc",
):
  return PlatformPosition(
    ticket=ticket,
    magic=magic,
    side=side,
    volume=volume,
    price_open=price_open,
    sl=sl,
    tp=tp,
    symbol=symbol,
  )


class FakePlatformGateway:
  """In-memory stand-in for a BasePlatformGateway (the agnostic seam below
  ForexExecutor). Records placed/closed/modified orders and returns configurable
  TradeResults so the executor's business logic is testable without MetaTrader5."""

  name = "FAKE"

  def __init__(
    self,
    spec=None,
    tick=None,
    positions=None,
    account=None,
    place_results=None,
    close_results=None,
    modify_results=None,
  ):
    self._spec = spec if spec is not None else make_symbol_spec()
    self._tick = tick if tick is not None else Tick(bid=1999.5, ask=2000.0)
    self._positions = list(positions or [])
    self._account = (
      account if account is not None else {"balance": 10000.0, "equity": 10000.0}
    )
    self._place_results = list(place_results or [])
    self._close_results = list(close_results or [])
    self._modify_results = list(modify_results or [])
    self.placed: List[dict] = []
    self.closed: List[dict] = []
    self.modified: List[dict] = []

  # ── Lifecycle (stubs) ─────────────────────────────────────────────────── #
  def connect(self):
    return True

  def is_connected(self):
    return True

  def reconnect(self, max_attempts=0, delay_seconds=10.0):
    return True

  def restart_terminal(self, startup_wait=15.0):
    return True

  def close(self):
    pass

  def get_account_footer(self, account_id=None):
    return "FOOTER"

  def create_close_detection_job(self, magic_numbers, db_service, notifier):
    return None

  # ── Data ──────────────────────────────────────────────────────────────── #
  def resolve_symbol(self, base_symbol):
    return base_symbol

  def get_symbol_spec(self, symbol):
    return self._spec

  def get_tick(self, symbol):
    return self._tick

  def get_account(self):
    return self._account

  def get_positions(self, symbol=None):
    return list(self._positions)

  # ── Orders ────────────────────────────────────────────────────────────── #
  def place_order(self, symbol, side, volume, price, sl, tp, magic, comment):
    self.placed.append(
      {
        "symbol": symbol,
        "side": side,
        "volume": volume,
        "price": price,
        "sl": sl,
        "tp": tp,
        "magic": magic,
        "comment": comment,
      }
    )
    if self._place_results:
      return self._place_results.pop(0)
    # Echo the stops back like a real gateway: they are what the position ends
    # up carrying, after the caller's StopValidator pass.
    return TradeResult.ok(
      retcode=10009, ticket="999", price=price, volume=volume, sl=sl, tp=tp
    )

  def close_position(self, position, volume=None, comment="Close"):
    vol = position.volume if volume is None else volume
    self.closed.append({"position": position, "volume": vol, "comment": comment})
    if self._close_results:
      return self._close_results.pop(0)
    return TradeResult.ok(retcode=10009, ticket="999", price=1999.5, volume=vol)

  def modify_sl(self, position, new_sl):
    self.modified.append({"position": position, "new_sl": new_sl})
    if self._modify_results:
      return self._modify_results.pop(0)
    return TradeResult.ok(retcode=10009, ticket=str(position.ticket), new_sl=new_sl)


# ── CRYPTO exchange-gateway fakes ─────────────────────────────────────────── #


def make_symbol_filter(**overrides):
  """Return a SymbolFilter with sensible BTC-futures defaults."""
  defaults = {
    "step_size": 0.001,  # 0.001 BTC minimum increment
    "min_qty": 0.001,
    "tick_size": 0.1,  # $0.10 price tick
  }
  defaults.update(overrides)
  return SymbolFilter(**defaults)


def make_exchange_position(
  symbol="BTCUSDT",
  side=CRYPTO_SIDE_LONG,
  quantity=0.02,
  entry_price=30000.0,
  ticket=7,
  tp=0.0,
  sl=0.0,
  magic=0,
  unrealized_pnl=0.0,
):
  """Return an ExchangePosition with sensible BTC defaults."""
  return ExchangePosition(
    symbol=symbol,
    side=side,
    quantity=quantity,
    entry_price=entry_price,
    ticket=ticket,
    tp=tp,
    sl=sl,
    magic=magic,
    unrealized_pnl=unrealized_pnl,
  )


class FakeExchangeGateway(BaseExchangeGateway):
  """In-memory stand-in for :class:`BaseExchangeGateway` (the agnostic seam
  below ``CryptoExecutor``).

  Records placed orders, stop-loss requests, and cancellations, and returns
  configurable :class:`~worker.schemas.trade_result.TradeResult` objects so the
  executor's business logic can be unit-tested without a real exchange connection.

  Usage::

      gw = FakeExchangeGateway()
      ex = CryptoExecutor(gw, config)
      res = ex.open_position(make_signal(...))
      assert gw.orders == [("BTCUSDT", "LONG", 0.02, False)]
  """

  name = "FAKE"

  def __init__(
    self,
    positions=None,
    mark_price: float = 30000.0,
    symbol_filter=None,
    account=None,
    place_results=None,  # pre-queued TradeResults for place_market_order
    sl_results=None,  # pre-queued TradeResults for set_stop_loss
  ):
    self._positions = list(positions or [])
    self._mark = mark_price
    self._filter = symbol_filter if symbol_filter is not None else make_symbol_filter()
    self._account = (
      account if account is not None else {"balance": 1000.0, "equity": 5000.0}
    )
    self._place_results = list(place_results or [])
    self._sl_results = list(sl_results or [])
    self.orders: List[tuple] = []  # (symbol, side, quantity, reduce_only)
    self.stops: List[tuple] = []  # (symbol, position_side, stop_price, quantity)
    self.takes: List[tuple] = []  # (symbol, position_side, tp_price, quantity)
    self.cancelled: List[str] = []  # symbols whose open orders were cancelled

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  def connect(self) -> bool:
    return True

  def close(self) -> None:
    pass

  # ── Market data / rules ───────────────────────────────────────────────── #

  def get_symbol_filter(self, symbol: str) -> SymbolFilter:
    return self._filter

  def get_mark_price(self, symbol: str) -> float:
    return self._mark

  # ── Positions ─────────────────────────────────────────────────────────── #

  def get_positions(self, symbol=None):
    return list(self._positions)

  # ── Orders ────────────────────────────────────────────────────────────── #

  def place_market_order(
    self, symbol, side, quantity, reduce_only=False, client_order_id=None
  ) -> TradeResult:
    self.orders.append((symbol, side, quantity, reduce_only))
    if self._place_results:
      return self._place_results.pop(0)
    return TradeResult.ok(retcode=0, ticket="99", price=self._mark, volume=quantity)

  def set_stop_loss(self, symbol, position_side, stop_price, quantity) -> TradeResult:
    self.stops.append((symbol, position_side, stop_price, quantity))
    if self._sl_results:
      return self._sl_results.pop(0)
    return TradeResult.ok(retcode=0, ticket="100")

  def set_take_profit(self, symbol, position_side, tp_price, quantity) -> TradeResult:
    self.takes.append((symbol, position_side, tp_price, quantity))
    return TradeResult.ok(retcode=0, ticket="101")

  def cancel_all_orders(self, symbol: str) -> None:
    self.cancelled.append(symbol)

  # ── Account ───────────────────────────────────────────────────────────── #

  def get_account(self):
    return self._account
