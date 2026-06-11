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

from worker.schemas.signal_schema import SignalActionEnum, SignalSchema  # noqa: E402


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


def make_position(ticket=111, type_=0, volume=1.0, price_open=2000.0, tp=2050.0, magic=42, symbol="XAUUSDc"):
  return SimpleNamespace(
    ticket=ticket, type=type_, volume=volume, price_open=price_open, tp=tp, magic=magic, symbol=symbol
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
