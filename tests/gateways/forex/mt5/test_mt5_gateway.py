"""Regression tests for MT5Gateway market-data reads.

``worker.gateways.forex.mt5.bridge`` imports the MetaTrader5 native extension at
module scope and it only ships for Windows, so an empty stand-in is registered
when the real extension is absent — nothing here touches the bridge, which only
reaches for ``mt5.*`` inside its connection methods. The real module is preferred
whenever it imports, so this never shadows it on Windows.
"""

import sys
from types import ModuleType, SimpleNamespace

try:  # pragma: no cover - the native extension exists only on Windows
  import MetaTrader5  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - every non-Windows run
  sys.modules["MetaTrader5"] = ModuleType("MetaTrader5")

from helpers import FakeMt5, make_platform_position, make_tick  # noqa: E402

from worker.gateways.forex.mt5.gateway import MT5Gateway  # noqa: E402


def _gateway(mt5):
  return MT5Gateway(server="TestServer", login=1000, password="pw", mt5_api=mt5)


def test_get_tick_returns_the_live_quote():
  gw = _gateway(FakeMt5(tick=make_tick(ask=2000.0, bid=1999.5)))
  tick = gw.get_tick("XAUUSDc")
  assert (tick.bid, tick.ask) == (1999.5, 2000.0)


def test_get_tick_rejects_a_zeroed_quote():
  """The terminal reports bid=ask=0.0 for a symbol it has no price for (freshly
  added to Market Watch, or outside its session).

  ``symbol_info_tick`` returns a namedtuple, which is truthy even when every
  field is zero, so a plain falsiness check let that tick through: the entry was
  then priced at 0.0 and the stop math pushed SL to -0.01, which the broker
  rejected with retcode 10016 (Invalid stops).
  """
  gw = _gateway(FakeMt5(tick=make_tick(ask=0.0, bid=0.0)))
  assert gw.get_tick("XPTUSDm") is None


def test_get_tick_rejects_a_half_populated_quote():
  gw = _gateway(FakeMt5(tick=make_tick(ask=0.0, bid=1732.55)))
  assert gw.get_tick("XPTUSDm") is None


def test_get_tick_returns_none_when_the_terminal_has_no_tick():
  gw = _gateway(FakeMt5(tick=None))
  # FakeMt5 substitutes a default tick for None, so blank the read directly.
  gw._mt5.symbol_info_tick = lambda name: None
  assert gw.get_tick("XAUUSDc") is None


def test_close_position_refuses_to_price_off_a_zeroed_quote():
  """A close is priced from the same tick — it must not send price=0.0 either."""
  mt5 = FakeMt5(tick=make_tick(ask=0.0, bid=0.0))
  gw = _gateway(mt5)
  result = gw.close_position(make_platform_position(symbol="XPTUSDm"))
  assert result["success"] is False
  assert mt5.sent_requests == []


def test_place_order_sends_the_resolved_stops():
  """Guard-rail check: a healthy quote still produces a normal order request."""
  mt5 = FakeMt5(tick=make_tick(ask=2000.0, bid=1999.5))
  gw = _gateway(mt5)
  gw.place_order(
    symbol="XAUUSDc",
    side="LONG",
    volume=0.01,
    price=2000.0,
    sl=1990.0,
    tp=2050.0,
    magic=42,
    comment="strat-1 04",
  )
  sent = mt5.sent_requests[0]
  assert (sent["price"], sent["sl"], sent["tp"]) == (2000.0, 1990.0, 2050.0)
  assert isinstance(sent["symbol"], str)


def test_symbol_spec_maps_broker_rules():
  gw = _gateway(FakeMt5())
  spec = gw.get_symbol_spec("XAUUSDc")
  assert (spec.point, spec.digits, spec.stops_level) == (0.01, 2, 10)


def test_symbol_spec_is_none_for_an_unknown_symbol():
  mt5 = FakeMt5()
  mt5.symbol_info = lambda name: None
  assert _gateway(mt5).get_symbol_spec("NOPE") is None


def test_positions_are_mapped_to_platform_positions():
  raw = SimpleNamespace(
    ticket=201,
    type=FakeMt5.ORDER_TYPE_BUY,
    volume=0.5,
    price_open=1999.0,
    sl=1990.0,
    tp=2050.0,
    magic=42,
    symbol="XAUUSDc",
  )
  positions = _gateway(FakeMt5(positions=[raw])).get_positions("XAUUSDc")
  assert [(p.ticket, p.side, p.volume) for p in positions] == [(201, "LONG", 0.5)]
