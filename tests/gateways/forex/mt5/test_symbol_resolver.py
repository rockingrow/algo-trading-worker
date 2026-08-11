from types import SimpleNamespace

from helpers import FakeMt5, make_tick

from worker.gateways.forex.mt5.symbol_resolver import SymbolResolver


def _fast_resolver(mt5):
  """Resolver with a short quote wait so tests don't idle for seconds."""
  return SymbolResolver(mt5, tick_wait_timeout=0.05, tick_wait_interval=0.005)


def _zero_tick():
  """What the terminal returns for a symbol it holds no quote for."""
  return make_tick(ask=0.0, bid=0.0)


def test_resolves_to_broker_symbol():
  mt5 = FakeMt5(symbols=[SimpleNamespace(name="XAUUSDc", trade_mode=4, visible=True)])
  resolver = SymbolResolver(mt5)
  assert resolver.get_symbol("XAUUSD") == "XAUUSDc"


def test_caches_resolution(monkeypatch):
  mt5 = FakeMt5(symbols=[SimpleNamespace(name="XAUUSDc", trade_mode=4, visible=True)])
  calls = {"n": 0}
  original = mt5.symbols_get

  def counting(group=None):
    calls["n"] += 1
    return original(group)

  mt5.symbols_get = counting
  resolver = SymbolResolver(mt5)
  resolver.get_symbol("XAUUSD")
  resolver.get_symbol("XAUUSD")
  assert calls["n"] == 1  # second call served from cache


def test_returns_base_when_no_match():
  mt5 = FakeMt5(symbols=[])
  resolver = SymbolResolver(mt5)
  assert resolver.get_symbol("UNKNOWN") == "UNKNOWN"


def test_selects_symbol_when_not_visible():
  mt5 = FakeMt5(symbols=[SimpleNamespace(name="XAUUSDc", trade_mode=4, visible=False)])
  resolver = SymbolResolver(mt5)
  resolver.get_symbol("XAUUSD")
  assert ("XAUUSDc", True) in mt5.selected


def test_skips_disabled_symbol():
  mt5 = FakeMt5(
    symbols=[
      SimpleNamespace(name="XAUUSDc", trade_mode=0, visible=True),  # disabled
    ]
  )
  resolver = SymbolResolver(mt5)
  # only candidate is disabled -> falls through to base symbol
  assert resolver.get_symbol("XAUUSD") == "XAUUSD"


# ── First-quote wait after symbol_select ───────────────────────────────────── #
#
# symbol_select() only asks the terminal to subscribe; the first tick lands a
# moment later. Without waiting, the very first signal for a symbol is priced off
# a bid=ask=0.0 tick, which the stop math turns into a negative SL and the broker
# rejects with retcode 10016.


def test_waits_for_the_first_quote_after_selecting():
  mt5 = FakeMt5(symbols=[SimpleNamespace(name="XPTUSDm", trade_mode=4, visible=False)])
  ticks = [_zero_tick(), _zero_tick(), make_tick(ask=1732.83, bid=1732.55)]
  polled = []

  def scripted(name):
    tick = ticks[min(len(polled), len(ticks) - 1)]
    polled.append(name)
    return tick

  mt5.symbol_info_tick = scripted
  assert _fast_resolver(mt5).get_symbol("XPTUSD") == "XPTUSDm"
  # Kept polling past the zeroed ticks instead of returning on the first one.
  assert len(polled) == 3


def test_gives_up_waiting_when_the_symbol_never_quotes():
  """Market closed: resolution still succeeds — the caller's own tick check is
  what fails the trade, and it must not be blocked forever."""
  mt5 = FakeMt5(
    symbols=[SimpleNamespace(name="XPTUSDm", trade_mode=4, visible=False)],
    tick=_zero_tick(),
  )
  assert _fast_resolver(mt5).get_symbol("XPTUSD") == "XPTUSDm"


def test_visible_symbol_is_not_waited_on():
  """An already-subscribed symbol has its quote — no reason to poll."""
  mt5 = FakeMt5(symbols=[SimpleNamespace(name="XAUUSDc", trade_mode=4, visible=True)])
  polled = []
  mt5.symbol_info_tick = lambda name: polled.append(name) or make_tick()
  _fast_resolver(mt5).get_symbol("XAUUSD")
  assert polled == []
