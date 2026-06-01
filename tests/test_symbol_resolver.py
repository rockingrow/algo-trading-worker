from types import SimpleNamespace

from conftest import FakeMt5

from worker.mt5.symbol_resolver import SymbolResolver


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
