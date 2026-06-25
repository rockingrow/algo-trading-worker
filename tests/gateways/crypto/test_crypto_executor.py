"""Tests for CryptoExecutor against a fake exchange gateway."""

from dataclasses import replace

from helpers import make_signal

from worker.gateways.crypto.base import SIDE_LONG, ExchangePosition, SymbolFilter
from worker.gateways.crypto.executor import CryptoExecutor
from worker.schemas.signal_schema import SignalActionEnum


class FakeGateway:
  def __init__(self, positions=None, mark_price=30000.0, step=0.001, sl_ok=True):
    self._positions = positions if positions is not None else []
    self._mark = mark_price
    self._filter = SymbolFilter(step_size=step, min_qty=step, tick_size=0.1)
    self._sl_ok = sl_ok
    self.orders = []
    self.client_order_ids = []
    self.stops = []
    self.takes = []
    self.cancelled = []

  def get_symbol_filter(self, symbol):
    return self._filter

  def get_mark_price(self, symbol):
    return self._mark

  def get_positions(self, symbol=None):
    return list(self._positions)

  def place_market_order(self, symbol, side, quantity, reduce_only=False, client_order_id=None):
    self.orders.append((symbol, side, quantity, reduce_only))
    self.client_order_ids.append(client_order_id)
    return {"success": True, "retcode": 0, "ticket": 99, "price": self._mark, "volume": quantity}

  def set_stop_loss(self, symbol, position_side, stop_price, quantity):
    self.stops.append((symbol, position_side, stop_price, quantity))
    if not self._sl_ok:
      return {"success": False, "retcode": -2021, "comment": "SL rejected"}
    return {"success": True, "retcode": 0, "ticket": 100}

  def set_take_profit(self, symbol, position_side, tp_price, quantity):
    self.takes.append((symbol, position_side, tp_price, quantity))
    return {"success": True, "retcode": 0, "ticket": 101}

  def cancel_all_orders(self, symbol):
    self.cancelled.append(symbol)

  def get_account(self):
    return {"balance": 1000.0, "equity": 5000.0}


def _long_pos(qty=0.02, entry=30000.0):
  return ExchangePosition(
    symbol="BTCUSDT", side=SIDE_LONG, quantity=qty, entry_price=entry, ticket=7
  )


def test_get_symbol_appends_quote():
  ex = CryptoExecutor(FakeGateway(), None, quote_asset="USDT")
  assert ex.get_symbol("BTCUSD") == "BTCUSDT"
  assert ex.get_symbol("BTCUSDT") == "BTCUSDT"
  assert ex.get_symbol("eth") == "ETHUSDT"


def test_get_symbol_strips_perpetual_suffix():
  ex = CryptoExecutor(FakeGateway(), None, quote_asset="USDT")
  assert ex.get_symbol("BTCUSDT.P") == "BTCUSDT"
  assert ex.get_symbol("BTCUSD.P") == "BTCUSDT"


def test_get_symbol_strips_exchange_prefix():
  ex = CryptoExecutor(FakeGateway(), None, quote_asset="USDT")
  assert ex.get_symbol("BINANCE:BTCUSDT.P") == "BTCUSDT"
  assert ex.get_symbol("BINANCE:BTCUSDT") == "BTCUSDT"
  assert ex.get_symbol("OANDA:XAUUSD") == "XAUUSDT"


def test_get_symbol_respects_quote_asset():
  ex = CryptoExecutor(FakeGateway(), None, quote_asset="USDC")
  assert ex.get_symbol("BTCUSD") == "BTCUSDC"
  assert ex.get_symbol("BTCUSDC") == "BTCUSDC"
  assert ex.get_symbol("BINANCE:ETHUSDC.P") == "ETHUSDC"


def test_normalize_volume_floors_to_step():
  ex = CryptoExecutor(FakeGateway(step=0.001), None)
  assert ex.normalize_volume("BTCUSD", 0.0239) == 0.023


def test_normalize_volume_no_float_underfloor():
  # 0.3 / 0.1 == 2.9999… in IEEE-754; must floor to 3, not 2.
  ex = CryptoExecutor(FakeGateway(step=0.1), None)
  assert ex.normalize_volume("BTCUSD", 0.3) == 0.3
  # 0.24 / 0.04 is another classic case.
  ex4 = CryptoExecutor(FakeGateway(step=0.04), None)
  assert ex4.normalize_volume("BTCUSD", 0.24) == 0.24


def test_calculate_quantity_risk_based(config):
  ex = CryptoExecutor(FakeGateway(step=0.001), config)
  # capital=1000, risk=2% → $20 risk; |30000-29000|=1000 → 0.02
  assert ex.calculate_quantity("BTCUSD", 30000, 29000, 2.0, 1000) == 0.02


def test_open_position_risk_mode_places_order_and_stop(config):
  gw = FakeGateway()
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0))
  assert res["success"] is True
  assert gw.orders == [("BTCUSDT", "LONG", 0.02, False)]
  assert gw.stops and gw.stops[0][2] == 29000.0


def test_open_position_sizes_against_account_equity_when_enabled(config):
  """USE_ACCOUNT_EQUITY=true sizes off live equity, not the fixed CAPITAL."""
  cfg = replace(config, use_account_equity=True)
  gw = FakeGateway()  # equity = 5000
  ex = CryptoExecutor(gw, cfg)
  # equity=5000, risk=2% → $100 risk; |30000-29000|=1000 → 0.1 (vs 0.02 on CAPITAL=1000)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0))
  assert res["success"] is True
  assert gw.orders == [("BTCUSDT", "LONG", 0.1, False)]


def test_open_position_account_equity_unavailable_falls_back_to_min_qty(config):
  """When equity can't be read, fall back to min qty rather than the wrong base."""
  cfg = replace(config, use_account_equity=True)
  gw = FakeGateway(step=0.001)
  gw.get_account = lambda: None
  ex = CryptoExecutor(gw, cfg)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0))
  assert res["success"] is True
  assert gw.orders[0][2] == 0.001  # min_qty


def test_open_position_rolls_back_when_sl_fails(config):
  """A filled entry whose protective stop fails must be closed, not persisted."""
  gw = FakeGateway(sl_ok=False)
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0))

  assert res["success"] is False
  assert "stop-loss failed" in res["comment"]
  # Entry opened (reduce_only=False) then rolled back (reduce_only=True).
  assert gw.orders == [
    ("BTCUSDT", "LONG", 0.02, False),
    ("BTCUSDT", "LONG", 0.02, True),
  ]
  assert res["sl_update"]["success"] is False


def test_open_position_payload_quantity_mode(config):
  cfg = replace(config, volume_decision_enabled=False)
  gw = FakeGateway()
  ex = CryptoExecutor(gw, cfg)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, symbol="BTCUSD", quantity=0.05, sl=None)
  )
  assert res["success"] is True
  assert gw.orders[0][2] == 0.05  # quantity used directly


def test_open_position_risk_mode_applies_scale_in_factor(config):
  """VOLUME_DECISION sizes from risk, so the scale-in quantity factor must be
  re-applied to the computed qty (the broker's pre-scaled qty is ignored here)."""
  gw = FakeGateway()
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(
    make_signal(
      SignalActionEnum.LONG,
      symbol="BTCUSD",
      sl=29000.0,
      is_scale_position=True,
      scaling={"quantity": 0.5},
    )
  )
  assert res["success"] is True
  # Base risk qty is 0.02 (risk=2%); scale factor 0.5 halves it to 0.01.
  assert gw.orders == [("BTCUSDT", "LONG", 0.01, False)]


def test_open_position_payload_mode_ignores_scale_factor(config):
  """In payload-quantity mode the broker's scaled quantity is used verbatim — the
  scale factor must NOT be re-applied (that would double-scale)."""
  cfg = replace(config, volume_decision_enabled=False)
  gw = FakeGateway()
  ex = CryptoExecutor(gw, cfg)
  res = ex.open_position(
    make_signal(
      SignalActionEnum.LONG,
      symbol="BTCUSD",
      quantity=0.05,
      sl=None,
      is_scale_position=True,
      scaling={"quantity": 0.5},
    )
  )
  assert res["success"] is True
  assert gw.orders[0][2] == 0.05  # broker-scaled quantity used as-is, not 0.025


def test_open_position_places_tp_from_signal_tp2(config):
  """When the entry signal carries TP2, a resting take-profit is placed alongside SL."""
  gw = FakeGateway()
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0, tp2=31000.0)
  )
  assert res["success"] is True
  assert gw.stops and gw.stops[0][2] == 29000.0
  assert gw.takes == [("BTCUSDT", "LONG", 31000.0, 0.02)]
  assert res["tp_update"]["success"] is True


def test_open_position_no_tp_when_signal_has_no_tp2(config):
  """No TP2 in the signal → no take-profit order placed (SL still set)."""
  gw = FakeGateway()
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0, tp2=None)
  )
  assert res["success"] is True
  assert gw.takes == []


def test_open_position_tolerates_tp_failure(config):
  """A failed TP placement is logged but does not roll back the SL-protected entry."""
  gw = FakeGateway()
  gw.set_take_profit = lambda *a, **k: {"success": False, "retcode": -1, "comment": "TP rejected"}
  ex = CryptoExecutor(gw, config)
  res = ex.open_position(
    make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0, tp2=31000.0)
  )
  assert res["success"] is True  # entry stands; SL is set, TP best-effort
  assert res["tp_update"]["success"] is False
  # The entry was NOT rolled back (no reduce-only counter-order).
  assert gw.orders == [("BTCUSDT", "LONG", 0.02, False)]


class _FakeDB:
  """Minimal PositionStore stub exposing only the lookup the executor needs."""

  def __init__(self, rows):
    self._rows = rows

  def get_open_positions_by_strategy(self, strategy, symbol):
    return list(self._rows)


def test_update_sl_replaces_tp2_from_persisted_entry_signal(config):
  """Breakeven SL cancels resting orders, then re-places TP2 from the entry signal."""
  import json

  gw = FakeGateway(positions=[_long_pos()])
  db = _FakeDB([{"gateway_message": json.dumps({"tp2": 31000.0})}])
  ex = CryptoExecutor(gw, config, db=db)
  res = ex.update_position_sl("BTCUSD", new_sl=30000.0, position_ticket=7, strategy="strat-1")
  assert res["success"] is True
  assert gw.cancelled == ["BTCUSDT"]            # old SL + old TP cleared
  assert gw.stops[0][2] == 30000.0              # breakeven stop re-placed
  assert gw.takes == [("BTCUSDT", "LONG", 31000.0, 0.02)]  # TP2 re-established


def test_update_sl_no_tp_replace_when_unavailable(config):
  """No DB / no recoverable TP2 → breakeven SL still set, no TP re-placed."""
  gw = FakeGateway(positions=[_long_pos()])
  ex = CryptoExecutor(gw, config)  # db=None
  res = ex.update_position_sl("BTCUSD", new_sl=30000.0, position_ticket=7, strategy="strat-1")
  assert res["success"] is True
  assert gw.stops[0][2] == 30000.0
  assert gw.takes == []


def test_partial_close_uses_reduce_only(config):
  gw = FakeGateway(positions=[_long_pos(qty=0.02)])
  ex = CryptoExecutor(gw, config)
  res = ex.partial_close_position("BTCUSD", close_volume=0.006, position_ticket=7)
  assert res["success"] is True
  assert gw.orders == [("BTCUSDT", "LONG", 0.006, True)]
  assert res["source_ticket"] == "7"


def test_update_sl_cancels_then_sets(config):
  gw = FakeGateway(positions=[_long_pos()])
  ex = CryptoExecutor(gw, config)
  res = ex.update_position_sl("BTCUSD", new_sl=30000.0, position_ticket=7)
  assert res["success"] is True
  assert gw.cancelled == ["BTCUSDT"]
  assert gw.stops[0][2] == 30000.0


def test_close_all_positions_reduce_only(config):
  gw = FakeGateway(positions=[_long_pos(qty=0.02)])
  ex = CryptoExecutor(gw, config)
  res = ex.close_all_positions("BTCUSD", reason="SL")
  assert res["success"] is True
  assert gw.orders == [("BTCUSDT", "LONG", 0.02, True)]
  assert "SL" in res["comment"]


def test_close_orders_carry_worker_marker(config):
  # Reduce-only closes must tag their clientOrderId with WORKER_ORDER_PREFIX so
  # the exchange event stream can tell them apart from an external manual close.
  from worker.gateways.crypto.base import WORKER_ORDER_PREFIX

  gw = FakeGateway(positions=[_long_pos(qty=0.02)])
  ex = CryptoExecutor(gw, config)
  ex.partial_close_position("BTCUSD", close_volume=0.006, position_ticket=7)
  ex.close_all_positions("BTCUSD", reason="SL")

  assert gw.client_order_ids  # both closes recorded an id
  assert all(cid and cid.startswith(WORKER_ORDER_PREFIX) for cid in gw.client_order_ids)


def test_close_all_no_positions(config):
  ex = CryptoExecutor(FakeGateway(positions=[]), config)
  res = ex.close_all_positions("BTCUSD")
  assert res["success"] is False


def test_owned_magics_empty(config):
  assert CryptoExecutor(FakeGateway(), config).owned_magics() == set()
