from dataclasses import replace

from helpers import FakeMt5, make_symbol_info

from worker.gateways.mt5.lot_sizing import LotSizer, _decimals_for_step
from worker.gateways.mt5.symbol_resolver import SymbolResolver


def _sizer(mt5, config):
  return LotSizer(mt5, SymbolResolver(mt5), config)


def test_decimals_for_step():
  assert _decimals_for_step(1.0) == 0
  assert _decimals_for_step(0.1) == 1
  assert _decimals_for_step(0.01) == 2
  assert _decimals_for_step(0.001) == 3


def test_calculate_lot_size_uses_configured_capital(config):
  mt5 = FakeMt5()
  sizer = _sizer(mt5, config)
  # capital=1000, risk=2% -> risk_cash=20; SL distance = 10 / 0.01 = 1000 points;
  # point_value = (1/0.01)*0.01 = 1; lot = 20 / (1000*1) = 0.02
  lot = sizer.calculate_lot_size("XAUUSD", 2000.0, 1990.0, 2.0)
  assert lot == 0.02


def test_calculate_lot_size_uses_account_equity_when_enabled(config):
  cfg = replace(config, use_account_equity=True)
  mt5 = FakeMt5()  # default account equity = 10000
  sizer = _sizer(mt5, cfg)
  # risk_cash = 10000 * 2% = 200; lot = 200 / 1000 = 0.2
  lot = sizer.calculate_lot_size("XAUUSD", 2000.0, 1990.0, 2.0)
  assert lot == 0.2


def test_calculate_lot_size_missing_symbol_returns_floor(config):
  mt5 = FakeMt5(symbol_info=None)
  mt5._symbol_info = None
  sizer = _sizer(mt5, config)
  assert sizer.calculate_lot_size("XAUUSD", 2000.0, 1990.0, 2.0) == 0.01


def test_calculate_lot_size_invalid_sl_distance_returns_volume_min(config):
  mt5 = FakeMt5()
  sizer = _sizer(mt5, config)
  # entry == sl -> zero distance -> volume_min
  assert sizer.calculate_lot_size("XAUUSD", 2000.0, 2000.0, 2.0) == 0.01


def test_calculate_lot_size_clamps_to_volume_max(config):
  mt5 = FakeMt5(symbol_info=make_symbol_info(volume_max=0.05))
  sizer = _sizer(mt5, config)
  cfg = replace(config, capital=1_000_000)
  sizer = _sizer(mt5, cfg)
  lot = sizer.calculate_lot_size("XAUUSD", 2000.0, 1990.0, 2.0)
  assert lot == 0.05


def test_convert_quantity_to_lots(config):
  mt5 = FakeMt5()  # contract_size=100, step=0.01
  sizer = _sizer(mt5, config)
  # 250 / 100 = 2.5
  assert sizer.convert_quantity_to_lots("XAUUSD", 250) == 2.5


def test_convert_quantity_floors_to_step(config):
  mt5 = FakeMt5(symbol_info=make_symbol_info(volume_step=0.1, trade_contract_size=100))
  sizer = _sizer(mt5, config)
  # 145 / 100 = 1.45 -> floor to step 0.1 -> 1.4
  assert sizer.convert_quantity_to_lots("XAUUSD", 145) == 1.4


def test_normalize_volume_floors_and_clamps(config):
  mt5 = FakeMt5()
  sizer = _sizer(mt5, config)
  assert sizer.normalize_volume("XAUUSD", 0.123456) == 0.12
  # below min clamps up to volume_min
  assert sizer.normalize_volume("XAUUSD", 0.0001) == 0.01


def test_normalize_volume_missing_symbol_returns_input(config):
  mt5 = FakeMt5()
  mt5._symbol_info = None
  sizer = _sizer(mt5, config)
  assert sizer.normalize_volume("XAUUSD", 0.77) == 0.77
