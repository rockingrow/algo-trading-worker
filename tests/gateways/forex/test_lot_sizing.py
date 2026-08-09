from helpers import make_symbol_spec

from worker.gateways.forex.lot_sizing import LotSizer, _decimals_for_step


def test_decimals_for_step():
  assert _decimals_for_step(1.0) == 0
  assert _decimals_for_step(0.1) == 1
  assert _decimals_for_step(0.01) == 2
  assert _decimals_for_step(0.001) == 3


def test_calculate_lot_size_uses_capital(config):
  sizer = LotSizer(config)
  # capital=1000, risk=2% -> risk_cash=20; SL distance = 10 / 0.01 = 1000 points;
  # point_value = (1/0.01)*0.01 = 1; lot = 20 / (1000*1) = 0.02
  assert (
    sizer.calculate_lot_size(make_symbol_spec(), 2000.0, 1990.0, 2.0, 1000.0) == 0.02
  )


def test_calculate_lot_size_scales_with_capital(config):
  sizer = LotSizer(config)
  # risk_cash = 10000 * 2% = 200; lot = 200 / 1000 = 0.2
  assert (
    sizer.calculate_lot_size(make_symbol_spec(), 2000.0, 1990.0, 2.0, 10000.0) == 0.2
  )


def test_calculate_lot_size_missing_spec_returns_floor(config):
  sizer = LotSizer(config)
  assert sizer.calculate_lot_size(None, 2000.0, 1990.0, 2.0, 1000.0) == 0.01


def test_calculate_lot_size_invalid_sl_distance_returns_volume_min(config):
  sizer = LotSizer(config)
  # entry == sl -> zero distance -> volume_min
  assert (
    sizer.calculate_lot_size(make_symbol_spec(), 2000.0, 2000.0, 2.0, 1000.0) == 0.01
  )


def test_calculate_lot_size_clamps_to_volume_max(config):
  sizer = LotSizer(config)
  lot = sizer.calculate_lot_size(
    make_symbol_spec(volume_max=0.05), 2000.0, 1990.0, 2.0, 1_000_000
  )
  assert lot == 0.05


def test_convert_quantity_to_lots(config):
  sizer = LotSizer(config)
  # 250 / 100 = 2.5
  assert sizer.convert_quantity_to_lots(make_symbol_spec(), 250) == 2.5


def test_convert_quantity_floors_to_step(config):
  sizer = LotSizer(config)
  # 145 / 100 = 1.45 -> floor to step 0.1 -> 1.4
  spec = make_symbol_spec(volume_step=0.1, contract_size=100)
  assert sizer.convert_quantity_to_lots(spec, 145) == 1.4


def test_normalize_volume_floors_and_clamps(config):
  sizer = LotSizer(config)
  assert sizer.normalize_volume(make_symbol_spec(), 0.123456) == 0.12
  # below min clamps up to volume_min
  assert sizer.normalize_volume(make_symbol_spec(), 0.0001) == 0.01


def test_normalize_volume_missing_spec_returns_input(config):
  sizer = LotSizer(config)
  assert sizer.normalize_volume(None, 0.77) == 0.77
