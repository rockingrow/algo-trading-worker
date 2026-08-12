from types import SimpleNamespace

from helpers import make_signal

from worker.gateways.crypto.message_presenter import CryptoMessagePresenter
from worker.icons import LOSS, PROFIT, SUCCESS
from worker.schemas.signal_schema import SignalActionEnum
from worker.settings import CryptoExchangeEnum


def test_startup_message_shows_hedge_mode():
  cfg = {
    "crypto_exchange": CryptoExchangeEnum.BINANCE,
    "binance_testnet": False,
    "binance_hedge_mode": True,
    "capital": 1000,
  }
  msg = CryptoMessagePresenter.startup(cfg, "FOOTER")
  assert "HEDGE_MODE: <b>Hedge</b>" in msg


def test_startup_message_shows_one_way_mode():
  cfg = {
    "crypto_exchange": CryptoExchangeEnum.BINANCE,
    "binance_hedge_mode": False,
    "capital": 1000,
  }
  msg = CryptoMessagePresenter.startup(cfg, "FOOTER")
  assert "HEDGE_MODE: <b>One-way</b>" in msg


def test_position_imported_manual_message():
  pos = SimpleNamespace(symbol="BTCUSDT", side="LONG", volume=0.02, price_open=64000.0)
  msg = CryptoMessagePresenter.position_imported_manual(
    pos, "MANUAL-BTCUSDT-abcd1234", "FOOTER"
  )
  assert "Manual Position Imported" in msg
  assert "BTCUSDT" in msg and "LONG" in msg
  assert "0.02" in msg and "64000.0" in msg
  assert "MANUAL-BTCUSDT-abcd1234" in msg


def test_order_filled_shows_sl_set():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD", sl=29000.0)
  result = {
    "price": 30000.0,
    "volume": 0.02,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "Order Filled" in msg
  assert "SL:" in msg and "29000.0" in msg and SUCCESS in msg


def test_order_filled_warns_when_sl_not_set():
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD")
  result = {
    "price": 31000.0,
    "volume": 0.006,
    "ticket": 9,
    "sl_update": {"success": False, "comment": "SL rejected"},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "SL NOT SET" in msg and "SL rejected" in msg


def test_order_filled_no_sl_line_for_full_close():
  signal = make_signal(SignalActionEnum.TP2, symbol="BTCUSD")
  result = {"price": 32000.0, "volume": 0.02, "ticket": 11}  # no sl_update
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "SL:" not in msg and "SL NOT SET" not in msg


def test_order_filled_shows_scale_position_block():
  # The broker pre-scales the signal, so tp1/tp2/quantity here are already the
  # final values and are displayed verbatim.
  signal = make_signal(
    SignalActionEnum.LONG,
    symbol="BTCUSD",
    is_scale_position=True,
    tp1=31000.0,
    tp2=32000.0,
  )
  result = {
    "price": 30000.0,
    "volume": 0.04,
    "ticket": 7,
    "sl_update": {"success": True},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 7, "FOOTER")
  assert "Scaled Position" in msg
  assert "TP1:" in msg and "31000.0" in msg
  assert "TP2:" in msg and "32000.0" in msg
  assert "0.04" in msg  # final scaled quantity


def test_order_filled_no_scale_block_for_normal_entry():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD")
  result = {
    "price": 30000.0,
    "volume": 0.02,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "Scaled Position" not in msg
  assert "TP1:" not in msg and "TP2:" not in msg


def test_order_filled_shows_override_section_and_tp1_qty_when_settings_given():
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD")
  result = {
    "price": 30000.0,
    "volume": 0.1353,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  settings = {
    "volume_decision_enabled": True,
    "use_custom_risk_percentage": True,
    "risk_percentage": 3.0,
    "use_account_equity": True,
    "use_custom_position_tp1_percent": True,
    "position_tp1_percent": 50.0,
    "tp1_move_sl_to_breakeven": None,
  }
  msg = CryptoMessagePresenter.order_filled(
    signal, result, 5, "FOOTER", settings_dict=settings
  )
  # Override section sits between the position block and the footer.
  assert "VOLUME_DECISION_ENABLED: <b>ENABLED</b>" in msg
  assert "RISK_PERCENTAGE: <b>ENABLED (3.0%)</b>" in msg
  assert "USE_ACCOUNT_EQUITY: <b>ENABLED</b>" in msg
  assert "POSITION_TP1_PERCENT: <b>ENABLED (50.0%)</b>" in msg
  assert "TP1_MOVE_SL_TO_BREAKEVEN: <b>DISABLED</b>" in msg
  # Quantity line shows "TP1 <pct>%" with a gear only for TP1 action.
  assert "Quantity: <b>0.1353 (TP1 50.0% " in msg


def test_order_filled_tp1_qty_uses_signal_percent_without_gear():
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD", tp1_percent=40.0)
  result = {
    "price": 30000.0,
    "volume": 0.02,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  settings = {"volume_decision_enabled": True, "use_custom_position_tp1_percent": False}
  msg = CryptoMessagePresenter.order_filled(
    signal, result, 5, "FOOTER", settings_dict=settings
  )
  assert "Quantity: <b>0.02 (TP1 40.0%)</b>" in msg


def test_order_filled_tp1_qty_falls_back_to_config_when_signal_percent_absent():
  # use_custom=False but the signal carries no tp1_percent: the suffix must defer
  # to POSITION_TP1_PERCENT, exactly as _resolve_tp1_params sizes the close.
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD")
  result = {
    "price": 30000.0,
    "volume": 0.02,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  settings = {
    "volume_decision_enabled": True,
    "use_custom_position_tp1_percent": False,
    "position_tp1_percent": 30.0,
  }
  msg = CryptoMessagePresenter.order_filled(
    signal, result, 5, "FOOTER", settings_dict=settings
  )
  assert "Quantity: <b>0.02 (TP1 30.0%)</b>" in msg


def test_order_filled_tp1_qty_dropped_when_volume_decision_disabled():
  # VOLUME_DECISION off → the close is sized from signal.quantity, not a percent,
  # so no "(TP1 x%)" suffix should be appended even though a percent is configured.
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD", tp1_percent=40.0)
  result = {
    "price": 30000.0,
    "volume": 0.02,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  settings = {
    "volume_decision_enabled": False,
    "use_custom_position_tp1_percent": True,
    "position_tp1_percent": 50.0,
  }
  msg = CryptoMessagePresenter.order_filled(
    signal, result, 5, "FOOTER", settings_dict=settings
  )
  assert "Quantity: <b>0.02</b>" in msg
  assert "TP1 " not in msg


def test_order_filled_long_short_no_qty_suffix():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD", tp1_percent=50.0)
  result = {
    "price": 30000.0,
    "volume": 0.1353,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  settings = {
    "use_custom_position_tp1_percent": True,
    "position_tp1_percent": 50.0,
  }
  msg = CryptoMessagePresenter.order_filled(
    signal, result, 5, "FOOTER", settings_dict=settings
  )
  assert "Quantity: <b>0.1353</b>" in msg


def test_order_filled_no_override_section_without_settings():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD")
  result = {
    "price": 30000.0,
    "volume": 0.02,
    "ticket": 5,
    "sl_update": {"success": True},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "RISK_PERCENTAGE" not in msg
  assert "Quantity: <b>0.02</b>" in msg


# ── Every close reports its PnL ─────────────────────────────────────────────── #
#
# Same contract as the FOREX presenter: a partial or full close always carries the
# line, whether the close came from a signal, a force close, an ADMIN FLAT, the
# exchange itself, or the reconciler.


def test_order_filled_shows_pnl_for_a_full_close():
  signal = make_signal(SignalActionEnum.TP2, symbol="BTCUSD")
  msg = CryptoMessagePresenter.order_filled(
    signal,
    {"price": 32000.0, "volume": 0.02, "ticket": 11, "profit": 40.0},
    5,
    "FOOTER",
  )
  assert "PnL: <b>+40.00</b>" in msg and PROFIT in msg


def test_order_filled_shows_pnl_for_a_tp1_partial_close():
  signal = make_signal(SignalActionEnum.TP1, symbol="BTCUSD")
  result = {
    "price": 31000.0,
    "volume": 0.006,
    "ticket": 9,
    "profit": -6.0,
    "sl_update": {"success": True},
  }
  msg = CryptoMessagePresenter.order_filled(signal, result, 5, "FOOTER")
  assert "PnL: <b>-6.00</b>" in msg and LOSS in msg


def test_order_filled_reports_an_unreadable_pnl_as_unknown():
  # Some venues answer a filled MARKET order with a 0 price, leaving the amount
  # unknown — which must read as unknown, not as a break-even close.
  signal = make_signal(SignalActionEnum.FLAT, symbol="BTCUSD")
  msg = CryptoMessagePresenter.order_filled(
    signal, {"price": 32000.0, "volume": 0.02, "ticket": 11}, 5, "FOOTER"
  )
  assert "PnL: <b>n/a</b>" in msg


def test_order_filled_has_no_pnl_line_for_an_entry():
  signal = make_signal(SignalActionEnum.LONG, symbol="BTCUSD")
  msg = CryptoMessagePresenter.order_filled(
    signal, {"price": 30000.0, "volume": 0.02, "ticket": 5}, 5, "FOOTER"
  )
  assert "PnL" not in msg


def test_force_closed_shows_pnl():
  msg = CryptoMessagePresenter.force_closed(
    "BTCUSD",
    "strat-1",
    {
      "price": 29500.0,
      "volume": 0.02,
      "ref_id": 7,
      "ref_source_id": 3,
      "profit": -10.0,
    },
    "FOOTER",
  )
  assert "PnL: <b>-10.00</b>" in msg


def test_admin_flat_closed_shows_pnl():
  msg = CryptoMessagePresenter.admin_flat_closed(
    {"symbol": "BTCUSD", "strategy": "strat-A", "ref_source_id": 10},
    {"price": 30500.0, "volume": 0.02, "ticket": 999, "profit": 10.0},
    "FOOTER",
  )
  assert "PnL: <b>+10.00</b>" in msg


def test_exchange_close_renders_pnl_through_the_shared_line():
  # The exchange delivers a realized figure of its own; it must still read like
  # every other close notification rather than as a raw stream value.
  event = SimpleNamespace(
    reason=SimpleNamespace(value="SL"),
    symbol="BTCUSDT",
    close_price=29000.0,
    close_volume=0.02,
    realized_pnl=-20.0,
    order_id="8811",
  )
  msg = CryptoMessagePresenter.exchange_close(event, "FOOTER")
  assert "PnL: <b>-20.00</b>" in msg and LOSS in msg


def test_position_reconciled_closed_labels_its_pnl_an_estimate():
  # The real fill was never delivered, so the figure is measured against the same
  # approximate close price shown above it.
  db_pos = {"symbol": "BTCUSD", "strategy": "strat-A", "ref_source_id": "abc"}
  msg = CryptoMessagePresenter.position_reconciled_closed(
    db_pos, 31000.0, "FOOTER", profit=20.0
  )
  assert "PnL (est.): <b>+20.00</b>" in msg


def test_position_reconciled_closed_without_a_pnl_says_unknown():
  db_pos = {"symbol": "BTCUSD", "strategy": "strat-A", "ref_source_id": "abc"}
  msg = CryptoMessagePresenter.position_reconciled_closed(db_pos, None, "FOOTER")
  assert "PnL (est.): <b>n/a</b>" in msg
