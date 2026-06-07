"""Tests for the pure Binance user-data-stream event parser."""

from worker.gateways.crypto.binance.user_data_stream import (
  ExchangeCloseReason,
  parse_order_trade_update,
)


def _frame(order: dict) -> dict:
  return {"e": "ORDER_TRADE_UPDATE", "o": order}


def test_ignores_non_order_events():
  assert parse_order_trade_update({"e": "ACCOUNT_UPDATE"}) is None


def test_ignores_unfilled_orders():
  assert parse_order_trade_update(_frame({"X": "NEW", "ot": "STOP_MARKET"})) is None


def test_ignores_worker_market_close():
  # A reduce-only MARKET fill is the worker closing a position itself.
  assert parse_order_trade_update(_frame({"X": "FILLED", "ot": "MARKET", "R": True})) is None


def test_parses_stop_loss():
  event = parse_order_trade_update(
    _frame(
      {
        "s": "BTCUSDT",
        "X": "FILLED",
        "ot": "STOP_MARKET",
        "ap": "29000",
        "z": "0.5",
        "i": 42,
        "c": "x-strat-1",
        "rp": "-10.5",
      }
    )
  )
  assert event is not None
  assert event.reason == ExchangeCloseReason.SL
  assert event.symbol == "BTCUSDT"
  assert event.close_price == 29000.0
  assert event.close_volume == 0.5
  assert event.order_id == 42
  assert event.realized_pnl == -10.5


def test_parses_take_profit():
  event = parse_order_trade_update(
    _frame({"s": "ETHUSDT", "X": "FILLED", "ot": "TAKE_PROFIT_MARKET", "ap": "2100", "z": "1"})
  )
  assert event is not None
  assert event.reason == ExchangeCloseReason.TP


def test_parses_liquidation():
  event = parse_order_trade_update(
    _frame({"s": "BTCUSDT", "X": "FILLED", "ot": "LIQUIDATION", "ap": "1", "z": "1"})
  )
  assert event is not None
  assert event.reason == ExchangeCloseReason.LIQUIDATION
