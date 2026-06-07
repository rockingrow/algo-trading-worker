"""Tests for BinanceFuturesGateway request-building over the binance_common transport.

`send_request` is replaced with a fake that records the call and returns canned
bodies, so we assert the gateway sends the right method/path/params and maps
responses into TradeResult / ExchangePosition — without any network.
"""

import worker.gateways.crypto.binance.gateway as gw_mod
from worker.gateways.crypto.base import SIDE_LONG


class FakeResp:
  def __init__(self, data):
    self._data = data

  def data(self):
    return self._data


def _exchange_info():
  return {
    "symbols": [
      {
        "symbol": "BTCUSDT",
        "filters": [
          {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
          {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
        ],
      }
    ]
  }


def make_gateway(monkeypatch):
  calls = []

  def fake_send(session, config, *, method, path, payload, is_signed, response_model):
    calls.append({"method": method, "path": str(path), "payload": payload, "signed": is_signed})
    if str(path) == "/fapi/v1/order":
      return FakeResp({"orderId": 555, "avgPrice": "30000", "executedQty": "0.02", "status": "FILLED"})
    if str(path) == "/fapi/v2/positionRisk":
      return FakeResp([
        {"symbol": "BTCUSDT", "positionAmt": "0.02", "entryPrice": "30000", "unRealizedProfit": "1.5"},
        {"symbol": "ETHUSDT", "positionAmt": "0", "entryPrice": "0"},
      ])
    if str(path) == "/fapi/v2/account":
      return FakeResp({"totalWalletBalance": "1000", "totalMarginBalance": "1010", "availableBalance": "900"})
    if str(path) == "/fapi/v1/exchangeInfo":
      return FakeResp(_exchange_info())
    if str(path) == "/fapi/v1/premiumIndex":
      return FakeResp({"markPrice": "30050"})
    return FakeResp({})

  monkeypatch.setattr(gw_mod, "send_request", fake_send)
  gw = gw_mod.BinanceFuturesGateway("key", "secret", testnet=True)
  return gw, calls


def test_place_market_order_open_long(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  res = gw.place_market_order("BTCUSDT", SIDE_LONG, 0.02, reduce_only=False, client_order_id="x-1")
  assert res["success"] is True
  assert res["ticket"] == 555 and res["price"] == 30000.0 and res["volume"] == 0.02
  c = calls[-1]
  assert c["method"] == "POST" and c["path"] == "/fapi/v1/order" and c["signed"] is True
  assert c["payload"]["side"] == "BUY"
  assert c["payload"]["type"] == "MARKET"
  assert "reduce_only" not in c["payload"]
  assert c["payload"]["new_client_order_id"] == "x-1"
  assert c["payload"]["recv_window"] == 5000  # added for signed requests


def test_place_market_order_reduce_only_long_sells(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  gw.place_market_order("BTCUSDT", SIDE_LONG, 0.02, reduce_only=True)
  c = calls[-1]
  assert c["payload"]["side"] == "SELL"
  assert c["payload"]["reduce_only"] == "true"


def test_set_stop_loss_is_stop_market_close_position(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  res = gw.set_stop_loss("BTCUSDT", SIDE_LONG, stop_price=29000.0, quantity=0.02)
  assert res["success"] is True
  c = calls[-1]
  assert c["path"] == "/fapi/v1/order" and c["signed"] is True
  assert c["payload"]["type"] == "STOP_MARKET"
  assert c["payload"]["side"] == "SELL"        # close a LONG
  assert c["payload"]["stop_price"] == 29000.0
  assert c["payload"]["close_position"] == "true"


def test_get_positions_maps_and_filters_zero(monkeypatch):
  gw, _ = make_gateway(monkeypatch)
  positions = gw.get_positions("BTCUSDT")
  assert len(positions) == 1                    # the zero ETHUSDT row is dropped
  p = positions[0]
  assert p.symbol == "BTCUSDT" and p.side == SIDE_LONG
  assert p.quantity == 0.02 and p.entry_price == 30000.0


def test_get_account_maps_fields(monkeypatch):
  gw, _ = make_gateway(monkeypatch)
  acct = gw.get_account()
  assert acct == {"balance": 1000.0, "equity": 1010.0, "available": 900.0}


def test_get_symbol_filter_indexes_by_symbol(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  f = gw.get_symbol_filter("BTCUSDT")
  assert f.step_size == 0.001 and f.min_qty == 0.001 and f.tick_size == 0.10
  # exchangeInfo is unsigned and fetched once (cached)
  gw.get_symbol_filter("BTCUSDT")
  info_calls = [c for c in calls if c["path"] == "/fapi/v1/exchangeInfo"]
  assert len(info_calls) == 1 and info_calls[0]["signed"] is False


def test_mark_price_unsigned(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  assert gw.get_mark_price("BTCUSDT") == 30050.0
  assert calls[-1]["signed"] is False


def test_cancel_all_orders_delete_signed(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  gw.cancel_all_orders("BTCUSDT")
  c = calls[-1]
  assert c["method"] == "DELETE" and c["path"] == "/fapi/v1/allOpenOrders" and c["signed"] is True


def test_order_failure_returns_trade_result_fail(monkeypatch):
  gw, _ = make_gateway(monkeypatch)

  def boom(*a, **k):
    raise RuntimeError("rejected")

  monkeypatch.setattr(gw_mod, "send_request", boom)
  res = gw.place_market_order("BTCUSDT", SIDE_LONG, 0.02)
  assert res["success"] is False
  assert "rejected" in res["comment"]
