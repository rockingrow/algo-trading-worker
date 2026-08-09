"""Tests for BinanceFuturesGateway request-building over the binance_common transport.

`send_request` is replaced with a fake that records the call and returns canned
bodies, so we assert the gateway sends the right method/path/params and maps
responses into TradeResult / ExchangePosition — without any network.
"""

import worker.gateways.crypto.binance.gateway as gw_mod
from worker.gateways.crypto.base import SIDE_LONG, SIDE_SHORT


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


def make_gateway(monkeypatch, hedge_mode=False):
  calls = []

  def fake_send(session, config, *, method, path, payload, is_signed, response_model):
    calls.append(
      {"method": method, "path": str(path), "payload": payload, "signed": is_signed}
    )
    if str(path) == "/fapi/v1/order":
      return FakeResp(
        {"orderId": 555, "avgPrice": "30000", "executedQty": "0.02", "status": "FILLED"}
      )
    if str(path) == "/fapi/v1/algoOrder":
      return FakeResp({"algoId": 777, "status": "NEW"})
    if str(path) == "/fapi/v1/algoOpenOrders":
      return FakeResp(
        {
          "code": 200,
          "msg": "The algo open order cancellation request is successfully sent.",
        }
      )
    if str(path) == "/fapi/v2/positionRisk":
      return FakeResp(
        [
          {
            "symbol": "BTCUSDT",
            "positionAmt": "0.02",
            "entryPrice": "30000",
            "unRealizedProfit": "1.5",
          },
          {"symbol": "ETHUSDT", "positionAmt": "0", "entryPrice": "0"},
        ]
      )
    if str(path) == "/fapi/v2/account":
      return FakeResp(
        {
          "totalWalletBalance": "1000",
          "totalMarginBalance": "1010",
          "availableBalance": "900",
        }
      )
    if str(path) == "/fapi/v1/exchangeInfo":
      return FakeResp(_exchange_info())
    if str(path) == "/fapi/v1/premiumIndex":
      return FakeResp({"markPrice": "30050"})
    if str(path) == "/fapi/v1/time":
      return FakeResp({"serverTime": 1_700_000_000_000})
    return FakeResp({})

  monkeypatch.setattr(gw_mod, "send_request", fake_send)
  gw = gw_mod.BinanceFuturesGateway(
    "key", "secret", testnet=True, hedge_mode=hedge_mode
  )
  return gw, calls


def test_place_market_order_open_long(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  res = gw.place_market_order(
    "BTCUSDT", SIDE_LONG, 0.02, reduce_only=False, client_order_id="x-1"
  )
  assert res["success"] is True
  assert res["ticket"] == "555" and res["price"] == 30000.0 and res["volume"] == 0.02
  c = calls[-1]
  assert c["method"] == "POST" and c["path"] == "/fapi/v1/order" and c["signed"] is True
  assert c["payload"]["side"] == "BUY"
  assert c["payload"]["type"] == "MARKET"
  assert "reduce_only" not in c["payload"]
  assert c["payload"]["new_client_order_id"] == "x-1"
  assert c["payload"]["recv_window"] == 5000  # added for signed requests
  # RESULT response type so the fill (avgPrice/executedQty) comes back, not ACK.
  assert c["payload"]["new_order_resp_type"] == "RESULT"


def test_place_market_order_reduce_only_long_sells(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  gw.place_market_order("BTCUSDT", SIDE_LONG, 0.02, reduce_only=True)
  c = calls[-1]
  assert c["payload"]["side"] == "SELL"
  assert c["payload"]["reduce_only"] == "true"


def test_set_stop_loss_is_conditional_algo_order_close_position(monkeypatch):
  # Conditional stops go on the Algo Order endpoint (Binance rejects STOP_MARKET
  # on /fapi/v1/order with -4120), with `trigger_price` rather than `stop_price`.
  gw, calls = make_gateway(monkeypatch)
  res = gw.set_stop_loss("BTCUSDT", SIDE_LONG, stop_price=29000.0, quantity=0.02)
  assert res["success"] is True
  assert res["ticket"] == "777"  # algoId mapped to ticket
  c = calls[-1]
  assert c["path"] == "/fapi/v1/algoOrder" and c["signed"] is True
  assert c["payload"]["algo_type"] == "CONDITIONAL"
  assert c["payload"]["type"] == "STOP_MARKET"
  assert c["payload"]["side"] == "SELL"  # close a LONG
  assert c["payload"]["trigger_price"] == 29000.0
  assert c["payload"]["close_position"] == "true"
  assert "stop_price" not in c["payload"]  # legacy field must not leak through
  assert "quantity" not in c["payload"]  # closePosition forbids quantity


def test_set_stop_loss_snaps_trigger_price_to_tick(monkeypatch):
  # A breakeven stop at a live entry price (63764.73) is off the 0.10 tick grid;
  # Binance rejects off-grid prices with -1111, so the gateway must snap it first.
  gw, calls = make_gateway(monkeypatch)
  gw.set_stop_loss("BTCUSDT", SIDE_LONG, stop_price=63764.73, quantity=0.02)
  assert calls[-1]["payload"]["trigger_price"] == 63764.7


def test_set_take_profit_is_conditional_algo_order_close_position(monkeypatch):
  # Take-profit targets are conditional orders on the Algo Order endpoint
  # (TAKE_PROFIT_MARKET, algoType=CONDITIONAL), mirroring the stop but on the
  # opposite trigger direction. closePosition=true flattens whatever remains.
  gw, calls = make_gateway(monkeypatch)
  res = gw.set_take_profit("BTCUSDT", SIDE_LONG, tp_price=31000.0, quantity=0.02)
  assert res["success"] is True
  assert res["ticket"] == "777"  # algoId mapped to ticket
  c = calls[-1]
  assert c["path"] == "/fapi/v1/algoOrder" and c["signed"] is True
  assert c["payload"]["algo_type"] == "CONDITIONAL"
  assert c["payload"]["type"] == "TAKE_PROFIT_MARKET"
  assert c["payload"]["side"] == "SELL"  # close a LONG
  assert c["payload"]["trigger_price"] == 31000.0


def test_place_market_order_hedge_mode_sends_position_side_not_reduce_only(monkeypatch):
  # Hedge Mode accounts require every order to carry positionSide and reject
  # reduceOnly outright (-1106) — this is the -4061 fix for Hedge accounts.
  gw, calls = make_gateway(monkeypatch, hedge_mode=True)
  gw.place_market_order("BTCUSDT", SIDE_LONG, 0.02, reduce_only=False)
  c = calls[-1]
  assert c["payload"]["side"] == "BUY"
  assert c["payload"]["position_side"] == "LONG"
  assert "reduce_only" not in c["payload"]


def test_place_market_order_hedge_mode_close_keeps_position_side_of_the_position(
  monkeypatch,
):
  # Closing a LONG in Hedge Mode still sells, but positionSide stays LONG (the
  # position being reduced) — it must not flip to SHORT.
  gw, calls = make_gateway(monkeypatch, hedge_mode=True)
  gw.place_market_order("BTCUSDT", SIDE_LONG, 0.02, reduce_only=True)
  c = calls[-1]
  assert c["payload"]["side"] == "SELL"
  assert c["payload"]["position_side"] == "LONG"
  assert "reduce_only" not in c["payload"]


def test_place_market_order_hedge_mode_short(monkeypatch):
  gw, calls = make_gateway(monkeypatch, hedge_mode=True)
  gw.place_market_order("BTCUSDT", SIDE_SHORT, 0.02, reduce_only=False)
  c = calls[-1]
  assert c["payload"]["side"] == "SELL"
  assert c["payload"]["position_side"] == "SHORT"


def test_set_stop_loss_hedge_mode_sends_position_side(monkeypatch):
  gw, calls = make_gateway(monkeypatch, hedge_mode=True)
  gw.set_stop_loss("BTCUSDT", SIDE_LONG, stop_price=29000.0, quantity=0.02)
  c = calls[-1]
  assert c["payload"]["position_side"] == "LONG"
  assert c["payload"]["side"] == "SELL"


def test_set_take_profit_hedge_mode_sends_position_side(monkeypatch):
  gw, calls = make_gateway(monkeypatch, hedge_mode=True)
  gw.set_take_profit("BTCUSDT", SIDE_SHORT, tp_price=28000.0, quantity=0.02)
  c = calls[-1]
  assert c["payload"]["position_side"] == "SHORT"
  assert c["payload"]["side"] == "BUY"  # close a SHORT
  assert c["payload"]["close_position"] == "true"
  assert "quantity" not in c["payload"]  # closePosition forbids quantity


def test_set_take_profit_snaps_trigger_price_to_tick(monkeypatch):
  # Off-grid TP trigger (0.10 tick) would be rejected with -1111 — snap it first.
  gw, calls = make_gateway(monkeypatch)
  gw.set_take_profit("BTCUSDT", SIDE_LONG, tp_price=63764.73, quantity=0.02)
  assert calls[-1]["payload"]["trigger_price"] == 63764.7


# ── Leverage / account-cap (-4421) handling ─────────────────────────────── #


class FakeCapError(Exception):
  """Mimics the binance_common exception: carries status_code / error_message."""

  def __init__(self, status_code, error_message):
    super().__init__(error_message)
    self.status_code = status_code
    self.error_message = error_message


def _cap_err(message, code=gw_mod._LEVERAGE_CAP_CODE):
  return FakeCapError(code, message)


def test_leverage_cap_parsed_from_4421_message():
  cap = gw_mod.BinanceFuturesGateway._leverage_cap_from_error(
    _cap_err("Subaccounts are restricted from using leverage greater than 5x."),
    upper_bound=10,
  )
  assert cap == 5


def test_leverage_cap_ignored_for_non_4421_error():
  assert (
    gw_mod.BinanceFuturesGateway._leverage_cap_from_error(
      _cap_err("greater than 5x", code=-1102), upper_bound=10
    )
    is None
  )


def test_leverage_cap_at_or_above_request_is_treated_as_anomaly():
  # A -4421 is by definition a restriction below what we asked; a parsed value
  # >= request means we grabbed the wrong number — discard it.
  assert (
    gw_mod.BinanceFuturesGateway._leverage_cap_from_error(
      _cap_err("greater than 10x"), upper_bound=10
    )
    is None
  )


def test_leverage_cap_unparseable_returns_none():
  # Reworded message the regex no longer matches.
  assert (
    gw_mod.BinanceFuturesGateway._leverage_cap_from_error(
      _cap_err("Leverage is limited to 5 for this account."), upper_bound=10
    )
    is None
  )


def _leverage_gateway(monkeypatch, error_on_first):
  """Gateway whose LEVERAGE POST raises *error_on_first* the first call, then
  echoes back whatever leverage it is sent. Records each requested leverage."""
  requested = []

  def fake_send(session, config, *, method, path, payload, is_signed, response_model):
    if str(path) == "/fapi/v1/leverage":
      lev = payload["leverage"]
      requested.append(lev)
      if len(requested) == 1 and error_on_first is not None:
        raise error_on_first
      return FakeResp({"symbol": payload["symbol"], "leverage": lev})
    return FakeResp({})

  monkeypatch.setattr(gw_mod, "send_request", fake_send)
  gw = gw_mod.BinanceFuturesGateway("key", "secret", testnet=True)
  return gw, requested


def test_set_leverage_retries_at_parsed_cap(monkeypatch):
  gw, requested = _leverage_gateway(monkeypatch, _cap_err("greater than 5x"))
  applied = gw.set_leverage("BTCUSDT", 10, min_leverage_cap=5)
  assert applied == 5
  assert requested == [10, 5]  # tried 10, retried at parsed 5


def test_set_leverage_falls_back_to_floor_when_unparseable(monkeypatch):
  # -4421 but the message can't be parsed → retry at min(min_leverage_cap, target).
  gw, requested = _leverage_gateway(monkeypatch, _cap_err("Leverage limited to 5."))
  applied = gw.set_leverage("BTCUSDT", 10, min_leverage_cap=5)
  assert applied == 5
  assert requested == [10, 5]


def test_set_leverage_floor_never_raised_above_target(monkeypatch):
  # Floor (5) is above target (3): clamping yields 3, which is the value that
  # just failed — so no pointless retry fires and the symbol is left untouched
  # (account is restricted below target → manual fix), never bumped up to 5.
  gw, requested = _leverage_gateway(monkeypatch, _cap_err("reworded message"))
  applied = gw.set_leverage("BTCUSDT", 3, min_leverage_cap=5)
  assert applied is None
  assert requested == [3]  # no retry above target


def test_set_leverage_no_fallback_without_floor(monkeypatch):
  # Unparseable -4421 and no floor configured → give up, leave symbol untouched.
  gw, requested = _leverage_gateway(monkeypatch, _cap_err("reworded message"))
  applied = gw.set_leverage("BTCUSDT", 10, min_leverage_cap=None)
  assert applied is None
  assert requested == [10]  # no retry


def test_set_leverage_success_first_try(monkeypatch):
  gw, requested = _leverage_gateway(monkeypatch, None)
  applied = gw.set_leverage("BTCUSDT", 10, min_leverage_cap=5)
  assert applied == 10
  assert requested == [10]


def test_get_positions_maps_and_filters_zero(monkeypatch):
  gw, _ = make_gateway(monkeypatch)
  positions = gw.get_positions("BTCUSDT")
  assert len(positions) == 1  # the zero ETHUSDT row is dropped
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
  # cancel_all_orders must cancel BOTH regular orders (allOpenOrders) AND algo
  # conditional orders (algoOpenOrders) — they are separate Binance systems.
  # Failing to cancel the algo side leaves an old SL running alongside the new
  # one after update_position_sl (double-stop scenario).
  gw, calls = make_gateway(monkeypatch)
  gw.cancel_all_orders("BTCUSDT")
  deleted = [c for c in calls if c["method"] == "DELETE"]
  paths = {c["path"] for c in deleted}
  assert "/fapi/v1/allOpenOrders" in paths
  assert "/fapi/v1/algoOpenOrders" in paths
  assert all(c["signed"] is True for c in deleted)


def test_get_timestamp_is_wrapped_with_offset():
  # The SDK transport stamps signed requests via binance_common.utils.get_timestamp;
  # importing the gateway must redirect that to the offset-aware version so the
  # configured clock skew is applied to every signed request.
  import binance_common.utils as bc_utils

  assert bc_utils.get_timestamp is gw_mod._offset_timestamp


def test_sync_time_sets_offset_to_track_server(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  # Local clock is far ahead of the (fixed) fake server time, so the measured
  # offset must be strongly negative, pulling signed timestamps back to ~server.
  monkeypatch.setattr(gw_mod, "_TIME_OFFSET_MS", 0, raising=False)
  gw._sync_time()
  assert gw_mod._TIME_OFFSET_MS < 0
  assert abs(gw_mod._offset_timestamp() - 1_700_000_000_000) < 1000
  assert calls[-1]["path"] == "/fapi/v1/time" and calls[-1]["signed"] is False


def test_sync_time_failure_falls_back_to_local_clock(monkeypatch):
  gw, _ = make_gateway(monkeypatch)
  monkeypatch.setattr(gw_mod, "_TIME_OFFSET_MS", 0, raising=False)

  def boom(*a, **k):
    raise RuntimeError("network down")

  monkeypatch.setattr(gw_mod, "send_request", boom)
  gw._sync_time()  # must not raise
  assert gw_mod._TIME_OFFSET_MS == 0  # untouched → raw local clock


def test_connect_syncs_time_before_balance(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  assert gw.connect() is True
  signed_paths = [c["path"] for c in calls]
  # Time sync happens first, then the signed balance probe.
  assert signed_paths.index("/fapi/v1/time") < signed_paths.index("/fapi/v2/balance")


def test_signed_request_resyncs_and_retries_on_minus_1021(monkeypatch):
  # First signed attempt is rejected with -1021 (clock drifted); the gateway must
  # re-sync time (a /fapi/v1/time call) and retry the same request once, succeeding.
  from binance_common.errors import BadRequestError

  gw, calls = make_gateway(monkeypatch)
  state = {"first": True}

  def flaky_send(session, config, *, method, path, payload, is_signed, response_model):
    calls.append(
      {"method": method, "path": str(path), "payload": payload, "signed": is_signed}
    )
    if str(path) == "/fapi/v1/time":
      return FakeResp({"serverTime": 1_700_000_000_000})
    if str(path) == "/fapi/v1/order" and state["first"]:
      state["first"] = False
      raise BadRequestError(error_message="Timestamp ... ahead", status_code=-1021)
    return FakeResp(
      {"orderId": 7, "avgPrice": "10", "executedQty": "1", "status": "FILLED"}
    )

  monkeypatch.setattr(gw_mod, "send_request", flaky_send)
  res = gw.place_market_order("BTCUSDT", SIDE_LONG, 1.0)
  assert res["success"] is True and res["ticket"] == "7"
  paths = [c["path"] for c in calls]
  assert paths.count("/fapi/v1/order") == 2  # original + retry
  assert "/fapi/v1/time" in paths  # re-synced between attempts


def test_signed_request_does_not_retry_on_other_errors(monkeypatch):
  from binance_common.errors import BadRequestError

  gw, calls = make_gateway(monkeypatch)

  def boom(session, config, *, method, path, payload, is_signed, response_model):
    calls.append({"path": str(path)})
    raise BadRequestError(error_message="bad symbol", status_code=-1121)

  monkeypatch.setattr(gw_mod, "send_request", boom)
  res = gw.place_market_order("BTCUSDT", SIDE_LONG, 1.0)
  assert res["success"] is False  # surfaced, not retried
  assert calls.count({"path": "/fapi/v1/order"}) == 1
  assert "/fapi/v1/time" not in [c["path"] for c in calls]


def test_order_failure_returns_trade_result_fail(monkeypatch):
  gw, _ = make_gateway(monkeypatch)

  def boom(*a, **k):
    raise RuntimeError("rejected")

  monkeypatch.setattr(gw_mod, "send_request", boom)
  res = gw.place_market_order("BTCUSDT", SIDE_LONG, 0.02)
  assert res["success"] is False
  assert "rejected" in res["comment"]


def test_order_result_falls_back_to_cumquote_when_avgprice_zero(monkeypatch):
  # Binance Futures testnet (and occasionally live) returns avgPrice="0" for
  # MARKET orders even when the fill is complete. _order_result must fall back to
  # cumQuote / executedQty to recover the real average price.
  gw, _ = make_gateway(monkeypatch)

  def fake_send(session, config, *, method, path, payload, is_signed, response_model):
    return FakeResp(
      {
        "orderId": 999,
        "avgPrice": "0",
        "executedQty": "0.0333",
        "cumQuote": "2179.15",
        "status": "FILLED",
      }
    )

  monkeypatch.setattr(gw_mod, "send_request", fake_send)
  res = gw.place_market_order("BTCUSDT", SIDE_LONG, 0.0333)
  assert res["success"] is True
  assert res["volume"] == 0.0333
  assert abs(res["price"] - 2179.15 / 0.0333) < 0.01


# ── Position mode ───────────────────────────────────────────────────────────── #


def test_set_position_mode_hedge_posts_dual_true(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  assert gw.set_position_mode(True) is True
  c = calls[-1]
  assert c["method"] == "POST" and c["path"] == "/fapi/v1/positionSide/dual"
  assert c["signed"] is True
  assert c["payload"]["dual_side_position"] == "true"


def test_set_position_mode_one_way_posts_dual_false(monkeypatch):
  gw, calls = make_gateway(monkeypatch)
  assert gw.set_position_mode(False) is True
  assert calls[-1]["payload"]["dual_side_position"] == "false"


def test_set_position_mode_treats_no_change_as_success(monkeypatch):
  # Binance returns -4059 ("No need to change position side.") when the account is
  # already in the requested mode — the desired end state, so it counts as success.
  from binance_common.errors import BadRequestError

  gw, _ = make_gateway(monkeypatch)

  def already(session, config, *, method, path, payload, is_signed, response_model):
    raise BadRequestError(
      error_message="No need to change position side.", status_code=-4059
    )

  monkeypatch.setattr(gw_mod, "send_request", already)
  assert gw.set_position_mode(True) is True


def test_set_position_mode_returns_false_on_reject(monkeypatch):
  # A genuine rejection (e.g. -4068 with open positions/orders) is best-effort:
  # logged and reported as False so startup proceeds on the current mode.
  from binance_common.errors import BadRequestError

  gw, _ = make_gateway(monkeypatch)

  def boom(session, config, *, method, path, payload, is_signed, response_model):
    raise BadRequestError(
      error_message="Position side cannot be changed", status_code=-4068
    )

  monkeypatch.setattr(gw_mod, "send_request", boom)
  assert gw.set_position_mode(True) is False
