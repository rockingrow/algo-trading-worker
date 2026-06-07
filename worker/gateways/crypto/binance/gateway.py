"""
worker/crypto/binance/gateway.py
────────────────────────────────
Binance USDⓈ-M Futures adapter — the first concrete ``BaseExchangeGateway``.

Talks to the Binance Futures REST API with HMAC-SHA256 signed requests. The
worker speaks LONG/SHORT; this adapter maps that to Binance BUY/SELL and
normalizes responses back into the broker-neutral result dict / ``ExchangePosition``
shapes the rest of the worker understands.

Only the surface ``CryptoExecutor`` and the user-data-stream job need is
implemented (Interface Segregation). Network I/O uses ``requests`` (already a
project dependency); no Binance SDK is pulled in.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from enum import StrEnum
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from worker.gateways.crypto.base import (
  SIDE_LONG,
  SIDE_SHORT,
  BaseExchangeGateway,
  ExchangePosition,
  SymbolFilter,
)
from worker.logger import get_logger
from worker.schemas.trade_result import TradeResult

logger = get_logger("worker.gateways.crypto.binance.gateway")

_MAINNET = "https://fapi.binance.com"
_TESTNET = "https://testnet.binancefuture.com"


class _API_Endpoints(StrEnum):
  BALANCE = "/fapi/v2/balance"
  EXCHANGE_INFO = "/fapi/v1/exchangeInfo"
  PREMIUM_INDEX = "/fapi/v1/premiumIndex"
  POSITION_RISK = "/fapi/v2/positionRisk"
  ORDER = "/fapi/v1/order"
  ALL_OPEN_ORDERS = "/fapi/v1/allOpenOrders"
  ACCOUNT = "/fapi/v2/account"
  LISTEN_KEY = "/fapi/v1/listenKey"

# Binance order side mapping for opening positions in one-way mode.
_OPEN_SIDE = {SIDE_LONG: "BUY", SIDE_SHORT: "SELL"}
# To reduce/close a LONG you SELL; to reduce/close a SHORT you BUY.
_CLOSE_SIDE = {SIDE_LONG: "SELL", SIDE_SHORT: "BUY"}


class BinanceFuturesGateway(BaseExchangeGateway):
  """Binance USDⓈ-M Futures implementation of :class:`BaseExchangeGateway`."""

  name = "BINANCE"

  def __init__(
    self,
    api_key: str,
    api_secret: str,
    testnet: bool = False,
    recv_window: int = 5000,
    session: Optional[requests.Session] = None,
  ) -> None:
    self._api_key = api_key
    self._api_secret = api_secret.encode()
    self._base_url = _TESTNET if testnet else _MAINNET
    self._recv_window = recv_window
    self._session = session or requests.Session()
    self._session.headers.update({"X-MBX-APIKEY": api_key})
    # Cache symbol filters; exchangeInfo is large and rarely changes.
    self._filters: Dict[str, SymbolFilter] = {}

  @property
  def base_url(self) -> str:
    return self._base_url

  # ── Signing / transport ───────────────────────────────────────────────── #

  def _sign(self, params: Dict[str, Any]) -> str:
    query = urlencode(params)
    signature = hmac.new(self._api_secret, query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"

  def _request(
    self, method: str, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = False
  ) -> Any:
    params = dict(params or {})
    url = f"{self._base_url}{path}"
    if signed:
      params["timestamp"] = int(time.time() * 1000)
      params["recvWindow"] = self._recv_window
      url = f"{url}?{self._sign(params)}"
      resp = self._session.request(method, url, timeout=10)
    else:
      resp = self._session.request(method, url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  def connect(self) -> bool:
    try:
      self._request("GET", _API_Endpoints.BALANCE, signed=True)
      logger.info("[Binance] Connected (%s).", self._base_url)
      return True
    except Exception as exc:
      logger.exception("[Binance] connect() failed: %s", exc)
      return False

  def close(self) -> None:
    try:
      self._session.close()
    except Exception:  # pragma: no cover - best effort
      pass

  # ── Market data / rules ───────────────────────────────────────────────── #

  def get_symbol_filter(self, symbol: str) -> SymbolFilter:
    if symbol in self._filters:
      return self._filters[symbol]
    try:
      info = self._request("GET", _API_Endpoints.EXCHANGE_INFO, {"symbol": symbol})
      symbols = info.get("symbols", [])
      if not symbols:
        return SymbolFilter()
      filters = {f["filterType"]: f for f in symbols[0].get("filters", [])}
      lot = filters.get("LOT_SIZE", {})
      price = filters.get("PRICE_FILTER", {})
      sf = SymbolFilter(
        step_size=float(lot.get("stepSize", 0) or 0),
        min_qty=float(lot.get("minQty", 0) or 0),
        tick_size=float(price.get("tickSize", 0) or 0),
      )
      self._filters[symbol] = sf
      return sf
    except Exception as exc:
      logger.exception("[Binance] get_symbol_filter(%s) failed: %s", symbol, exc)
      return SymbolFilter()

  def get_mark_price(self, symbol: str) -> float:
    data = self._request("GET", _API_Endpoints.PREMIUM_INDEX, {"symbol": symbol})
    return float(data["markPrice"])

  # ── Positions ─────────────────────────────────────────────────────────── #

  def get_positions(self, symbol: Optional[str] = None) -> List[ExchangePosition]:
    params = {"symbol": symbol} if symbol else {}
    rows = self._request("GET", _API_Endpoints.POSITION_RISK, params, signed=True)
    positions: List[ExchangePosition] = []
    for row in rows:
      amt = float(row.get("positionAmt", 0) or 0)
      if amt == 0:
        continue
      entry = float(row.get("entryPrice", 0) or 0)
      positions.append(
        ExchangePosition(
          symbol=row["symbol"],
          side=SIDE_LONG if amt > 0 else SIDE_SHORT,
          quantity=amt,
          entry_price=entry,
          # Binance has no per-position ticket; derive a stable synthetic id
          # from the symbol so the same position maps consistently.
          ticket=abs(hash(row["symbol"])) % (10**9),
          unrealized_pnl=float(row.get("unRealizedProfit", 0) or 0),
        )
      )
    return positions

  # ── Orders ────────────────────────────────────────────────────────────── #

  def place_market_order(
    self,
    symbol: str,
    side: str,
    quantity: float,
    reduce_only: bool = False,
    client_order_id: Optional[str] = None,
  ) -> TradeResult:
    order_side = (_CLOSE_SIDE if reduce_only else _OPEN_SIDE).get(side)
    if order_side is None:
      return TradeResult.fail(f"Bad side: {side}")

    params: Dict[str, Any] = {
      "symbol": symbol,
      "side": order_side,
      "type": "MARKET",
      "quantity": quantity,
    }
    if reduce_only:
      params["reduceOnly"] = "true"
    if client_order_id:
      params["newClientOrderId"] = client_order_id

    try:
      data = self._request("POST", _API_Endpoints.ORDER, params, signed=True)
      return self._order_result(data)
    except Exception as exc:
      logger.exception("[Binance] place_market_order failed: %s", exc)
      return TradeResult.fail(str(exc))

  def set_stop_loss(
    self, symbol: str, position_side: str, stop_price: float, quantity: float
  ) -> TradeResult:
    # closePosition=true closes the whole remaining position when the stop trips,
    # which is exactly the breakeven-after-TP1 behavior the strategy wants.
    params: Dict[str, Any] = {
      "symbol": symbol,
      "side": _CLOSE_SIDE.get(position_side, "SELL"),
      "type": "STOP_MARKET",
      "stopPrice": stop_price,
      "closePosition": "true",
    }
    try:
      data = self._request("POST", _API_Endpoints.ORDER, params, signed=True)
      return self._order_result(data)
    except Exception as exc:
      logger.exception("[Binance] set_stop_loss failed: %s", exc)
      return TradeResult.fail(str(exc))

  def cancel_all_orders(self, symbol: str) -> None:
    try:
      self._request("DELETE", _API_Endpoints.ALL_OPEN_ORDERS, {"symbol": symbol}, signed=True)
    except Exception as exc:
      logger.warning("[Binance] cancel_all_orders(%s) failed: %s", symbol, exc)

  # ── Account ───────────────────────────────────────────────────────────── #

  def get_account(self) -> Optional[Dict[str, Any]]:
    try:
      data = self._request("GET", _API_Endpoints.ACCOUNT, signed=True)
      return {
        "balance": float(data.get("totalWalletBalance", 0) or 0),
        "equity": float(data.get("totalMarginBalance", 0) or 0),
        "available": float(data.get("availableBalance", 0) or 0),
      }
    except Exception as exc:
      logger.warning("[Binance] get_account failed: %s", exc)
      return None

  # ── User data stream (used by the event job) ──────────────────────────── #

  def create_listen_key(self) -> str:
    data = self._request("POST", _API_Endpoints.LISTEN_KEY)
    return data["listenKey"]

  def keepalive_listen_key(self) -> None:
    self._request("PUT", _API_Endpoints.LISTEN_KEY)

  @property
  def ws_base_url(self) -> str:
    return (
      "wss://stream.binancefuture.com/ws"
      if self._base_url == _TESTNET
      else "wss://fstream.binance.com/ws"
    )

  def create_event_stream(self, handler):
    # Lazy import keeps the websocket dependency out of import time.
    from worker.gateways.crypto.binance.user_data_stream import BinanceUserDataStream

    return BinanceUserDataStream(self, handler)

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _order_result(data: Dict[str, Any]) -> TradeResult:
    return TradeResult.ok(
      ticket=data.get("orderId"),
      price=float(data.get("avgPrice", 0) or 0),
      volume=float(data.get("executedQty", 0) or 0),
      comment=data.get("status", "NEW"),
    )
