"""
worker/gateways/crypto/binance/gateway.py
─────────────────────────────────────────
Binance USDⓈ-M Futures adapter — concrete ``BaseExchangeGateway``.

REST goes through the **official** ``binance_common`` transport
(``send_request``): it signs (HMAC), adds the timestamp, applies retries/backoff,
raises typed rate-limit/error exceptions, and converts snake_case params to the
Binance wire format. We call it directly (rather than the generated typed
methods) because the generated ``new_order`` cannot express ``STOP_MARKET`` /
``closePosition`` — which we need for stop-losses — and because going through one
transport keeps every endpoint uniform. The worker speaks LONG/SHORT; this
adapter maps that to BUY/SELL and normalizes responses into the broker-neutral
``TradeResult`` / ``ExchangePosition`` shapes.

The websocket user-data stream (fills / SL / TP / liquidation) is provided by the
official SDK — see :mod:`worker.gateways.crypto.binance.user_data_stream`.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

import requests
from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import (
  DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
  DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
)
from binance_common.utils import send_request

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


# ``str``-mixin Enum (not ``enum.StrEnum``, which is 3.11+) so the package keeps
# working on the declared minimum Python 3.10. ``__str__`` returns the bare value
# so ``str(endpoint)`` yields the path (matching StrEnum), which ``_send`` relies on.
class _API_Endpoints(str, Enum):
  BALANCE = "/fapi/v2/balance"
  EXCHANGE_INFO = "/fapi/v1/exchangeInfo"
  PREMIUM_INDEX = "/fapi/v1/premiumIndex"
  POSITION_RISK = "/fapi/v2/positionRisk"
  ORDER = "/fapi/v1/order"
  ALL_OPEN_ORDERS = "/fapi/v1/allOpenOrders"
  ACCOUNT = "/fapi/v2/account"

  def __str__(self) -> str:
    return self.value


# Binance order side mapping for one-way mode.
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
    self._api_secret = api_secret
    self._testnet = testnet
    self._recv_window = recv_window
    self._config = ConfigurationRestAPI(
      api_key=api_key,
      api_secret=api_secret,
      base_path=(
        DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL
        if testnet
        else DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
      ),
    )
    self._session = session or requests.Session()
    self._filters: Optional[Dict[str, SymbolFilter]] = None

  # ── Transport ─────────────────────────────────────────────────────────── #

  def _send(
    self,
    method: str,
    path: str,
    payload: Optional[dict] = None,
    *,
    signed: bool = False,
  ) -> Any:
    """Send one request via the official transport and return its parsed body."""
    params = dict(payload or {})
    if signed:
      params.setdefault("recv_window", self._recv_window)
    resp = send_request(
      self._session,
      self._config,
      method=method,
      path=str(path),
      payload=params,
      is_signed=signed,
      response_model=None,
    )
    return resp.data()

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  def connect(self) -> bool:
    try:
      self._send("GET", _API_Endpoints.BALANCE, signed=True)
      logger.info("[Binance] Connected (testnet=%s).", self._testnet)
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
    if self._filters is None:
      self._filters = self._load_filters()
    return self._filters.get(symbol, SymbolFilter())

  def _load_filters(self) -> Dict[str, SymbolFilter]:
    """Fetch exchangeInfo once and build per-symbol step/tick rules.

    Note: futures ``exchangeInfo`` returns *all* symbols (the ``symbol`` query is
    not honored), so we index the whole list rather than assuming ``symbols[0]``.
    """
    out: Dict[str, SymbolFilter] = {}
    try:
      data = self._send("GET", _API_Endpoints.EXCHANGE_INFO)
      for s in data.get("symbols", []):
        filters = {f["filterType"]: f for f in s.get("filters", [])}
        lot = filters.get("LOT_SIZE", {})
        price = filters.get("PRICE_FILTER", {})
        out[s["symbol"]] = SymbolFilter(
          step_size=float(lot.get("stepSize", 0) or 0),
          min_qty=float(lot.get("minQty", 0) or 0),
          tick_size=float(price.get("tickSize", 0) or 0),
        )
    except Exception as exc:
      logger.exception("[Binance] exchangeInfo failed: %s", exc)
    return out

  def get_mark_price(self, symbol: str) -> float:
    data = self._send("GET", _API_Endpoints.PREMIUM_INDEX, {"symbol": symbol})
    return float(data["markPrice"])

  # ── Positions ─────────────────────────────────────────────────────────── #

  def get_positions(self, symbol: Optional[str] = None) -> List[ExchangePosition]:
    payload = {"symbol": symbol} if symbol else None
    rows = self._send("GET", _API_Endpoints.POSITION_RISK, payload, signed=True)
    positions: List[ExchangePosition] = []
    for row in rows or []:
      amt = float(row.get("positionAmt", 0) or 0)
      if amt == 0:
        continue
      positions.append(
        ExchangePosition(
          symbol=row["symbol"],
          side=SIDE_LONG if amt > 0 else SIDE_SHORT,
          quantity=amt,
          entry_price=float(row.get("entryPrice", 0) or 0),
          # Binance has no per-position ticket; derive a process-local synthetic id.
          # hash() is randomised per Python process (hash seed), so this value
          # is NOT stable across restarts.  It is intentional: SignalHandler
          # overwrites it with ref_source_id from the DB before any DB write,
          # so it never reaches persistent storage.
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

    payload: Dict[str, Any] = {
      "symbol": symbol,
      "side": order_side,
      "type": "MARKET",
      "quantity": quantity,
    }
    if reduce_only:
      payload["reduce_only"] = "true"
    if client_order_id:
      payload["new_client_order_id"] = client_order_id
    try:
      return self._order_result(self._send("POST", _API_Endpoints.ORDER, payload, signed=True))
    except Exception as exc:
      logger.exception("[Binance] place_market_order failed: %s", exc)
      return TradeResult.fail(str(exc))

  def set_stop_loss(
    self, symbol: str, position_side: str, stop_price: float, quantity: float
  ) -> TradeResult:
    # STOP_MARKET + closePosition=true closes the whole remaining position when
    # the stop trips — exactly the breakeven-after-TP1 behavior the strategy wants.
    payload: Dict[str, Any] = {
      "symbol": symbol,
      "side": _CLOSE_SIDE.get(position_side, "SELL"),
      "type": "STOP_MARKET",
      "stop_price": stop_price,
      "close_position": "true",
    }
    try:
      return self._order_result(self._send("POST", _API_Endpoints.ORDER, payload, signed=True))
    except Exception as exc:
      logger.exception("[Binance] set_stop_loss failed: %s", exc)
      return TradeResult.fail(str(exc))

  def cancel_all_orders(self, symbol: str) -> None:
    try:
      self._send("DELETE", _API_Endpoints.ALL_OPEN_ORDERS, {"symbol": symbol}, signed=True)
    except Exception as exc:
      logger.warning("[Binance] cancel_all_orders(%s) failed: %s", symbol, exc)

  # ── Account ───────────────────────────────────────────────────────────── #

  def get_account(self) -> Optional[Dict[str, Any]]:
    try:
      data = self._send("GET", _API_Endpoints.ACCOUNT, signed=True)
      return {
        "balance": float(data.get("totalWalletBalance", 0) or 0),
        "equity": float(data.get("totalMarginBalance", 0) or 0),
        "available": float(data.get("availableBalance", 0) or 0),
      }
    except Exception as exc:
      logger.warning("[Binance] get_account failed: %s", exc)
      return None

  # ── Event ingestion ───────────────────────────────────────────────────── #

  def create_event_stream(self, handler):
    from worker.gateways.crypto.binance.user_data_stream import BinanceUserDataStream

    return BinanceUserDataStream(
      api_key=self._api_key,
      api_secret=self._api_secret,
      testnet=self._testnet,
      handler=handler,
    )

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _order_result(data: Dict[str, Any]) -> TradeResult:
    return TradeResult.ok(
      ticket=data.get("orderId"),
      price=float(data.get("avgPrice", 0) or 0),
      volume=float(data.get("executedQty", 0) or 0),
      comment=data.get("status", "NEW"),
    )
