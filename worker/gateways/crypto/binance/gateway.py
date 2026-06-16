"""
worker/gateways/crypto/binance/gateway.py
─────────────────────────────────────────
Binance USDⓈ-M Futures adapter — concrete ``BaseExchangeGateway``.

REST goes through the **official** ``binance_common`` transport
(``send_request``): it signs (HMAC), adds the timestamp, applies retries/backoff,
raises typed rate-limit/error exceptions, and converts snake_case params to the
Binance wire format (``stop_price`` → ``stopPrice`` …). We call it directly
(rather than the generated typed methods) so every endpoint goes through one
uniform path. Note stop-losses are conditional orders: Binance no longer accepts
``STOP_MARKET`` on ``/fapi/v1/order`` (it returns -4120 → "use the Algo Order API
endpoints instead"), so :meth:`set_stop_loss` posts to ``/fapi/v1/algoOrder``
(``algoType=CONDITIONAL``; docs:
https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order).
The worker speaks LONG/SHORT; this
adapter maps that to BUY/SELL and normalizes responses into the broker-neutral
``TradeResult`` / ``ExchangePosition`` shapes.

The websocket user-data stream (fills / SL / TP / liquidation) is provided by the
official SDK — see :mod:`worker.gateways.crypto.binance.user_data_stream`.
"""

from __future__ import annotations

import time
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

import binance_common.utils as _bc_utils
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


# ── Server-time offset ────────────────────────────────────────────────────── #
# Binance rejects a signed request whose ``timestamp`` runs more than 1000ms
# *ahead* of server time (error -1021). ``recvWindow`` only forgives timestamps
# that are *behind* server time, so it cannot rescue a fast clock — which is the
# common failure inside containers whose host clock has drifted. The SDK's
# ``send_request`` stamps every signed request with ``binance_common.utils
# .get_timestamp()`` (raw local clock) and exposes no offset hook, so we wrap
# that module-level function to add an offset measured against Binance at
# ``connect()`` (see :meth:`BinanceFuturesGateway._sync_time`). The wrap reads a
# module global so re-syncs take effect without re-patching; if a future SDK
# drops the symbol the wrap simply never runs and we fall back to the raw local
# clock (today's behavior).
_TIME_OFFSET_MS = 0


def _offset_timestamp() -> int:
  return int(time.time() * 1000) + _TIME_OFFSET_MS


_bc_utils.get_timestamp = _offset_timestamp


# ``str``-mixin Enum (not ``enum.StrEnum``, which is 3.11+) so the package keeps
# working on the declared minimum Python 3.10. ``__str__`` returns the bare value
# so ``str(endpoint)`` yields the path (matching StrEnum), which ``_send`` relies on.
class _API_Endpoints(str, Enum):
  TIME = "/fapi/v1/time"
  BALANCE = "/fapi/v2/balance"
  EXCHANGE_INFO = "/fapi/v1/exchangeInfo"
  PREMIUM_INDEX = "/fapi/v1/premiumIndex"
  POSITION_RISK = "/fapi/v2/positionRisk"
  ORDER = "/fapi/v1/order"
  ALGO_ORDER = "/fapi/v1/algoOrder"
  ALGO_OPEN_ORDERS = "/fapi/v1/algoOpenOrders"
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
    """Send one request via the official transport and return its parsed body.

    Signed requests re-sync the clock and retry once on a -1021 timestamp
    rejection: the offset is measured at connect(), but an undisciplined host
    clock keeps drifting, so a long-lived worker can drift back outside Binance's
    1000ms-ahead window mid-session. Re-syncing on demand keeps the offset honest
    without a background timer, and one retry turns a would-be order failure into
    a transparent recovery.
    """
    params = dict(payload or {})
    if signed:
      params.setdefault("recv_window", self._recv_window)

    def _call() -> Any:
      return send_request(
        self._session,
        self._config,
        method=method,
        path=str(path),
        payload=params,
        is_signed=signed,
        response_model=None,
      ).data()

    try:
      return _call()
    except Exception as exc:
      if signed and getattr(exc, "status_code", None) == -1021:
        logger.warning(
          "[Binance] -1021 timestamp rejection on %s; re-syncing clock and retrying once.",
          path,
        )
        self._sync_time()
        return _call()
      raise

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  def connect(self) -> bool:
    try:
      self._sync_time()
      self._send("GET", _API_Endpoints.BALANCE, signed=True)
      logger.info("[Binance] Connected (testnet=%s).", self._testnet)
      return True
    except Exception as exc:
      logger.exception("[Binance] connect() failed: %s", exc)
      return False

  def _sync_time(self) -> None:
    """Align signed-request timestamps to Binance server time.

    Measures the local-vs-server clock offset via the public (unsigned)
    ``/fapi/v1/time`` endpoint and stores it so :func:`_offset_timestamp` adds it
    to every signed request, sidestepping -1021 when the container clock drifts.
    The offset is corrected for transit by anchoring to the midpoint of the
    round trip rather than to when the reply arrived. Best effort: on failure we
    log and fall back to the raw local clock.
    """
    global _TIME_OFFSET_MS
    try:
      t0 = time.time() * 1000
      data = self._send("GET", _API_Endpoints.TIME)
      t1 = time.time() * 1000
      server_time = int(data["serverTime"])
      _TIME_OFFSET_MS = int(server_time - (t0 + t1) / 2)
      logger.info(
        "[Binance] Clock offset vs server: %+d ms (rtt=%.0f ms).",
        _TIME_OFFSET_MS,
        t1 - t0,
      )
    except Exception as exc:
      logger.warning("[Binance] time sync failed (%s); using local clock.", exc)

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
        notional = filters.get("MIN_NOTIONAL", filters.get("NOTIONAL", {}))
        out[s["symbol"]] = SymbolFilter(
          step_size=float(lot.get("stepSize", 0) or 0),
          min_qty=float(lot.get("minQty", 0) or 0),
          tick_size=float(price.get("tickSize", 0) or 0),
          min_notional=float(notional.get("minNotional", 0) or 0),
        )
    except Exception as exc:
      logger.exception("[Binance] exchangeInfo failed: %s", exc)
    return out

  def get_mark_price(self, symbol: str) -> float:
    data = self._send("GET", _API_Endpoints.PREMIUM_INDEX, {"symbol": symbol})
    return float(data["markPrice"])

  def _round_to_tick(self, symbol: str, price: float) -> float:
    """Snap *price* to the symbol's ``tick_size`` grid.

    Binance rejects any order price that is not an exact multiple of the
    symbol's PRICE_FILTER tick with -1111 ("Precision is over the maximum
    defined for this asset"). A round SL like 63500 passes by luck, but a
    breakeven stop placed at a live entry price (e.g. 63764.73) does not — so
    every price we send to the wire must be snapped here first.
    """
    tick = self.get_symbol_filter(symbol).tick_size
    if not tick or tick <= 0:
      return price
    steps = round(price / tick)
    decimals = max(0, -Decimal(str(tick)).normalize().as_tuple().exponent)
    return round(steps * tick, decimals)

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
      # Ask for the filled order back (avgPrice / executedQty). The default ACK
      # response omits both, so without this the recorded fill price and volume
      # are persisted as 0 — corrupting the DB row and every downstream event.
      "new_order_resp_type": "RESULT",
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
    # Conditional orders go on the dedicated Algo Order endpoint
    # (algoType=CONDITIONAL); Binance rejects STOP_MARKET on /fapi/v1/order with
    # -4120. Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order
    # The trigger field is `trigger_price` here (vs `stop_price` on the legacy order endpoint).
    #
    # closePosition=true closes the whole remaining position when the stop trips —
    # exactly the breakeven-after-TP1 behaviour the strategy wants — and must not
    # be combined with quantity/reduceOnly. NOTE: *quantity* is kept for
    # BaseExchangeGateway parity but is intentionally unused.
    payload: Dict[str, Any] = {
      "algo_type": "CONDITIONAL",
      "symbol": symbol,
      "side": _CLOSE_SIDE.get(position_side, "SELL"),
      "type": "STOP_MARKET",
      # Snap to tick_size — an off-grid trigger price is rejected with -1111.
      "trigger_price": self._round_to_tick(symbol, stop_price),
      "close_position": "true",
    }
    try:
      return self._order_result(self._send("POST", _API_Endpoints.ALGO_ORDER, payload, signed=True))
    except Exception as exc:
      logger.exception("[Binance] set_stop_loss failed: %s", exc)
      return TradeResult.fail(str(exc))

  def cancel_all_orders(self, symbol: str) -> None:
    # Regular orders and algo (conditional) orders live in separate Binance systems.
    # allOpenOrders covers standard orders; algoOpenOrders covers STOP_MARKET/TP
    # conditionals placed via /fapi/v1/algoOrder. Both must be cancelled so an old
    # SL algo doesn't survive into the next stop-placement (double-stop scenario).
    try:
      self._send("DELETE", _API_Endpoints.ALL_OPEN_ORDERS, {"symbol": symbol}, signed=True)
    except Exception as exc:
      logger.warning("[Binance] cancel_all_orders(%s) failed: %s", symbol, exc)
    try:
      self._send("DELETE", _API_Endpoints.ALGO_OPEN_ORDERS, {"symbol": symbol}, signed=True)
    except Exception as exc:
      logger.warning("[Binance] cancel_algo_orders(%s) failed: %s", symbol, exc)

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
    # Market/limit orders return `orderId`; conditional algo orders return `algoId`.
    order_id = data.get("orderId") or data.get("algoId")
    avg_price = float(data.get("avgPrice", 0) or 0)
    exec_qty = float(data.get("executedQty", 0) or 0)
    # Binance Futures testnet (and occasionally live) returns avgPrice="0" on
    # MARKET fills even though the order was fully executed.  cumQuote (total
    # quote asset transacted) / executedQty always gives the correct average.
    if avg_price == 0 and exec_qty > 0:
      cum_quote = float(data.get("cumQuote", 0) or 0)
      if cum_quote > 0:
        avg_price = cum_quote / exec_qty
    return TradeResult.ok(
      ticket=str(order_id) if order_id is not None else None,
      price=avg_price,
      volume=exec_qty,
      comment=data.get("status", "NEW"),
    )
