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

import re
import time
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, TypedDict

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


class _API_Endpoints(Enum):
  TIME             = ("GET",    "/fapi/v1/time")
  BALANCE          = ("GET",    "/fapi/v2/balance")
  EXCHANGE_INFO    = ("GET",    "/fapi/v1/exchangeInfo")
  PREMIUM_INDEX    = ("GET",    "/fapi/v1/premiumIndex")
  POSITION_RISK    = ("GET",    "/fapi/v2/positionRisk")
  ORDER            = ("POST",   "/fapi/v1/order")
  ALGO_ORDER       = ("POST",   "/fapi/v1/algoOrder")
  ALGO_OPEN_ORDERS = ("DELETE", "/fapi/v1/algoOpenOrders")
  ALL_OPEN_ORDERS  = ("DELETE", "/fapi/v1/allOpenOrders")
  ACCOUNT          = ("GET",    "/fapi/v2/account")
  LEVERAGE_BRACKET = ("GET",    "/fapi/v1/leverageBracket")
  LEVERAGE         = ("POST",   "/fapi/v1/leverage")
  POSITION_MODE    = ("POST",   "/fapi/v1/positionSide/dual")

  @property
  def method(self) -> str:
    return self.value[0]

  @property
  def path(self) -> str:
    return self.value[1]


# Binance error code for an account-level leverage cap (sub-account / VIP tier
# restriction). This ceiling is NOT exposed by /fapi/v1/leverageBracket — it
# surfaces only when POSTing /fapi/v1/leverage, with the real limit embedded in
# the message ("Subaccounts are restricted from using leverage greater than 5x.").
_LEVERAGE_CAP_CODE = -4421
_LEVERAGE_CAP_RE = re.compile(r"greater than (\d+)\s*x", re.IGNORECASE)

# Binance returns -4059 ("No need to change position side.") when the account is
# already in the requested position mode. That is the desired end state, so we
# treat it as success rather than a failure.
_POSITION_MODE_NO_CHANGE_CODE = -4059


# Shape of the POST /fapi/v1/leverage response. ``total=False`` because the
# worker only relies on ``leverage`` (the value the exchange actually applied,
# which may be clamped below the request) and tolerates the rest being absent.
class LeverageResponse(TypedDict, total=False):
  symbol: str
  leverage: int
  maxNotionalValue: str


# Binance order side mapping. Valid in both One-way and Hedge position modes —
# Hedge mode additionally requires positionSide, added by the callers below.
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
    hedge_mode: bool = False,
    session: Optional[requests.Session] = None,
  ) -> None:
    self._api_key = api_key
    self._api_secret = api_secret
    self._testnet = testnet
    self._recv_window = recv_window
    # Must match the account's actual Binance position mode (Preferences >
    # Position Mode). One-way accounts default positionSide to BOTH and infer
    # direction from `side` alone; Hedge accounts require every order to carry
    # an explicit positionSide (LONG/SHORT) and reject `reduceOnly` outright
    # (-1106) since positionSide + side already disambiguate open vs close.
    # Mismatching this flag against the account setting is exactly what
    # produces -4061 ("Order's position side does not match user's setting").
    self._hedge_mode = hedge_mode
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
    endpoint: _API_Endpoints,
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
        method=endpoint.method,
        path=endpoint.path,
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
          endpoint.path,
        )
        self._sync_time()
        return _call()
      raise

  # ── Lifecycle ─────────────────────────────────────────────────────────── #

  def connect(self) -> bool:
    try:
      self._sync_time()
      self._send(_API_Endpoints.BALANCE, signed=True)
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
      data = self._send(_API_Endpoints.TIME)
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

  def set_position_mode(self, hedge_mode: bool) -> bool:
    """Switch the account into Hedge (dualSidePosition=true) or One-way mode.

    Best-effort: Binance returns -4059 ("No need to change position side.") when
    the account is already in the requested mode — that is the desired end state,
    so it counts as success. A genuine failure (e.g. -4068 when open positions or
    orders block the switch) is logged and returns False so the worker keeps
    running on the account's current mode; the mismatch then surfaces as -4061 on
    the first order, exactly as before this reconciliation existed.
    """
    target = "Hedge" if hedge_mode else "One-way"
    try:
      self._send(
        _API_Endpoints.POSITION_MODE,
        {"dual_side_position": "true" if hedge_mode else "false"},
        signed=True,
      )
      logger.info("[Binance] Position mode set to %s.", target)
      return True
    except Exception as exc:
      if getattr(exc, "status_code", None) == _POSITION_MODE_NO_CHANGE_CODE:
        logger.info("[Binance] Position mode already %s; no change needed.", target)
        return True
      logger.warning(
        "[Binance] set_position_mode(%s) failed (%s); leaving the account on its "
        "current mode — a mismatch will surface as -4061 on the first order.",
        target, exc,
      )
      return False

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
      data = self._send(_API_Endpoints.EXCHANGE_INFO)
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
    data = self._send(_API_Endpoints.PREMIUM_INDEX, {"symbol": symbol})
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
    rows = self._send(_API_Endpoints.POSITION_RISK, payload, signed=True)
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
    if self._hedge_mode:
      # side param is already the position's LONG/SHORT (not the wire BUY/SELL),
      # so it's exactly the positionSide Binance wants for both opens and closes.
      payload["position_side"] = side
    elif reduce_only:
      payload["reduce_only"] = "true"
    if client_order_id:
      payload["new_client_order_id"] = client_order_id
    try:
      return self._order_result(self._send(_API_Endpoints.ORDER, payload, signed=True))
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
    if self._hedge_mode:
      payload["position_side"] = position_side
    try:
      return self._order_result(self._send(_API_Endpoints.ALGO_ORDER, payload, signed=True))
    except Exception as exc:
      logger.exception("[Binance] set_stop_loss failed: %s", exc)
      return TradeResult.fail(str(exc))

  def set_take_profit(
    self, symbol: str, position_side: str, tp_price: float, quantity: float
  ) -> TradeResult:
    # Take-profit targets are conditional orders too, so they go on the same Algo
    # Order endpoint as the stop (algoType=CONDITIONAL, TAKE_PROFIT_MARKET is one
    # of the supported types). closePosition=true closes the whole remaining
    # position when the target trips — so after a TP1 partial close it still flat-
    # tens the runner — and must not be combined with quantity/reduceOnly, so
    # *quantity* is accepted for BaseExchangeGateway parity but intentionally
    # unused. The user-data stream maps a TAKE_PROFIT_MARKET fill to a TP close.
    payload: Dict[str, Any] = {
      "algo_type": "CONDITIONAL",
      "symbol": symbol,
      "side": _CLOSE_SIDE.get(position_side, "SELL"),
      "type": "TAKE_PROFIT_MARKET",
      # Snap to tick_size — an off-grid trigger price is rejected with -1111.
      "trigger_price": self._round_to_tick(symbol, tp_price),
      "close_position": "true",
    }
    if self._hedge_mode:
      payload["position_side"] = position_side
    try:
      return self._order_result(self._send(_API_Endpoints.ALGO_ORDER, payload, signed=True))
    except Exception as exc:
      logger.exception("[Binance] set_take_profit failed: %s", exc)
      return TradeResult.fail(str(exc))

  def cancel_all_orders(self, symbol: str) -> None:
    # Regular orders and algo (conditional) orders live in separate Binance systems.
    # allOpenOrders covers standard orders; algoOpenOrders covers STOP_MARKET/TP
    # conditionals placed via /fapi/v1/algoOrder. Both must be cancelled so an old
    # SL algo doesn't survive into the next stop-placement (double-stop scenario).
    try:
      self._send(_API_Endpoints.ALL_OPEN_ORDERS, {"symbol": symbol}, signed=True)
    except Exception as exc:
      logger.warning("[Binance] cancel_all_orders(%s) failed: %s", symbol, exc)
    try:
      self._send(_API_Endpoints.ALGO_OPEN_ORDERS, {"symbol": symbol}, signed=True)
    except Exception as exc:
      logger.warning("[Binance] cancel_algo_orders(%s) failed: %s", symbol, exc)

  # ── Leverage ──────────────────────────────────────────────────────────── #

  def get_max_leverage(self, symbol: str) -> Optional[int]:
    """Max leverage Binance allows for *symbol* on this account.

    ``/fapi/v1/leverageBracket`` returns notional brackets in ascending order;
    the first bracket (lowest notional) carries the highest ``initialLeverage``,
    which is the symbol's *market-wide* ceiling. NOTE: account-level caps
    (sub-account / VIP-tier restrictions) are **not** reflected here — a
    sub-account limited to 5x still returns the market ceiling (e.g. 125). That
    cap surfaces only when setting leverage (-4421), where :meth:`set_leverage`
    detects and honors it. Returns ``None`` on error so the caller can skip
    rather than mis-size.
    """
    try:
      data = self._send(_API_Endpoints.LEVERAGE_BRACKET, {"symbol": symbol}, signed=True)
    except Exception as exc:
      logger.warning("[Binance] leverageBracket(%s) failed: %s", symbol, exc)
      return None
    # Response shape: list of {symbol, brackets:[...]} (one entry per symbol).
    # Older API versions returned a single dict directly — handle both.
    entry = data[0] if isinstance(data, list) and data else data
    brackets = (entry or {}).get("brackets") or []
    if not brackets:
      return None
    try:
      return int(brackets[0].get("initialLeverage"))
    except (TypeError, ValueError):
      return None

  def set_leverage(
    self, symbol: str, leverage: int, min_leverage_cap: Optional[int] = None
  ) -> Optional[int]:
    """Set per-symbol working leverage.

    Returns the leverage **actually applied** on success — which may be below
    the requested value when an account-level cap forces it lower — or ``None``
    on failure. Account caps (sub-account / VIP-tier) are not visible in
    :meth:`get_max_leverage`; they only appear here as a -4421 rejection that
    names the real ceiling, so we parse that ceiling and retry once at it rather
    than leaving the symbol on its stale exchange setting.

    ``min_leverage_cap`` is a *last-resort* floor for the case where the -4421
    message can no longer be parsed (e.g. Binance reworded it and the regex no
    longer matches): rather than give up and leave the symbol at its dangerous
    default, retry once at ``min(min_leverage_cap, leverage)``. It only fires on
    a genuine -4421 that we could not parse — a known-safe value the operator
    asserts no sub-account is restricted below. If the account is in fact capped
    lower, this retry also fails and the symbol is left untouched (logged loudly
    for manual fix).
    """
    leverage = int(leverage)
    try:
      data = self._send(_API_Endpoints.LEVERAGE, {"symbol": symbol, "leverage": leverage}, signed=True)
      # Return what the exchange actually applied (response carries the live
      # `leverage`), not the requested value — covers the case where Binance
      # silently clamps to a lower account cap instead of rejecting.
      return self._applied_leverage(data, leverage)
    except Exception as exc:
      cap = self._leverage_cap_from_error(exc, upper_bound=leverage)
      if cap is None and min_leverage_cap and self._is_leverage_cap_error(exc):
        # -4421 confirmed but the real ceiling could not be parsed — fall back
        # to the operator-asserted floor instead of abandoning the symbol.
        cap = min(int(min_leverage_cap), leverage)
        logger.warning(
          "[Binance] set_leverage(%s=%s) hit -4421 but cap was unparseable; "
          "falling back to known floor %sx.", symbol, leverage, cap,
        )
      if cap is not None and cap < leverage:
        logger.warning(
          "[Binance] set_leverage(%s=%s) rejected by account cap (-4421); "
          "retrying at %sx.", symbol, leverage, cap,
        )
        try:
          data = self._send(_API_Endpoints.LEVERAGE, {"symbol": symbol, "leverage": cap}, signed=True)
          return self._applied_leverage(data, cap)
        except Exception as exc2:
          logger.warning(
            "[Binance] set_leverage(%s=%s) retry failed: %s", symbol, cap, exc2
          )
          return None
      logger.warning(
        "[Binance] set_leverage(%s=%s) failed: %s", symbol, leverage, exc
      )
      return None

  @staticmethod
  def _applied_leverage(data: Optional[LeverageResponse], requested: int) -> int:
    """Leverage the exchange reports as applied (POST /fapi/v1/leverage echoes the
    live ``leverage``), falling back to *requested* if the field is absent/unparseable."""
    try:
      return int((data or {}).get("leverage"))
    except (TypeError, ValueError, AttributeError):
      return requested

  @staticmethod
  def _is_leverage_cap_error(exc: Exception) -> bool:
    """True if *exc* is Binance's -4421 account-leverage-cap rejection."""
    return getattr(exc, "status_code", None) == _LEVERAGE_CAP_CODE

  @classmethod
  def _leverage_cap_from_error(cls, exc: Exception, upper_bound: Optional[int] = None) -> Optional[int]:
    """Pull the account leverage ceiling out of a -4421 error, else ``None``.

    ``upper_bound`` (the leverage we requested, already ``min(exchange_max,
    MAX_LEVERAGE_CAP)``) clamps the parsed value: a -4421 cap is by definition a
    *restriction*, so anything at or above what we asked for is a parse anomaly,
    not a real ceiling, and is discarded.
    """
    if not cls._is_leverage_cap_error(exc):
      return None
    msg = getattr(exc, "error_message", None) or str(exc)
    match = _LEVERAGE_CAP_RE.search(msg)
    if not match:
      # The code says "account leverage cap" but the message no longer matches
      # our pattern — most likely Binance reworded it. Surface it so the regex
      # can be updated instead of silently losing the retry path.
      logger.warning(
        "[Binance] -4421 leverage cap but message did not match parser; "
        "skipping cap-retry. message=%r", msg,
      )
      return None
    cap = int(match.group(1))
    if cap <= 0:
      return None
    if upper_bound is not None and cap >= upper_bound:
      logger.warning(
        "[Binance] -4421 parsed cap %sx >= requested %sx — treating as parse "
        "anomaly and ignoring. message=%r", cap, upper_bound, msg,
      )
      return None
    return cap

  # ── Account ───────────────────────────────────────────────────────────── #

  def get_account(self) -> Optional[Dict[str, Any]]:
    try:
      data = self._send(_API_Endpoints.ACCOUNT, signed=True)
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
