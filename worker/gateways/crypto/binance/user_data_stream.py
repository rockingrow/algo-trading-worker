"""
worker/gateways/crypto/binance/user_data_stream.py
──────────────────────────────────────────────────
Event ingestion from Binance — the CRYPTO counterpart of ``MT5EventJob``.

Uses the **official SDK** websocket (``binance_sdk_derivatives_trading_usds_futures``)
for the User Data Stream — it manages the connection and auto-reconnect/renew —
while we manage the ``listenKey`` (start + periodic keepalive) and convert each
``ORDER_TRADE_UPDATE`` frame into a broker-neutral :class:`ExchangeCloseEvent`.

The frame-parsing logic is a pure, fully unit-tested function
(:func:`parse_order_trade_update`) that works on the raw Binance payload; the SDK
delivers typed events, so :meth:`BinanceUserDataStream._to_raw_dict` unwraps them
back to that raw shape before parsing.
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional

from worker.gateways.crypto.base import WORKER_ORDER_PREFIX
from worker.logger import get_logger

log = get_logger("worker.gateways.crypto.binance.user_data_stream")

_KEEPALIVE_INTERVAL = 30 * 60   # seconds (Binance expires the listenKey at 60 min)
_KEEPALIVE_RETRY   = 30        # seconds — back off this long after a failed keepalive
_PING_INTERVAL     =  3 * 60   # seconds between active WS ping probes
_PING_TIMEOUT      = 15        # seconds to wait for the ping frame to be written


class ExchangeCloseReason(str, Enum):
  SL = "SL"
  TP = "TP"
  LIQUIDATION = "LIQUIDATION"
  MANUAL = "MANUAL"


@dataclass
class ExchangeCloseEvent:
  """A position-closing fill that originated on the exchange (not via the worker)."""

  symbol: str
  reason: ExchangeCloseReason
  close_price: float
  close_volume: float
  order_id: str
  client_order_id: str
  realized_pnl: float = 0.0


# Original order types that represent an exchange-managed protective exit.
_STOP_TYPES = {"STOP_MARKET", "STOP"}
_TP_TYPES = {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}
_LIQUIDATION_TYPES = {"LIQUIDATION"}

# Binance system-generated clientOrderId prefixes for forced/automatic exits. A
# reduce-only MARKET fill bearing one of these is a liquidation/ADL/settlement,
# not a discretionary manual close.
_LIQUIDATION_PREFIXES = ("autoclose-", "adl_autoclose", "settlement_autoclose")


def parse_order_trade_update(msg: Dict[str, Any]) -> Optional[ExchangeCloseEvent]:
  """Translate one Binance frame into an :class:`ExchangeCloseEvent`, or None.

  Returns an event for any *exchange-side* close that has actually filled:
  stop-loss / take-profit / liquidation, **and** a discretionary manual close
  done outside the worker (web UI, mobile, a third-party API client). The only
  fills ignored are the worker's own reduce-only MARKET closes — identified by
  the ``WORKER_ORDER_PREFIX`` clientOrderId marker — and non-fill updates, since
  those are already handled by the normal signal flow.
  """
  if msg.get("e") != "ORDER_TRADE_UPDATE":
    return None
  o = msg.get("o") or {}
  if o.get("X") != "FILLED":
    return None

  original_type = o.get("ot") or o.get("o")
  client_id = o.get("c") or ""
  if original_type in _STOP_TYPES:
    reason = ExchangeCloseReason.SL
  elif original_type in _TP_TYPES:
    reason = ExchangeCloseReason.TP
  elif original_type in _LIQUIDATION_TYPES:
    reason = ExchangeCloseReason.LIQUIDATION
  elif original_type == "MARKET" and o.get("R"):
    # A reduce-only MARKET fill. If the worker placed it, it carries our marker
    # and is dropped (already handled by the signal flow). Otherwise it is an
    # external close: a Binance auto-exit prefix means liquidation/ADL, anything
    # else is a discretionary manual close.
    if client_id.startswith(WORKER_ORDER_PREFIX):
      return None
    reason = (
      ExchangeCloseReason.LIQUIDATION
      if client_id.startswith(_LIQUIDATION_PREFIXES)
      else ExchangeCloseReason.MANUAL
    )
  else:
    return None

  symbol = o.get("s", "")
  close_price = float(o.get("ap") or o.get("L") or 0)
  close_volume = float(o.get("z") or o.get("q") or 0)
  # A protective exit must carry a symbol and a non-zero fill price/volume; a
  # frame missing those is malformed — drop it rather than reconcile a degenerate
  # (symbol="", price=0) close that could never match a DB row anyway.
  if not symbol or close_price <= 0 or close_volume <= 0:
    log.warning(
      "[parse_order_trade_update] Ignoring %s fill with incomplete fields "
      "(symbol=%r price=%s volume=%s).",
      reason.value, symbol, close_price, close_volume,
    )
    return None

  return ExchangeCloseEvent(
    symbol=symbol,
    reason=reason,
    close_price=close_price,
    close_volume=close_volume,
    order_id=str(o.get("i") or ""),
    client_order_id=client_id,
    realized_pnl=float(o.get("rp") or 0),
  )


class BinanceUserDataStream:
  """Daemon that streams Binance user events (via the SDK) and dispatches closes.

  *handler* is called with each :class:`ExchangeCloseEvent`.
  """

  def __init__(
    self,
    api_key: str,
    api_secret: str,
    testnet: bool,
    handler: Callable[[ExchangeCloseEvent], None],
  ) -> None:
    self._api_key = api_key
    self._api_secret = api_secret
    self._testnet = testnet
    self._handler = handler
    self._stop_event = threading.Event()
    self._thread: Optional[threading.Thread] = None

  # ── lifecycle ─────────────────────────────────────────────────────────── #

  def start(self, stop_event=None) -> None:
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run, name="binance-user-stream", daemon=True
    )
    self._thread.start()
    log.info("[BinanceUserDataStream] Started.")

  def stop(self) -> None:
    self._stop_event.set()

  # ── internals ─────────────────────────────────────────────────────────── #

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        asyncio.run(self._stream())
      except Exception as exc:
        log.exception("[BinanceUserDataStream] stream error: %s", exc)
      if not self._stop_event.is_set():
        self._stop_event.wait(5)  # backoff before reconnecting

  def _build_client(self):
    # Lazy import so the SDK (aiohttp etc.) loads only when the stream runs.
    import ssl

    import certifi
    from binance_common.configuration import (
      ConfigurationRestAPI,
      ConfigurationWebSocketStreams,
    )
    from binance_common.constants import (
      DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
      DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
      DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
      DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL,
    )
    from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
      DerivativesTradingUsdsFutures,
    )

    # Create explicit SSL context with certifi CA bundle — required on Windows Server
    # where the default SSL context fails certificate verification for aiohttp
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    rest_cfg = ConfigurationRestAPI(
      api_key=self._api_key,
      api_secret=self._api_secret,
      base_path=(
        DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL
        if self._testnet
        else DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
      ),
    )
    ws_cfg = ConfigurationWebSocketStreams(
      stream_url=(
        DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_TESTNET_URL
        if self._testnet
        else DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL
      ),
      https_agent=ssl_context
    )
    return DerivativesTradingUsdsFutures(
      config_rest_api=rest_cfg, config_ws_streams=ws_cfg
    )

  @staticmethod
  def _listen_key(client) -> Optional[str]:
    data = client.rest_api.start_user_data_stream().data()
    key = getattr(data, "listen_key", None)
    if key is None and isinstance(data, dict):
      key = data.get("listenKey")
    return key

  async def _ping_check(self, connection: Any) -> bool:
    """Send a WS PING on the private connection. Return True if the loop should break.

    Active liveness probe: a write failure or a closed socket means the connection
    is dead — reconnect immediately instead of waiting for a silence timeout that
    fires spuriously on quiet markets (no fills → no data messages).
    SDK ping() swallows exceptions, so we go one layer lower to the aiohttp
    ClientWebSocketResponse directly.
    """
    ws_conn = next(
      (c for c in connection.connections if getattr(c, "url_path", None) == "private"),
      None,
    )
    if ws_conn is None or ws_conn.websocket.closed:
      log.warning("[BinanceUserDataStream] Private WebSocket unavailable — reconnecting.")
      return True
    try:
      await asyncio.wait_for(ws_conn.websocket.ping(), timeout=_PING_TIMEOUT)
      log.debug("[BinanceUserDataStream] WS ping OK.")
      return False
    except Exception as exc:
      log.warning(
        "[BinanceUserDataStream] WS ping failed (%s) — reconnecting.", type(exc).__name__
      )
      return True

  async def _keepalive_check(self, client: Any, now: float) -> float:
    """Refresh the listenKey and return the timestamp for the next keepalive."""
    try:
      await asyncio.to_thread(client.rest_api.keepalive_user_data_stream)
      log.debug("[BinanceUserDataStream] listenKey kept alive.")
      return now + _KEEPALIVE_INTERVAL
    except Exception as exc:
      # Back off instead of retrying every loop tick — advance the next attempt.
      log.warning(
        "[BinanceUserDataStream] keepalive failed: %s — retrying in %ds.",
        exc, _KEEPALIVE_RETRY,
      )
      return now + _KEEPALIVE_RETRY

  async def _stream(self) -> None:
    client = self._build_client()
    listen_key = await asyncio.to_thread(self._listen_key, client)
    if not listen_key:
      raise RuntimeError("Failed to obtain listenKey")

    connection = await client.websocket_streams.create_connection()
    next_keepalive = time.monotonic() + _KEEPALIVE_INTERVAL
    next_ping = time.monotonic() + _PING_INTERVAL
    log.info("[BinanceUserDataStream] Connecting to user data stream.")
    try:
      stream = await connection.user_data(listenKey=listen_key)
      stream.on("message", self._on_message)
      log.info("[BinanceUserDataStream] Subscribed.")

      while not self._stop_event.is_set():
        await asyncio.sleep(1.0)
        now = time.monotonic()

        if now >= next_ping:
          if await self._ping_check(connection):
            break
          next_ping = now + _PING_INTERVAL

        if now >= next_keepalive:
          next_keepalive = await self._keepalive_check(client, now)

    finally:
      try:
        await client.websocket_streams.close_connection()
      except Exception:  # pragma: no cover - best effort
        pass
      # Best-effort release of the SDK client's own transports (REST aiohttp
      # session) so repeated reconnects don't accumulate sockets/connectors.
      close = getattr(client, "close", None)
      if close is not None:
        try:
          res = close()
          if asyncio.iscoroutine(res):
            await res
        except Exception:  # pragma: no cover - best effort
          pass

  # ── message handling ──────────────────────────────────────────────────── #

  @staticmethod
  def _to_raw_dict(event: Any) -> Dict[str, Any]:
    """Unwrap an SDK typed user event back to the raw Binance payload dict."""
    inst = getattr(event, "actual_instance", event)
    if hasattr(inst, "model_dump"):
      return inst.model_dump(by_alias=True, exclude_none=True)
    return inst if isinstance(inst, dict) else {}

  def _on_message(self, event: Any) -> None:
    try:
      parsed = parse_order_trade_update(self._to_raw_dict(event))
    except Exception as exc:
      log.exception("[BinanceUserDataStream] failed to parse event: %s", exc)
      return
    if parsed is not None:
      self._dispatch(parsed)

  def _dispatch(self, event: ExchangeCloseEvent) -> None:
    try:
      self._handler(event)
    except Exception as exc:
      log.exception("[BinanceUserDataStream] handler failed: %s", exc)
