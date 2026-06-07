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

from worker.logger import get_logger

log = get_logger("worker.gateways.crypto.binance.user_data_stream")

_KEEPALIVE_INTERVAL = 30 * 60  # seconds (Binance expires the listenKey at 60 min)


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
  order_id: int
  client_order_id: str
  realized_pnl: float = 0.0


# Original order types that represent an exchange-managed protective exit. A plain
# ``MARKET`` reduce-only fill is the worker closing a position itself and is
# ignored here to avoid double-handling.
_STOP_TYPES = {"STOP_MARKET", "STOP"}
_TP_TYPES = {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}
_LIQUIDATION_TYPES = {"LIQUIDATION"}


def parse_order_trade_update(msg: Dict[str, Any]) -> Optional[ExchangeCloseEvent]:
  """Translate one Binance frame into an :class:`ExchangeCloseEvent`, or None.

  Returns an event only for *exchange-triggered* protective exits that have
  actually filled (stop-loss / take-profit / liquidation). Worker-initiated
  market closes and non-fill updates are ignored.
  """
  if msg.get("e") != "ORDER_TRADE_UPDATE":
    return None
  o = msg.get("o") or {}
  if o.get("X") != "FILLED":
    return None

  original_type = o.get("ot") or o.get("o")
  if original_type in _STOP_TYPES:
    reason = ExchangeCloseReason.SL
  elif original_type in _TP_TYPES:
    reason = ExchangeCloseReason.TP
  elif original_type in _LIQUIDATION_TYPES:
    reason = ExchangeCloseReason.LIQUIDATION
  else:
    return None

  return ExchangeCloseEvent(
    symbol=o.get("s", ""),
    reason=reason,
    close_price=float(o.get("ap") or o.get("L") or 0) or 0.0,
    close_volume=float(o.get("z") or o.get("q") or 0) or 0.0,
    order_id=int(o.get("i") or 0),
    client_order_id=o.get("c", ""),
    realized_pnl=float(o.get("rp") or 0) or 0.0,
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
      )
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

  async def _stream(self) -> None:
    client = self._build_client()
    listen_key = await asyncio.to_thread(self._listen_key, client)
    if not listen_key:
      raise RuntimeError("Failed to obtain listenKey")

    connection = await client.websocket_streams.create_connection()
    last_keepalive = time.monotonic()
    log.info("[BinanceUserDataStream] Connecting to user data stream.")
    try:
      stream = await connection.user_data(listenKey=listen_key)
      stream.on("message", self._on_message)
      log.info("[BinanceUserDataStream] Subscribed.")

      while not self._stop_event.is_set():
        await asyncio.sleep(1.0)
        if time.monotonic() - last_keepalive >= _KEEPALIVE_INTERVAL:
          try:
            await asyncio.to_thread(client.rest_api.keepalive_user_data_stream)
            last_keepalive = time.monotonic()
            log.debug("[BinanceUserDataStream] listenKey kept alive.")
          except Exception as exc:
            log.warning("[BinanceUserDataStream] keepalive failed: %s", exc)
    finally:
      try:
        await client.websocket_streams.close_connection()
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
