"""
worker/crypto/binance/user_data_stream.py
──────────────────────────────────────────
Event ingestion from Binance — the CRYPTO counterpart of ``MT5EventJob``.

Binance pushes account/order events over the **User Data Stream** (a websocket),
which is the optimal, low-latency way to learn about fills and exchange-triggered
exits (stop-loss / take-profit / liquidation) — far better than polling the REST
API. This module:

  1. Obtains a ``listenKey`` and connects to the futures user stream.
  2. Parses ``ORDER_TRADE_UPDATE`` frames into a broker-neutral
     :class:`ExchangeCloseEvent` and hands each to a caller-supplied handler.
  3. Keeps the ``listenKey`` alive (Binance expires it after 60 min).

The frame-parsing logic is a pure function (:func:`parse_order_trade_update`),
so it is fully unit-tested without any network. The websocket plumbing is kept
thin around it.
"""

from __future__ import annotations

import asyncio
import threading
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
  """Daemon that streams Binance user events and dispatches close events.

  *gateway* must expose ``create_listen_key()``, ``keepalive_listen_key()`` and
  ``ws_base_url`` (Binance gateway does). *handler* is called with each
  :class:`ExchangeCloseEvent`.
  """

  def __init__(
    self,
    gateway: Any,
    handler: Callable[[ExchangeCloseEvent], None],
  ) -> None:
    self._gateway = gateway
    self._handler = handler
    self._stop_event = threading.Event()
    self._thread: Optional[threading.Thread] = None
    self._keepalive_thread: Optional[threading.Thread] = None

  def start(self, stop_event=None) -> None:
    if stop_event is not None:
      self._stop_event = stop_event
    self._thread = threading.Thread(
      target=self._run, name="binance-user-stream", daemon=True
    )
    self._thread.start()
    self._keepalive_thread = threading.Thread(
      target=self._keepalive_loop, name="binance-listenkey-keepalive", daemon=True
    )
    self._keepalive_thread.start()
    log.info("[BinanceUserDataStream] Started.")

  def stop(self) -> None:
    self._stop_event.set()

  # ── internals ─────────────────────────────────────────────────────────── #

  def _keepalive_loop(self) -> None:
    while not self._stop_event.is_set():
      self._stop_event.wait(_KEEPALIVE_INTERVAL)
      if self._stop_event.is_set():
        break
      try:
        self._gateway.keepalive_listen_key()
        log.debug("[BinanceUserDataStream] listenKey kept alive.")
      except Exception as exc:
        log.warning("[BinanceUserDataStream] keepalive failed: %s", exc)

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        asyncio.run(self._stream())
      except Exception as exc:
        log.exception("[BinanceUserDataStream] stream error: %s", exc)
      if not self._stop_event.is_set():
        self._stop_event.wait(5)  # backoff before reconnecting

  async def _stream(self) -> None:
    import json

    import websockets  # lazy: keep the dep out of import time

    listen_key = self._gateway.create_listen_key()
    url = f"{self._gateway.ws_base_url}/{listen_key}"
    log.info("[BinanceUserDataStream] Connecting to user data stream.")

    async with websockets.connect(url, ping_interval=180) as ws:
      while not self._stop_event.is_set():
        try:
          raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
        except asyncio.TimeoutError:
          continue
        try:
          msg = json.loads(raw)
        except (ValueError, TypeError):
          continue
        event = parse_order_trade_update(msg)
        if event is not None:
          self._dispatch(event)

  def _dispatch(self, event: ExchangeCloseEvent) -> None:
    try:
      self._handler(event)
    except Exception as exc:
      log.exception("[BinanceUserDataStream] handler failed: %s", exc)
