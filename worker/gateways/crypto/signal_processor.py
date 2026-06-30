"""
worker/gateways/crypto/signal_processor.py
──────────────────────────────────────────
CRYPTO/CEX-specific signal processor.

Implements the broker-specific hooks of
:class:`~worker.gateways.processor.BaseSignalProcessor`: the exchange
gateway (built by :class:`~worker.gateways.crypto.factory.ExchangeFactory`), the
crypto executor, and the exchange user-data event stream. Everything
market-agnostic — the NATS loop, signal persistence, notifications, position CDC
— is inherited from the base.

This module imports **no** MetaTrader5 / ``worker.gateways.forex.mt5.*`` code, so the
CRYPTO path never initializes any Forex dependency.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from worker.gateways.crypto.binance.user_data_stream import (
  ExchangeCloseEvent,
  ExchangeCloseReason,
)
from worker.gateways.crypto.executor import CryptoExecutor
from worker.gateways.crypto.factory import ExchangeFactory
from worker.gateways.crypto.leverage_init_job import LeverageInitJob
from worker.gateways.crypto.message_presenter import CryptoMessagePresenter
from worker.gateways.processor import BaseSignalProcessor
from worker.logger import get_logger
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.system_schema import (
  SystemActionEnum,
  SystemCryptoLeverageInitSchema,
)

log = get_logger("worker.gateways.crypto.signal_processor")

# Exchange-triggered close reason → DB status.
_EXCHANGE_CLOSE_STATUS = {
  ExchangeCloseReason.SL: PositionStatusEnum.SL,
  ExchangeCloseReason.TP: PositionStatusEnum.TP2,
  ExchangeCloseReason.LIQUIDATION: PositionStatusEnum.FORCED_CLOSED,
  ExchangeCloseReason.MANUAL: PositionStatusEnum.TERMINAL_CLOSED,
}


class CryptoSignalProcessor(BaseSignalProcessor):
  """Exchange gateway + crypto executor + user-data stream over the shared skeleton."""

  name = "CRYPTO"
  presenter = CryptoMessagePresenter

  # ── Broker hooks ──────────────────────────────────────────────────────── #

  def _build_executor(self) -> CryptoExecutor:
    self.gateway = ExchangeFactory.create(self.settings)
    return CryptoExecutor(
      gateway=self.gateway,
      config=self.config,
      db=self.ctx.db_service,
      quote_asset=self.settings.get("crypto_quote_asset", "USDT"),
    )

  def _connect_broker(self) -> bool:
    if not self.gateway.connect():
      return False
    # Initialise per-symbol leverage once, here — after the gateway is verified
    # but before any signal can be processed — so the first order on a sticky
    # exchange-side leverage value never picks up a stale setting from a prior
    # session. The same pass can later be re-triggered at runtime via a SYSTEM
    # CRYPTO_LEVERAGE_INIT message (see _handle_system_action).
    self._run_leverage_init()
    return True

  def _run_leverage_init(
    self,
    symbols: Optional[list[str]] = None,
    max_leverage_cap: Optional[int] = None,
  ) -> None:
    """Run :class:`LeverageInitJob` once, shared by startup and the SYSTEM
    ``CRYPTO_LEVERAGE_INIT`` action.

    ``symbols`` / ``max_leverage_cap`` default to the configured values
    (``CRYPTO_LEVERAGE_INIT_SYMBOLS`` / ``MAX_LEVERAGE_CAP``); a SYSTEM message
    may override either to re-initialise an ad-hoc set of symbols or cap. Failures
    inside the job are logged per symbol and never propagate — a missed leverage
    init is recoverable on the next restart or re-trigger, but aborting over it
    would leave the worker down (startup) or drop the message (SYSTEM).
    """
    # When use_custom_leverage is on, the configured MAX_LEVERAGE_CAP always
    # wins: any per-call override (e.g. a SYSTEM message's default_leverage) is
    # ignored so the worker forces its own cap instead of the broker/ad-hoc one.
    if self.settings.get("use_custom_leverage", False):
      max_leverage_cap = self.settings.get("max_leverage_cap", 10)
    try:
      LeverageInitJob(
        gateway=self.gateway,
        symbols=symbols if symbols is not None else (self.settings.get("crypto_leverage_init_symbols") or []),
        max_leverage_cap=max_leverage_cap if max_leverage_cap is not None else self.settings.get("max_leverage_cap", 10),
        resolve_symbol=self.executor.get_symbol,
      ).run()
    except Exception:
      log.exception(
        "[CRYPTO Process] LeverageInitJob crashed — continuing; symbol leverages "
        "remain at their current exchange settings."
      )

  # ── SYSTEM actions (crypto handles CRYPTO_LEVERAGE_INIT) ──────────────── #

  def _handle_system_action(self, action: SystemActionEnum, data: dict) -> None:
    if action == SystemActionEnum.CRYPTO_LEVERAGE_INIT:
      msg = SystemCryptoLeverageInitSchema(**data)
      log.info(
        "[CRYPTO SYSTEM] CRYPTO_LEVERAGE_INIT | symbols=%s default_leverage=%s",
        msg.symbols, msg.default_leverage,
      )
      # Either field omitted → fall back to the configured default for that field
      # (CRYPTO_LEVERAGE_INIT_SYMBOLS / MAX_LEVERAGE_CAP). default_leverage acts as
      # the cap: each symbol is set to min(exchange_max, default_leverage).
      self._run_leverage_init(
        symbols=msg.symbols,
        max_leverage_cap=msg.default_leverage,
      )
      return
    super()._handle_system_action(action, data)

  def _disconnect_broker(self) -> None:
    self.gateway.close()

  def _account_footer(self) -> str:
    return self.gateway.get_account_footer(account_id=self._account_id)

  @property
  def _account_id(self) -> str:
    return self.settings.get("binance_account_id")

  def _magic_for(self, strategy: str) -> Optional[int]:
    return None  # crypto exchanges have no magic-number equivalent

  def _position_cdc_kwargs(self) -> Dict[str, Any]:
    return {
      "account_info_fn": self._account_snapshot,
      "account_name": self.settings.get("binance_account_id"),
      "strategy_magic_map": self.settings.get("strategy_magic_map") or {},
    }

  def _account_snapshot(self) -> Optional[dict]:
    info = self.gateway.get_account()
    if not info:
      return None
    return {"balance": info.get("balance"), "leverage": None}

  def _start_broker_jobs(self, stop_event) -> None:
    # Exchange-side event ingestion (Binance → websocket user data stream).
    event_stream = self.gateway.create_event_stream(self._on_exchange_close)
    if event_stream is not None:
      event_stream.start(stop_event=stop_event)
    else:
      log.info("[CRYPTO Process] Exchange has no push event stream; skipping.")

    # Durability backstop: the push stream above can miss a close (reconnect gap,
    # handler error, downtime). This periodic reconciler marks any DB-open
    # position that no longer exists on the exchange as closed, so a missed event
    # self-heals instead of leaving the row stuck OPEN forever.
    from worker.gateways.crypto.reconcile_job import CryptoReconcileJob

    CryptoReconcileJob(
      db_service=self.ctx.db_service,
      executor=self.executor,
      handler=self._on_missed_close,
    ).start(stop_event=stop_event)

  # ── Exchange-triggered close handler (from the user data stream) ──────── #

  def _on_exchange_close(self, event: ExchangeCloseEvent) -> None:
    status = _EXCHANGE_CLOSE_STATUS.get(event.reason, PositionStatusEnum.TERMINAL_CLOSED)
    log.info(
      "[Crypto Event] Exchange close | symbol=%s reason=%s price=%s",
      event.symbol, event.reason.value, event.close_price,
    )

    # Match by resolved exchange symbol: the DB stores the original signal symbol
    # (e.g. BTCUSD) while the event carries the exchange symbol (e.g. BTCUSDT).
    open_rows = self.ctx.db_service.get_open_positions_for_flat()
    matched = [
      row for row in open_rows
      if self.executor.get_symbol(row["symbol"]) == event.symbol
    ]
    if not matched:
      log.warning("[Crypto Event] No open DB position for %s — ignoring.", event.symbol)
      return

    if len(matched) > 1:
      # Multiple strategies held the same symbol simultaneously in netting mode.
      # One exchange fill closes the shared net position; all matched rows are
      # marked closed with the same total fill volume.
      log.warning(
        "[Crypto Event] %d DB rows matched %s — netting mode: one exchange fill "
        "closes all. Check CRYPTO_ALLOW_MULTI_STRATEGY_PER_SYMBOL config.",
        len(matched), event.symbol,
      )

    close_comment = (
      f"Exchange close [{event.reason.value}] (shared fill — netting mode)"
      if len(matched) > 1
      else f"Exchange close [{event.reason.value}]"
    )
    for row in matched:
      self.ctx.db_service.log_position(
        strategy=row["strategy"],
        ref_id=event.order_id,
        ref_source_id=row["ref_source_id"],
        symbol=row["symbol"],
        action=event.reason.value,
        volume=event.close_volume,  # total exchange fill volume
        price=event.close_price,
        sl=None,
        tp1=None,
        gateway_return_code=0,
        comment=close_comment,
        author=LogAuthorEnum.EXCHANGE.value,
        market_type=self._market_type,
      )
      self.ctx.db_service.update_position_status(
        ref_source_id=row["ref_source_id"],
        status=status,
        ref_id=event.order_id,
        closed_price=event.close_price,
        gateway_return_code=0,
        comment=close_comment,
      )
    self.ctx.channel_notifier.send_message(
      CryptoMessagePresenter.exchange_close(event, self.gateway.get_account_footer())
    )

  # ── Reconciler backstop (from CryptoReconcileJob) ─────────────────────── #

  def _on_missed_close(self, row: Dict[str, Any]) -> None:
    """Mark a DB-open position closed after the reconciler confirmed it no longer
    exists on the exchange (a fill event the user-data stream never delivered).

    The exact reason/price is unknown — the live event is what carries those — so
    the close is recorded as ``TERMINAL_CLOSED`` (the same status used for a
    CEX-side MANUAL close) with a best-effort mark price and a comment flagging it
    as reconciled. The CDC job then propagates the status downstream as usual.
    """
    resolved = self.executor.get_symbol(row["symbol"])
    close_price = self._best_effort_price(resolved)
    comment = "Reconciled: closed on exchange (fill event missed)"

    self.ctx.db_service.log_position(
      strategy=row.get("strategy"),
      ref_id=None,
      ref_source_id=row.get("ref_source_id"),
      symbol=row["symbol"],
      action=PositionStatusEnum.TERMINAL_CLOSED.value,
      volume=row.get("volume"),
      price=close_price,
      sl=None,
      tp1=None,
      gateway_return_code=0,
      comment=comment,
      author=LogAuthorEnum.EXCHANGE.value,
      market_type=self._market_type,
    )
    self.ctx.db_service.update_position_status(
      ref_source_id=row.get("ref_source_id"),
      status=PositionStatusEnum.TERMINAL_CLOSED,
      closed_price=close_price,
      gateway_return_code=0,
      comment=comment,
    )
    self.ctx.channel_notifier.send_message(
      CryptoMessagePresenter.position_reconciled_closed(
        row, close_price, self.gateway.get_account_footer()
      )
    )

  def _best_effort_price(self, resolved_symbol: str) -> Optional[float]:
    """Mark price as an approximate close price; ``None`` if it can't be read."""
    try:
      return self.gateway.get_mark_price(resolved_symbol)
    except Exception as exc:  # pragma: no cover - best effort
      log.warning("[Crypto Reconcile] mark price unavailable for %s: %s", resolved_symbol, exc)
      return None

  # ── ADMIN FLAT match keys (crypto reconciles by resolved symbol) ──────── #

  def _flat_match_key(self, pos: Any) -> Any:
    # ExchangePosition.symbol is already the resolved exchange symbol.
    return pos.symbol

  def _flat_db_match_keys(self, db_pos: dict) -> set:
    # The DB stores the original signal symbol; resolve it to the exchange form.
    return {self.executor.get_symbol(db_pos["symbol"])}
