"""
worker/interfaces/trade_presenter_protocol.py
─────────────────────────────────────────────
Contract for the per-market Telegram message presenters.

Both :class:`~worker.gateways.forex.message_presenter.ForexMessagePresenter` and
:class:`~worker.gateways.crypto.message_presenter.CryptoMessagePresenter` conform
to this structurally, so
:class:`~worker.gateways.processor.BaseSignalProcessor` renders
lifecycle/trade messages through this interface instead of a concrete presenter
(Dependency Inversion).
"""

from __future__ import annotations

from typing import Any, Protocol

from worker.schemas.signal_schema import SignalSchema


class TradePresenterProtocol(Protocol):
  @staticmethod
  def startup(settings_dict: dict, footer: str) -> str: ...
  @staticmethod
  def shutdown(footer: str) -> str: ...
  @staticmethod
  def force_closed(symbol: str, strategy: str, fc: dict, footer: str) -> str: ...
  @staticmethod
  def order_filled(
    signal: SignalSchema, result: dict, pos_ticket: Any, footer: str
  ) -> str: ...
  @staticmethod
  def order_failed(signal: SignalSchema, result: dict, footer: str) -> str: ...
  @staticmethod
  def position_unprotected_closed(
    signal: SignalSchema, result: dict, footer: str
  ) -> str: ...
  @staticmethod
  def signal_rejected(reason: str, footer: str) -> str: ...
  @staticmethod
  def admin_flat_closed(db_pos: dict, result: dict, footer: str) -> str: ...
