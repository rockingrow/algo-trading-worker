"""
worker/crypto/factory.py
────────────────────────
Exchange factory — turns the configured ``CryptoExchangeEnum`` into a concrete
:class:`~worker.gateways.crypto.base.BaseExchangeGateway`.

Adding a new CEX means adding a builder here and a gateway module; no other code
changes. This is the seam that keeps the worker from depending on any specific
exchange.
"""

from __future__ import annotations

from typing import Callable, Dict

from worker.gateways.crypto.base import BaseExchangeGateway
from worker.logger import get_logger
from worker.settings import CryptoExchangeEnum

logger = get_logger("worker.gateways.crypto.factory")


def _build_binance(settings_dict: dict) -> BaseExchangeGateway:
  # Imported lazily so non-Binance deployments never import its module.
  from worker.gateways.crypto.binance.gateway import BinanceFuturesGateway

  return BinanceFuturesGateway(
    api_key=settings_dict["crypto_api_key"].get_secret_value(),
    api_secret=settings_dict["crypto_api_secret"].get_secret_value(),
    testnet=bool(settings_dict.get("crypto_testnet", False)),
    hedge_mode=bool(settings_dict.get("crypto_hedge_mode", False)),
  )


_BUILDERS: Dict[CryptoExchangeEnum, Callable[[dict], BaseExchangeGateway]] = {
  CryptoExchangeEnum.BINANCE: _build_binance,
}


class ExchangeFactory:
  """Creates the configured exchange gateway."""

  @staticmethod
  def create(settings_dict: dict) -> BaseExchangeGateway:
    raw = settings_dict.get("crypto_exchange", CryptoExchangeEnum.BINANCE.value)
    exchange = raw if isinstance(raw, CryptoExchangeEnum) else CryptoExchangeEnum(raw)

    builder = _BUILDERS.get(exchange)
    if builder is None:
      raise ValueError(f"Unsupported or not-yet-implemented exchange: {exchange!r}")

    logger.info("[ExchangeFactory] Building gateway for exchange=%s", exchange.value)
    return builder(settings_dict)
