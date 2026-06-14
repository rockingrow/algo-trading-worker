"""
worker/gateways/forex/factory.py
────────────────────────────────
Platform factory — turns the configured ``ForexPlatformEnum`` into a concrete
:class:`~worker.gateways.forex.base.BasePlatformGateway`.

Adding a new platform (e.g. MT6) means adding a builder here and a gateway module;
no other code changes. This is the seam that keeps the worker from depending on
any specific forex platform — the mirror of
:class:`~worker.gateways.crypto.factory.ExchangeFactory`.
"""

from __future__ import annotations

from typing import Callable, Dict

from worker.gateways.forex.base import BasePlatformGateway
from worker.logger import get_logger
from worker.settings import ForexPlatformEnum

logger = get_logger("worker.gateways.forex.factory")


def _build_mt5(settings_dict: dict) -> BasePlatformGateway:
  # Imported lazily so non-MT5 (and non-FOREX) deployments never import MetaTrader5.
  from worker.gateways.forex.mt5.gateway import MT5Gateway

  return MT5Gateway(
    server=settings_dict["mt5_server"],
    login=settings_dict["mt5_login"],
    password=settings_dict["mt5_password"],
    path=settings_dict.get("mt5_path"),
    slippage_deviation=settings_dict["slippage_deviation"],
  )


_BUILDERS: Dict[ForexPlatformEnum, Callable[[dict], BasePlatformGateway]] = {
  ForexPlatformEnum.MT5: _build_mt5,
}


class PlatformFactory:
  """Creates the configured forex platform gateway."""

  @staticmethod
  def create(settings_dict: dict) -> BasePlatformGateway:
    raw = settings_dict.get("forex_platform", ForexPlatformEnum.MT5)
    platform = raw if isinstance(raw, ForexPlatformEnum) else ForexPlatformEnum(raw)

    builder = _BUILDERS.get(platform)
    if builder is None:
      raise ValueError(f"Unsupported or not-yet-implemented platform: {platform!r}")

    logger.info("[PlatformFactory] Building gateway for platform=%s", platform.value)
    return builder(settings_dict)
