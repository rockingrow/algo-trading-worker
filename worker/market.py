import asyncio
from abc import ABC, abstractmethod

from worker.logger import get_logger
from worker.settings import WATCHDOG_INTERVAL, MarketTypeEnum

log = get_logger("worker.market")


class MarketOrchestrator(ABC):
  """Abstract base class for market orchestrators."""
  @abstractmethod
  async def startup(self) -> None: ...

  @abstractmethod
  async def shutdown(self) -> None: ...


class ForexMarketOrchestrator(MarketOrchestrator):
  """Concrete implementation for Forex market orchestrator."""
  def __init__(self, settings_dict: dict) -> None:
    # Imported lazily so the CRYPTO path never loads MetaTrader5 / mt5 modules.
    from worker.mt5.manager import MT5Manager
    from worker.mt5_worker import mt5_worker_main

    self._manager = MT5Manager(settings_dict, mt5_worker_main, process_name="worker_mt5")
    self._watchdog_task: asyncio.Task | None = None

  async def startup(self) -> None:
    await asyncio.to_thread(self._manager.start)
    self._watchdog_task = asyncio.create_task(self._watchdog())
    log.info("[FOREX] MT5 worker process started.")

  async def shutdown(self) -> None:
    if self._watchdog_task:
      self._watchdog_task.cancel()
      try:
        await self._watchdog_task
      except asyncio.CancelledError:
        pass
    log.info("Shutting down MT5 worker subprocess...")
    await asyncio.to_thread(self._manager.stop)
    log.info("MT5 worker subprocess stopped.")

  async def _watchdog(self) -> None:
    while True:
      await asyncio.sleep(WATCHDOG_INTERVAL)
      if self._manager.stopping:
        break
      if not self._manager.is_alive:
        log.warning("[FOREX] Worker process died unexpectedly — restarting...")
        try:
          await asyncio.to_thread(self._manager.restart)
          log.info("[FOREX] Worker process restarted successfully.")
        except Exception as exc:
          log.exception("[FOREX] Failed to restart worker process: %s", exc)


class CryptoMarketOrchestrator(MarketOrchestrator):
  """Concrete implementation for Crypto market orchestrator.

  Spawns the crypto child process via the generic process manager and supervises
  it with the same watchdog/restart loop the Forex orchestrator uses. No MT5 /
  MetaTrader5 dependency is imported on this path.
  """

  def __init__(self, settings_dict: dict) -> None:
    # Imported lazily so the FOREX path never loads exchange/websocket modules.
    from worker.crypto_worker import crypto_worker_main
    from worker.process_manager import WorkerProcessManager

    self._manager = WorkerProcessManager(
      settings_dict, crypto_worker_main, process_name="worker_crypto"
    )
    self._watchdog_task: asyncio.Task | None = None

  async def startup(self) -> None:
    await asyncio.to_thread(self._manager.start)
    self._watchdog_task = asyncio.create_task(self._watchdog())
    log.info("[CRYPTO] Crypto worker process started.")

  async def shutdown(self) -> None:
    if self._watchdog_task:
      self._watchdog_task.cancel()
      try:
        await self._watchdog_task
      except asyncio.CancelledError:
        pass
    log.info("Shutting down crypto worker subprocess...")
    await asyncio.to_thread(self._manager.stop)
    log.info("Crypto worker subprocess stopped.")

  async def _watchdog(self) -> None:
    while True:
      await asyncio.sleep(WATCHDOG_INTERVAL)
      if self._manager.stopping:
        break
      if not self._manager.is_alive:
        log.warning("[CRYPTO] Worker process died unexpectedly — restarting...")
        try:
          await asyncio.to_thread(self._manager.restart)
          log.info("[CRYPTO] Worker process restarted successfully.")
        except Exception as exc:
          log.exception("[CRYPTO] Failed to restart worker process: %s", exc)


def create_market_orchestrator(settings_dict: dict) -> MarketOrchestrator:
  market_type = settings_dict.get("market_type", MarketTypeEnum.FOREX)
  if market_type == MarketTypeEnum.FOREX:
    return ForexMarketOrchestrator(settings_dict)
  if market_type == MarketTypeEnum.CRYPTO:
    return CryptoMarketOrchestrator(settings_dict)
  raise ValueError(f"Unsupported MARKET_TYPE: {market_type!r}")
