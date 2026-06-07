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


class GatewayProcessOrchestrator(MarketOrchestrator):
  """Supervises a market's worker child process.

  Both markets spawn a child process (so the GIL-holding MT5 extension or a
  long-lived websocket loop never blocks the FastAPI event loop) and supervise it
  with the same watchdog/restart loop — only the worker function, label, and
  process name differ. Parameterizing those collapses what used to be two nearly
  identical orchestrator classes into one (composition over duplication).
  """

  def __init__(
    self, settings_dict: dict, worker_fn, *, label: str, process_name: str
  ) -> None:
    from worker.process_manager import WorkerProcessManager

    self._manager = WorkerProcessManager(
      settings_dict, worker_fn, process_name=process_name
    )
    self._label = label
    self._watchdog_task: asyncio.Task | None = None

  async def startup(self) -> None:
    await asyncio.to_thread(self._manager.start)
    self._watchdog_task = asyncio.create_task(self._watchdog())
    log.info("[%s] Worker process started.", self._label)

  async def shutdown(self) -> None:
    if self._watchdog_task:
      self._watchdog_task.cancel()
      try:
        await self._watchdog_task
      except asyncio.CancelledError:
        pass
    log.info("[%s] Shutting down worker subprocess...", self._label)
    await asyncio.to_thread(self._manager.stop)
    log.info("[%s] Worker subprocess stopped.", self._label)

  async def _watchdog(self) -> None:
    while True:
      await asyncio.sleep(WATCHDOG_INTERVAL)
      if self._manager.stopping:
        break
      if not self._manager.is_alive:
        log.warning("[%s] Worker process died unexpectedly — restarting...", self._label)
        try:
          await asyncio.to_thread(self._manager.restart)
          log.info("[%s] Worker process restarted successfully.", self._label)
        except Exception as exc:
          log.exception("[%s] Failed to restart worker process: %s", self._label, exc)


def create_market_orchestrator(settings_dict: dict) -> MarketOrchestrator:
  """Build the orchestrator for the configured market.

  The market's worker function is imported lazily here so the other market's
  dependencies are never loaded (the CRYPTO path never imports MetaTrader5, and
  vice-versa).
  """
  market_type = settings_dict.get("market_type", MarketTypeEnum.FOREX)

  if market_type == MarketTypeEnum.FOREX:
    from worker.mt5_worker import mt5_worker_main

    return GatewayProcessOrchestrator(
      settings_dict, mt5_worker_main, label="FOREX", process_name="worker_mt5"
    )

  if market_type == MarketTypeEnum.CRYPTO:
    from worker.crypto_worker import crypto_worker_main

    return GatewayProcessOrchestrator(
      settings_dict, crypto_worker_main, label="CRYPTO", process_name="worker_crypto"
    )

  raise ValueError(f"Unsupported MARKET_TYPE: {market_type!r}")
