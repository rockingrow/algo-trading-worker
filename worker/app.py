import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from worker.logger import get_logger
from worker.mt5.manager import MT5Manager
from worker.mt5_worker import worker_initialized
from worker.services.db_service import DBService
from worker.settings import WATCHDOG_INTERVAL, settings

log = get_logger("worker.app")


async def _watchdog(manager: MT5Manager) -> None:
  """Restart the MT5 child process if it dies unexpectedly."""
  while True:
    await asyncio.sleep(WATCHDOG_INTERVAL)
    if manager.stopping:
      break
    if not manager.is_alive:
      log.warning("Worker process died unexpectedly — restarting...")
      try:
        await asyncio.to_thread(manager.restart)
        log.info("Worker process restarted successfully.")
      except Exception as exc:
        log.exception("Failed to restart Worker process: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
  # 1. Initialize Database (fast, local SQLite)
  db_service = DBService()
  db_service.initialize()
  log.info("Database initialized.")

  # 2. Build the settings dict passed to the child process.
  #    The child process imports MetaTrader5 — the parent (this process) never does.
  #    This is the key fix: the MT5 C extension's GIL-holding calls only happen
  #    in the child process, so the FastAPI event loop is never frozen.
  settings_dict = settings.model_dump()

  manager = MT5Manager(settings_dict, worker_initialized, process_name="worker_mt5")
  app.state.mt5_manager = manager

  # 3. Spawn child process in a thread so lifespan can yield immediately.
  #    Process.start() itself is fast (fork/spawn), but we offload it anyway
  #    to keep the event loop fully responsive from the first millisecond.
  await asyncio.to_thread(manager.start)

  watchdog_task = asyncio.create_task(_watchdog(manager))

  log.info(
    "FastAPI lifespan started. Worker running in subprocess. API is now accepting requests."
  )
  yield

  # 4. Shutdown: stop watchdog then child process.
  watchdog_task.cancel()
  try:
    await watchdog_task
  except asyncio.CancelledError:
    pass

  log.info("Shutting down Worker subprocess...")
  await asyncio.to_thread(manager.stop)
  log.info("Worker subprocess stopped.")


def create_app() -> FastAPI:
  """Build and return the FastAPI application with all routes wired up."""
  app = FastAPI(
    title="Worker algo trading",
    description="Worker process for algo trading.",
    version="1.0.0",
    lifespan=lifespan,
  )

  return app
