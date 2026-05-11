import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from worker.logger import get_logger
from worker.services.db_service import DBService
from worker.services.mt5_process import MT5ProcessManager
from worker.settings import settings

log = get_logger("worker.app")

_WATCHDOG_INTERVAL = 10  # seconds between liveness checks


async def _watchdog(manager: MT5ProcessManager) -> None:
  """Restart the MT5 child process if it dies unexpectedly."""
  while True:
    await asyncio.sleep(_WATCHDOG_INTERVAL)
    if manager.stopping:
      break
    if not manager.is_alive:
      log.warning("MT5 worker process died unexpectedly — restarting...")
      try:
        await asyncio.to_thread(manager.restart)
        log.info("MT5 worker process restarted successfully.")
      except Exception as exc:
        log.exception("Failed to restart MT5 worker process: %s", exc)


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
  settings_dict = {
    "mt5_server": settings.mt5_server,
    "mt5_login": settings.mt5_login,
    "mt5_password": settings.mt5_password,
    "mt5_path": settings.mt5_path,
    "nats_url": settings.nats_url,
    "nats_token": settings.nats_token,
    "magic_number": settings.magic_number,
    "slippage_deviation": settings.slippage_deviation,
    "broker_api_url": settings.broker_api_url,
    "broker_api_key": settings.broker_api_key,
    "telegram_chat_id": settings.telegram_chat_id,
    "telegram_chat_channel_id": settings.telegram_chat_channel_id,
  }

  manager = MT5ProcessManager(settings_dict)
  app.state.mt5_manager = manager

  # 3. Spawn child process in a thread so lifespan can yield immediately.
  #    Process.start() itself is fast (fork/spawn), but we offload it anyway
  #    to keep the event loop fully responsive from the first millisecond.
  await asyncio.to_thread(manager.start)

  watchdog_task = asyncio.create_task(_watchdog(manager))

  log.info(
    "FastAPI lifespan started. MT5 worker running in subprocess. API is now accepting requests."
  )
  yield

  # 4. Shutdown: stop watchdog then child process.
  watchdog_task.cancel()
  try:
    await watchdog_task
  except asyncio.CancelledError:
    pass

  log.info("Shutting down MT5 worker subprocess...")
  await asyncio.to_thread(manager.stop)
  log.info("MT5 worker subprocess stopped.")


def create_app() -> FastAPI:
  """Build and return the FastAPI application with all routes wired up."""
  app = FastAPI(
    title="MT5 Trading Worker",
    description="Connects to MT5 and executes trades based on NATS signals.",
    version="2.0.0",
    lifespan=lifespan,
  )

  return app
