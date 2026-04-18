import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from worker.logger import get_logger
from worker.services.db_service import DBService
from worker.services.mt5_process import MT5ProcessManager
from worker.settings import settings

log = get_logger("worker.app")


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
    "zmq_sub_host": settings.zmq_sub_host,
    "zmq_curve_server_public_key": settings.zmq_curve_server_public_key,
    "zmq_curve_client_public_key": settings.zmq_curve_client_public_key,
    "zmq_curve_client_secret_key": settings.zmq_curve_client_secret_key,
    "magic_number": settings.magic_number,
    "slippage_deviation": settings.slippage_deviation,
  }

  manager = MT5ProcessManager(settings_dict)
  app.state.mt5_manager = manager

  # 3. Spawn child process in a thread so lifespan can yield immediately.
  #    Process.start() itself is fast (fork/spawn), but we offload it anyway
  #    to keep the event loop fully responsive from the first millisecond.
  await asyncio.to_thread(manager.start)

  log.info(
    "FastAPI lifespan started. MT5 worker running in subprocess. API is now accepting requests."
  )
  yield

  # 4. Shutdown: stop the child process.
  log.info("Shutting down MT5 worker subprocess...")
  await asyncio.to_thread(manager.stop)
  log.info("MT5 worker subprocess stopped.")


def create_app() -> FastAPI:
  """Build and return the FastAPI application with all routes wired up."""
  app = FastAPI(
    title="MT5 Trading Worker",
    description="Connects to MT5 and executes trades based on ZMQ signals.",
    version="2.0.0",
    lifespan=lifespan,
  )

  return app
