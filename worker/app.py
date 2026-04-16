import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from worker.db import init_db
from worker.logger import get_logger
from worker.mt5.mt5 import MT5
from worker.mt5.executor import MT5Executor
from worker.router import get_core_router
from worker.services.worker_service import run_worker_loop
from worker.services.zmq_service import ZMQ
from worker.settings import settings

log = get_logger("worker.app")


@asynccontextmanager
async def lifespan(app: FastAPI):
  # 1. Initialize Database
  init_db()

  # 2. Initialize MT5
  bridge = MT5(
    server=settings.mt5_server,
    login=settings.mt5_login,
    password=settings.mt5_password,
    path=settings.mt5_path,
  )
  if not bridge.connect():
    log.error("Could not connect to MT5. Worker logic will not run.")
    yield
    return

  # 3. Initialize Event Subscriber
  subscriber = ZMQ(host=settings.zmq_sub_host)
  subscriber.connect()

  # 4. Initialize Trading Logic
  executor = MT5Executor(
    magic_number=settings.magic_number, slippage_deviation=settings.slippage_deviation
  )

  # Start the background task
  worker_task = asyncio.create_task(run_worker_loop(subscriber, executor, bridge))

  yield

  # Shutdown logic
  worker_task.cancel()
  try:
    await worker_task
  except asyncio.CancelledError:
    pass

  bridge.shutdown()
  log.info("MT5 Bridge shut down.")


def create_app() -> FastAPI:
  """Build and return the FastAPI application with all routes wired up."""
  app = FastAPI(
    title="MT5 Trading Worker",
    description="Connects to MT5 and executes trades based on ZMQ signals.",
    version="2.0.0",
    lifespan=lifespan,
  )

  # Include Core Router
  app.include_router(get_core_router())

  return app
