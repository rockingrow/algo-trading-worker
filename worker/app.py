from contextlib import asynccontextmanager

from fastapi import FastAPI

from worker.logger import get_logger
from worker.market import create_market_orchestrator
from worker.services.db_service import DBService
from worker.settings import settings

log = get_logger("worker.app")


@asynccontextmanager
async def lifespan(app: FastAPI):
  db_service = DBService()
  db_service.initialize()
  log.info("Database initialized.")

  settings_dict = settings.model_dump()
  processor = create_market_orchestrator(settings_dict)
  app.state.market_processor = processor

  await processor.startup()
  log.info("FastAPI lifespan started. API is now accepting requests.")
  yield

  await processor.shutdown()


def create_app() -> FastAPI:
  """Build and return the FastAPI application with all routes wired up."""
  app = FastAPI(
    title="Worker algo trading",
    description="Worker process for algo trading.",
    version="1.0.0",
    lifespan=lifespan,
  )
  return app
