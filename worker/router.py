from fastapi import APIRouter

from worker.apis.api import get_router as get_api_router


def get_core_router() -> APIRouter:
  """Aggregates all system routers into one core router."""
  router = APIRouter()

  # Include all sub-routers
  router.include_router(get_api_router())

  return router
