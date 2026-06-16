"""
worker/main.py — Entry point for the MT5 Trading Worker process.
"""

from __future__ import annotations

import multiprocessing

import uvicorn

from worker.app import create_app
from worker.logger import get_logger
from worker.settings import MarketTypeEnum, settings

log = get_logger("worker")


app = create_app()


def main() -> None:
  # Required on Windows: use 'spawn' (the default) and protect entry point.
  multiprocessing.freeze_support()

  log.info("Starting Algo Trading Worker v1.0 (market=%s)", settings.market_type.value)
  log.info("API Server -> http://%s:%d", settings.app_host, settings.app_port)
  if settings.market_type == MarketTypeEnum.FOREX:
    log.info("MT5 Server -> %s (login: %s)", settings.mt5_server, settings.mt5_login)
  else:
    log.info("Crypto Exchange -> %s", settings.crypto_exchange.value)

  import copy

  from uvicorn.config import LOGGING_CONFIG

  log_config = copy.deepcopy(LOGGING_CONFIG)

  # Ensure the logs directory exists just in case
  import os

  os.makedirs("logs", exist_ok=True)

  log_config["handlers"]["file_default"] = {
    "class": "worker.logger.DailyFileHandler",
    "directory": "logs",
    "mode": "a",
    "encoding": "utf-8",
    "formatter": "default",
  }
  log_config["handlers"]["file_access"] = {
    "class": "worker.logger.DailyFileHandler",
    "directory": "logs",
    "mode": "a",
    "encoding": "utf-8",
    "formatter": "access",
  }

  log_config["loggers"]["uvicorn"]["handlers"].append("file_default")
  log_config["loggers"]["uvicorn.access"]["handlers"].append("file_access")

  uvicorn.run(
    app,
    host=settings.app_host,
    port=settings.app_port,
    log_level=settings.log_level.lower(),
    log_config=log_config,
  )


if __name__ == "__main__":
  main()
