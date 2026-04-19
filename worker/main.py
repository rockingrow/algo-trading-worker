"""
worker/main.py — Entry point for the MT5 Trading Worker process.
"""

from __future__ import annotations

import multiprocessing

import uvicorn

from worker.app import create_app
from worker.logger import get_logger
from worker.settings import settings

log = get_logger("worker")


app = create_app()


def main() -> None:
  # Required on Windows: use 'spawn' (the default) and protect entry point.
  multiprocessing.freeze_support()

  log.info("Starting Algo Trading MT5 Worker v1.0")
  log.info("API Server -> http://%s:%d", settings.app_host, settings.app_port)
  log.info("ZMQ SUB    -> %s (listening for signals)", settings.zmq_sub_host)
  log.info("MT5 Server -> %s (login: %d)", settings.mt5_server, settings.mt5_login)

  uvicorn.run(
    app,
    host=settings.app_host,
    port=settings.app_port,
    log_level=settings.log_level.lower(),
  )


if __name__ == "__main__":
  main()
