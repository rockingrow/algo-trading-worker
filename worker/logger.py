"""
worker/logger.py — Structured coloured logger.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from worker.settings import settings

LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

def get_logger(name: str) -> logging.Logger:
  logger = logging.getLogger(name)

  if not logger.handlers:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logger.setLevel(level)

    import datetime

    class DailyFileHandler(logging.FileHandler):
        def __init__(self, directory: str | Path, mode: str = 'a', encoding: str = 'utf-8', delay: bool = False):
            self.directory = Path(directory)
            self.current_date = datetime.datetime.now().strftime('%Y%m%d')
            filename = self.directory / f"{self.current_date}.log"
            super().__init__(filename, mode, encoding, delay)

        def emit(self, record):
            new_date = datetime.datetime.now().strftime('%Y%m%d')
            if new_date != self.current_date:
                self.close()
                self.current_date = new_date
                self.baseFilename = str(self.directory / f"{self.current_date}.log").replace('\\', '/')
                self.stream = self._open()
            super().emit(record)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    file_handler = DailyFileHandler(LOGS_DIR, mode="a", encoding="utf-8")
    file_handler.setLevel(level)

    fmt = logging.Formatter(
      fmt="%(asctime)s | %(levelname)-8s | %(process)d | %(name)s | %(message)s",
      datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(fmt)
    file_handler.setFormatter(fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Prevent records from bubbling up to uvicorn's root handler
    logger.propagate = False

  return logger
