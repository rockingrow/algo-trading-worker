import sqlite3

from worker.logger import get_logger
from worker.settings import settings

logger = get_logger("worker.db.connection")


def _get_conn() -> sqlite3.Connection:
  conn = sqlite3.connect(settings.db_file, timeout=5)
  conn.execute("PRAGMA busy_timeout=5000")
  return conn
