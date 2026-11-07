"""
worker/db/connection.py
───────────────────────
Opening SQLite connections for the worker's database file.

One short-lived connection per operation: the signal path, the CDC job and the
notification jobs each write from their own thread, and a connection is not
shared across threads. WAL mode (set once by :func:`worker.db.schema.db_init`)
lets those writers and readers overlap; ``busy_timeout`` absorbs the brief
lock contention that remains.

The file comes from the caller when it has one, else from
``settings.database.file`` read *at connect time* — not at import — so a test or
a child process that points the setting elsewhere is honoured.
"""

import sqlite3
from contextlib import contextmanager
from typing import Iterator, Optional

from worker.logger import get_logger
from worker.settings import settings

logger = get_logger("worker.db.connection")


def open_connection(db_file: Optional[str] = None) -> sqlite3.Connection:
  conn = sqlite3.connect(db_file or settings.database.file, timeout=5)
  conn.execute("PRAGMA busy_timeout=5000")
  return conn


@contextmanager
def connect(db_file: Optional[str] = None) -> Iterator[sqlite3.Connection]:
  """A connection that is always closed, whatever the block raises.

  Commit stays explicit in the block: ``sqlite3.Connection``'s own context
  manager commits or rolls back but never closes, which is the half every
  repository method used to spell out by hand.
  """
  conn = open_connection(db_file)
  try:
    yield conn
  finally:
    conn.close()


# Kept for the schema bootstrap and existing imports.
_get_conn = open_connection
