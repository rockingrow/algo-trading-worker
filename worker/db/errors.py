"""
worker/db/errors.py
───────────────────
Exceptions the persistence layer raises into the pipeline.

Kept free of SQLite types so ``processor.py`` and ``signal_handler.py`` can catch
them without learning how the store is implemented.
"""

from __future__ import annotations


class PositionStoreUnavailableError(RuntimeError):
  """The position store could not answer a query a decision depends on.

  Distinct from an empty list on purpose, the DB-side twin of
  :class:`~worker.gateways.errors.PositionsUnavailableError`: "the query failed"
  read as "nothing is open" let entries through the one-order-per-symbol and
  MAX_OPEN_ORDERS guards, and made the reconcile job see every live position as
  untracked. A caller that cannot read the tracked positions must not act as
  though there are none.
  """
