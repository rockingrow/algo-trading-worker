"""
worker/mt5/manager.py
─────────────────────
Backward-compatible alias for the generic :class:`WorkerProcessManager`.

The supervisor logic is market-agnostic and now lives in
:mod:`worker.process_manager`; ``MT5Manager`` is retained as a named subclass so
the FOREX orchestrator (and any external callers) keep working unchanged.
"""

from __future__ import annotations

from worker.process_manager import WorkerProcessManager


class MT5Manager(WorkerProcessManager):
  """Supervises the MT5 child process (see :class:`WorkerProcessManager`)."""
