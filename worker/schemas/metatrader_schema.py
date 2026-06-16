"""
worker/schemas/metatrader_schema.py
───────────────────────────────────
Backward-compatibility shim.

``TradeResult`` is now a broker-neutral value object living in
:mod:`worker.schemas.trade_result`. It is re-exported here so the many existing
``from worker.schemas.metatrader_schema import TradeResult`` imports keep working.
"""

from worker.schemas.trade_result import TradeResult

__all__ = ["TradeResult"]
