"""
worker/gateways/errors.py
─────────────────────────
Market-neutral exceptions a gateway raises into the shared pipeline.

Kept free of any broker SDK so ``processor.py`` and ``signal_handler.py`` can
catch them without learning which market raised them.
"""

from __future__ import annotations


class PositionsUnavailableError(RuntimeError):
  """The broker could not report its open positions.

  Distinct from an empty list on purpose: "no data" read as "no positions" is
  how a dropped terminal once made every tracked row look flat. A caller that
  cannot tell what is live must not act as though nothing is.
  """
