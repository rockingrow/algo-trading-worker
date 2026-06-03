"""
worker/mt5/position_comment.py
──────────────────────────────
Encode/decode the owning *strategy* inside an MT5 position ``comment``.

Why
───
Positions are filtered broker-side only by ``magic`` + ``symbol``. That is too
coarse when several strategies trade the *same* symbol at once (e.g. a Long-only
and a Short-only strategy both holding a position on XAUUSD). Without a finer
discriminator, an entry/exit signal for one strategy would fetch — and
force-close — the other strategy's position too.

To keep each strategy isolated we stamp the strategy name into the position
comment when opening, then parse it back when we need to act on "only this
strategy's" positions.

Comment layout
──────────────
    "<strategy>|<action>"     e.g. "trend-v2|LONG"

The strategy is written *first* so it survives MT5's comment-length truncation
(the action is only informational once stored). The pipe ``|`` is reserved as
the field separator and must not appear inside a strategy name.

Matching is tolerant of broker-side truncation:
  * If the separator survived in the stored comment, the strategy was stored in
    full → require an exact match.
  * If the separator is gone, the strategy name itself was truncated → fall back
    to a prefix match against the queried strategy.

Legacy positions opened before this feature (e.g. ``"TV LONG"``) carry no
separator and will not match any real strategy name, so strategy-scoped queries
simply ignore them.
"""

from __future__ import annotations

from typing import Any

# MT5 caps the order ``comment`` field. 31 is the documented MetaTrader 5 limit;
# some brokers truncate further, which the prefix-match path below tolerates.
MT5_COMMENT_MAX_LEN = 31

# Reserved separator between the strategy name and the action in a comment.
COMMENT_SEP = "|"


def build_position_comment(strategy: str, action: str) -> str:
  """Build the MT5 comment for a freshly opened position.

  Format ``"<strategy>|<action>"`` truncated to :data:`MT5_COMMENT_MAX_LEN`.
  """
  strategy = (strategy or "").strip()
  action = (action or "").strip()
  return f"{strategy}{COMMENT_SEP}{action}"[:MT5_COMMENT_MAX_LEN]


def parse_strategy(comment: Any) -> str:
  """Extract the strategy name from a position ``comment`` (text before ``|``)."""
  if not comment:
    return ""
  return str(comment).split(COMMENT_SEP, 1)[0].strip()


def comment_matches_strategy(comment: Any, strategy: str) -> bool:
  """Return ``True`` if *comment* was stamped with *strategy*.

  Tolerant of broker-side truncation (see module docstring).
  """
  stored = parse_strategy(comment)
  if not stored:
    return False
  expected = (strategy or "").strip()
  if not expected:
    return False
  if COMMENT_SEP in str(comment):
    # Separator survived → the strategy was stored in full.
    return stored == expected
  # Separator gone → the strategy name itself was truncated; match on prefix.
  return expected.startswith(stored)
