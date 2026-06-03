"""Tests for strategy encoding/decoding in the MT5 position comment."""

from worker.mt5.position_comment import (
  COMMENT_SEP,
  MT5_COMMENT_MAX_LEN,
  build_position_comment,
  comment_matches_strategy,
  parse_strategy,
)


def test_build_comment_embeds_strategy_and_action():
  assert build_position_comment("trend-v2", "LONG") == "trend-v2|LONG"


def test_build_comment_truncated_to_mt5_limit():
  long_name = "x" * 40
  comment = build_position_comment(long_name, "SHORT")
  assert len(comment) == MT5_COMMENT_MAX_LEN


def test_parse_strategy_returns_text_before_separator():
  assert parse_strategy("trend-v2|LONG") == "trend-v2"


def test_parse_strategy_handles_empty():
  assert parse_strategy("") == ""
  assert parse_strategy(None) == ""


def test_round_trip_matches_strategy():
  comment = build_position_comment("strat-short", "SHORT")
  assert comment_matches_strategy(comment, "strat-short") is True


def test_different_strategy_does_not_match():
  comment = build_position_comment("strat-long", "LONG")
  assert comment_matches_strategy(comment, "strat-short") is False


def test_legacy_comment_without_separator_does_not_match():
  # Positions opened before this feature carried e.g. "TV LONG".
  assert comment_matches_strategy("TV LONG", "strat-long") is False


def test_truncated_strategy_still_matches_on_prefix():
  long_name = "alpha-strategy-very-long-name-1234567890"
  comment = build_position_comment(long_name, "LONG")
  # Separator was truncated away, but the (truncated) prefix still matches.
  assert COMMENT_SEP not in comment
  assert comment_matches_strategy(comment, long_name) is True
