"""Tests for the TradeResult value object and its dict-compat shim."""

from worker.schemas.trade_result import TradeResult


def test_ok_defaults():
  r = TradeResult.ok(ticket="5", price=100.0, volume=1.0)
  assert r.success is True
  assert r.retcode == 0
  assert r.ticket == "5"
  # mapping access mirrors attribute access
  assert r["success"] is True
  assert r.get("price") == 100.0


def test_fail_defaults():
  r = TradeResult.fail("boom")
  assert r.success is False
  assert r.retcode == -1
  assert r.comment == "boom"
  assert r["comment"] == "boom"


def test_fail_custom_retcode():
  r = TradeResult.fail("rejected", retcode=10016)
  assert r.retcode == 10016


def test_unknown_kwargs_go_to_extra():
  r = TradeResult.ok(weird="x")
  assert r.get("weird") == "x"
  assert r["weird"] == "x"
  assert "weird" in r


def test_mapping_setitem_known_and_unknown():
  r = TradeResult.ok(ticket="1")
  r["source_ticket"] = "9"     # known field → attribute
  r["custom"] = "y"            # unknown → extra
  assert r.source_ticket == "9"
  assert r["source_ticket"] == "9"
  assert r.extra["custom"] == "y"


def test_get_default_and_contains():
  r = TradeResult.fail("x")
  assert r.get("missing") is None
  assert r.get("missing", 42) == 42
  assert "ticket" in r          # declared field always "in"
  assert "nope" not in r


def test_setdefault_only_fills_missing():
  r = TradeResult.ok()          # volume is None
  assert r.setdefault("volume", 0.5) == 0.5
  assert r.volume == 0.5
  # already set → unchanged
  assert r.setdefault("volume", 9.9) == 0.5
