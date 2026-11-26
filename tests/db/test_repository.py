"""
Repository boundary tests: ref_id / ref_source_id stored and returned as str.
"""

import sqlite3

import pytest

from worker.db.errors import PositionStoreUnavailableError
from worker.db.repository import PositionRepository
from worker.db.schema import db_init
from worker.schemas.position_schema import PositionStatusEnum
from worker.settings import settings


@pytest.fixture
def repo(tmp_path, monkeypatch):
  monkeypatch.setattr(settings.database, "file", str(tmp_path / "repo_test.sqlite"))
  db_init()
  return PositionRepository()


def _raw(db_file, sql):
  conn = sqlite3.connect(db_file)
  conn.row_factory = sqlite3.Row
  rows = [dict(r) for r in conn.execute(sql).fetchall()]
  conn.close()
  return rows


def test_insert_then_get_returns_str_ids(repo):
  repo.insert_position(
    ref_id="12345",
    strategy="strat-a",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
    gateway_return_code=10009,
    message='{"x":1}',
    strategy_code=777,
  )
  pos = repo.get_position("12345")
  assert pos["ref_id"] == "12345" and isinstance(pos["ref_id"], str)
  assert pos["ref_source_id"] == "12345" and isinstance(pos["ref_source_id"], str)
  assert pos["strategy_code"] == 777
  assert pos["gateway_return_code"] == 10009
  assert pos["gateway_message"] == '{"x":1}'
  # Old MT5-specific aliases are not part of the gateway-neutral row.
  for k in ("ticket", "source_ticket", "magic", "mt5_retcode", "message"):
    assert k not in pos


def test_ref_ids_physically_stored_as_text(repo):
  repo.insert_position(
    ref_id="999",
    strategy="strat-a",
    symbol="BTCUSD",
    action="long",
    volume=1.0,
    opened_price=30000.0,
  )
  rows = _raw(
    settings.database.file,
    "SELECT ref_id, ref_source_id, typeof(ref_id) AS t FROM positions",
  )
  assert rows[0]["ref_id"] == "999"
  assert rows[0]["ref_source_id"] == "999"
  assert rows[0]["t"] == "text"


def test_update_status_matches_by_ref_source_id(repo):
  repo.insert_position(
    ref_id="100",
    strategy="strat-a",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
  )
  repo.update_position_status(
    ref_source_id="100",
    status=PositionStatusEnum.TP1,
    ref_id="200",
    closed_price=4600.0,
  )
  pos = repo.get_position("100")
  assert pos["status"] == "TP1"
  assert pos["ref_id"] == "200"
  assert pos["ref_source_id"] == "100"


def test_prices_persisted_without_lossy_rounding(repo):
  """Regression: prices must keep full precision. A 2-decimal round (forex-era)
  collapsed low-priced crypto (e.g. SHIB ~0.00002345 → 0.00), corrupting the
  stored opened/closed price and every downstream PositionEvent."""
  entry = 0.00002345
  close = 0.00002567
  repo.insert_position(
    ref_id="42",
    strategy="strat-a",
    symbol="SHIBUSD",
    action="long",
    volume=1_000_000.0,
    opened_price=entry,
  )
  repo.update_position_status(
    ref_source_id="42",
    status=PositionStatusEnum.SL,
    closed_price=close,
  )
  pos = repo.get_position("42")
  assert pos["opened_price"] == entry  # not rounded to 0.0
  assert pos["closed_price"] == close

  repo.log_position(
    strategy="strat-a",
    ref_id="42",
    ref_source_id="42",
    symbol="SHIBUSD",
    action="SL",
    volume=1_000_000.0,
    price=close,
    sl=None,
    tp1=None,
    gateway_return_code=0,
    comment="",
    market_type="CRYPTO",
  )
  rows = _raw(settings.database.file, "SELECT price FROM position_logs")
  assert rows[0]["price"] == close


def test_insert_rejected_position_writes_rejected_row_pending_sync(repo):
  """A policy-rejected entry is stored with status REJECTED and sync_status PENDING
  so PositionCDC forwards it to the broker on the TRADE subject."""
  repo.insert_rejected_position(
    ref_id="sig-1",
    strategy="strat-a",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
    gateway_return_code=-1,
    comment="Max open orders reached (5/5); entry not placed (MAX_OPEN_ORDERS).",
    message='{"signal_id":"sig-1"}',
    strategy_code=777,
    market_type="FOREX",
  )
  pos = repo.get_position("sig-1")
  assert pos["status"] == "REJECTED"
  assert pos["ref_id"] == "sig-1" and pos["ref_source_id"] == "sig-1"
  assert pos["sync_status"] == "PENDING"
  assert "Max open orders reached" in pos["comment"]
  # The REJECTED row is not OPENED/TP1, so it never occupies the one-active-position
  # slot — a real position can still open on the same (strategy, symbol).
  repo.insert_position(
    ref_id="100",
    strategy="strat-a",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
  )
  assert repo.get_position("100")["status"] == "OPENED"


def test_insert_rejected_position_swallows_errors(repo, monkeypatch):
  """Unlike insert_position, a failure recording a rejection must not raise —
  the rejection is advisory and must not abort signal processing."""

  def boom(*a, **k):
    raise sqlite3.OperationalError("disk full")

  monkeypatch.setattr(repo, "_insert_position_row", boom)
  # Must not propagate.
  repo.insert_rejected_position(
    ref_id="x",
    strategy="s",
    symbol="Y",
    action="long",
    volume=1.0,
    opened_price=1.0,
  )


def test_log_position_persists_generic_columns(repo):
  repo.log_position(
    strategy="strat-a",
    ref_id="5",
    ref_source_id="5",
    symbol="XAUUSD",
    action="SL",
    volume=0.5,
    price=4400.0,
    sl=None,
    tp1=None,
    gateway_return_code=0,
    comment="x",
    message='{"a":1}',
    author="terminal",
    market_type="FOREX",
  )
  rows = _raw(
    settings.database.file,
    "SELECT ref_id, ref_source_id, gateway_return_code, gateway_message, market_type FROM position_logs",
  )
  assert rows[0]["ref_id"] == "5"
  assert rows[0]["ref_source_id"] == "5"
  assert rows[0]["gateway_message"] == '{"a":1}'
  assert rows[0]["market_type"] == "FOREX"


# ── signal_id dedup (used by the ACK replay) ──────────────────────────────── #


def test_signal_exists_returns_true_only_after_signal_logged(repo):
  # A signal_id we've never seen is not present.
  assert repo.signal_exists("sig-abc") is False

  # Once a position for that signal_id is logged, the ACK replay handler
  # can find it and dedup incoming replays.
  repo.log_position(
    strategy="strat-a",
    ref_id="5",
    ref_source_id="5",
    symbol="XAUUSD",
    action="LONG",
    volume=0.5,
    price=4400.0,
    sl=None,
    tp1=None,
    gateway_return_code=0,
    comment="",
    signal_id="sig-abc",
  )
  assert repo.signal_exists("sig-abc") is True
  # Other ids must remain not-seen (no false positives from partial matches).
  assert repo.signal_exists("sig-abcd") is False


def test_signal_exists_treats_missing_id_as_not_seen(repo):
  # Signals without an id are never dedup'd — the caller decides what to do
  # (currently: pass through).
  assert repo.signal_exists("") is False
  assert repo.signal_exists(None) is False


def test_insert_position_persists_signal_id(repo):
  repo.insert_position(
    ref_id="200",
    strategy="strat-a",
    symbol="BTCUSD",
    action="long",
    volume=1.0,
    opened_price=30000.0,
    signal_id="sig-open",
  )
  rows = _raw(
    settings.database.file, "SELECT signal_id FROM positions WHERE ref_id='200'"
  )
  assert rows[0]["signal_id"] == "sig-open"
  # And signal_exists (which reads position_logs) is still driven by log_position,
  # not by the positions insert — a signal that only produced a positions row
  # without a matching log row is not visible to the dedup gate.
  assert repo.signal_exists("sig-open") is False


def test_update_status_never_rewrites_a_closed_row(repo):
  """ref_source_id is not unique over the table's history and a close event can
  arrive after another path already closed the row: only an active row changes."""
  repo.insert_position(
    ref_id="300",
    strategy="strat-a",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
  )
  repo.update_position_status(
    ref_source_id="300", status=PositionStatusEnum.TP2, closed_price=4600.0
  )
  repo.update_position_status(
    ref_source_id="300",
    status=PositionStatusEnum.TERMINAL_CLOSED,
    closed_price=4400.0,
  )
  pos = repo.get_position("300")
  assert pos["status"] == "TP2"
  assert pos["closed_price"] == 4600.0


def test_update_status_leaves_a_rejected_row_alone(repo):
  repo.insert_rejected_position(
    ref_id="sig-9",
    strategy="strat-a",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
    comment="Max open orders reached",
  )
  repo.update_position_status(ref_source_id="sig-9", status=PositionStatusEnum.FLATTED)
  assert repo.get_position("sig-9")["status"] == "REJECTED"


def test_repository_can_be_pointed_at_its_own_file(tmp_path, monkeypatch):
  """An injected db_file is used instead of the global settings' one."""
  own = tmp_path / "own.sqlite"
  monkeypatch.setattr(settings.database, "file", str(own))
  db_init()
  monkeypatch.setattr(settings.database, "file", str(tmp_path / "elsewhere.sqlite"))

  repo = PositionRepository(db_file=str(own))
  repo.insert_position(
    ref_id="1",
    strategy="s",
    symbol="XAUUSD",
    action="long",
    volume=0.1,
    opened_price=1.0,
  )

  assert repo.get_position("1")["symbol"] == "XAUUSD"
  assert not (tmp_path / "elsewhere.sqlite").exists()


# ── Failed reads raise instead of reporting "nothing open" ────────────────── #


def _broken_connect(*args, **kwargs):
  raise sqlite3.OperationalError("database is locked")


@pytest.mark.parametrize(
  "query",
  [
    lambda r: r.get_open_positions_for_flat(),
    lambda r: r.get_open_positions_for_flat(symbol="XAUUSD"),
    lambda r: r.get_open_positions_by_strategy("strat-a", "XAUUSD"),
  ],
)
def test_open_position_reads_raise_when_the_store_fails(repo, monkeypatch, query):
  """An empty list here is read as "nothing is open" by the entry guards, ADMIN
  FLAT and the reconcile job — a failed query must say so instead."""
  monkeypatch.setattr(repo, "_connect", _broken_connect)
  with pytest.raises(PositionStoreUnavailableError):
    query(repo)


# ── CDC sync_version ──────────────────────────────────────────────────────── #


def _insert_open(repo, ref_id="777"):
  repo.insert_position(
    ref_id=ref_id,
    strategy="strat-a",
    symbol="XAUUSD",
    action="long",
    volume=0.5,
    opened_price=4500.0,
  )
  return repo.get_pending_sync_positions()[0]


def test_every_status_change_bumps_sync_version(repo):
  row = _insert_open(repo)
  assert row["sync_version"] == 0
  repo.update_position_status("777", PositionStatusEnum.TP1)
  assert repo.get_position("777")["sync_version"] == 1


def test_mark_synced_applies_only_to_the_version_that_was_published(repo):
  """The CDC reads a row, publishes it, then marks it. A change landing in
  between — even within the same second, which ``updated_at`` cannot tell
  apart — must keep the row PENDING so the new state is published too."""
  read = _insert_open(repo)
  repo.update_position_status("777", PositionStatusEnum.TP2)

  assert repo.mark_position_synced(read["id"], read["sync_version"]) is False
  pending = repo.get_pending_sync_positions()
  assert [r["status"] for r in pending] == ["TP2"]

  assert repo.mark_position_synced(pending[0]["id"], pending[0]["sync_version"])
  assert repo.get_pending_sync_positions() == []


# ── The write-ahead signal attempt ────────────────────────────────────────── #


def _attempt(repo, **over):
  kwargs = {
    "strategy": "strat-a",
    "symbol": "XAUUSD",
    "action": "long",
    "volume": 0.5,
    "price": 4500.0,
    "sl": 4400.0,
    "tp1": 4600.0,
    "message": '{"signal_id":"sig-1"}',
    "author": "broker",
    "market_type": "FOREX",
    "signal_id": "sig-1",
    "signal_uxid": "uxid-1",
  }
  kwargs.update(over)
  return repo.begin_signal_attempt(**kwargs)


def test_an_attempt_is_recorded_before_the_broker_answers(repo):
  """The row must exist while the order is still in flight — it is the only
  durable proof the signal reached the broker if the worker then dies."""
  log_id = _attempt(repo)

  rows = _raw(settings.database.file, "SELECT * FROM position_logs")
  assert len(rows) == 1
  assert rows[0]["id"] == log_id
  assert rows[0]["attempt_outcome"] == "ATTEMPTING"
  assert rows[0]["signal_id"] == "sig-1"
  # The intent is recorded, not an outcome: no ticket, no return code yet.
  assert rows[0]["ref_id"] is None and rows[0]["gateway_return_code"] == -1


def test_finalizing_writes_the_outcome_onto_the_attempt(repo):
  log_id = _attempt(repo)

  assert (
    repo.finalize_signal_attempt(
      log_id,
      attempt_outcome="EXECUTED",
      ref_id="999",
      ref_source_id="999",
      volume=0.5,
      price=4502.0,
      sl=4395.0,
      gateway_return_code=10009,
      comment="filled",
    )
    is True
  )

  row = _raw(settings.database.file, "SELECT * FROM position_logs")[0]
  assert row["attempt_outcome"] == "EXECUTED"
  assert row["ref_id"] == "999" and row["gateway_return_code"] == 10009
  assert row["price"] == 4502.0 and row["sl"] == 4395.0


def test_a_settled_attempt_is_not_overwritten(repo):
  """Guarded on the row still being ATTEMPTING, so a late finalisation cannot
  undo an outcome already recorded."""
  log_id = _attempt(repo)
  repo.finalize_signal_attempt(log_id, attempt_outcome="EXECUTED", ref_id="999")

  assert (
    repo.finalize_signal_attempt(log_id, attempt_outcome="FAILED", ref_id="x") is False
  )
  row = _raw(settings.database.file, "SELECT * FROM position_logs")[0]
  assert row["attempt_outcome"] == "EXECUTED" and row["ref_id"] == "999"


def test_begin_attempt_raises_when_it_cannot_be_recorded(repo, monkeypatch):
  """An unrecordable attempt must stop the order: without the row there is
  nothing to dedup a redelivery against."""
  monkeypatch.setattr(repo, "_connect", _broken_connect)
  with pytest.raises(PositionStoreUnavailableError):
    _attempt(repo)


def test_a_failed_finalisation_leaves_the_row_attempting(repo, monkeypatch):
  """The order already happened, so this must not raise — and the row staying
  ATTEMPTING is the correct conservative state."""
  log_id = _attempt(repo)
  monkeypatch.setattr(repo, "_connect", _broken_connect)

  assert repo.finalize_signal_attempt(log_id, attempt_outcome="EXECUTED") is False


# ── The dedup lookup reads the verdict, not the row's existence ───────────── #


@pytest.mark.parametrize(
  "outcome,blocks",
  [("EXECUTED", True), ("ATTEMPTING", True), ("FAILED", False)],
)
def test_signal_exists_blocks_only_unsettled_or_executed_attempts(
  repo, outcome, blocks
):
  """A FAILED attempt is replayable — nothing is live for it, and filling that
  gap is what the broker's retry channel is for. The other two block: one is
  done, the other may have left a live position."""
  log_id = _attempt(repo, signal_id="sig-x")
  if outcome != "ATTEMPTING":
    repo.finalize_signal_attempt(log_id, attempt_outcome=outcome)

  assert repo.signal_exists("sig-x") is blocks


def test_signal_exists_is_false_for_an_unseen_or_empty_id(repo):
  assert repo.signal_exists("never-seen") is False
  assert repo.signal_exists("") is False
  assert repo.signal_exists(None) is False


def test_signal_exists_raises_when_the_lookup_fails(repo, monkeypatch):
  """It used to answer "seen", which silently dropped a replayed signal for
  good. The callers need opposite dispositions, so the error reaches them."""
  monkeypatch.setattr(repo, "_connect", _broken_connect)
  with pytest.raises(PositionStoreUnavailableError):
    repo.signal_exists("sig-1")


# ── Unresolved attempts are reported, not forgotten ──────────────────────── #


def test_stale_attempts_respect_the_cutoff_and_the_market(repo):
  _attempt(repo, signal_id="fresh")
  _attempt(repo, signal_id="other-market", market_type="CRYPTO")
  settled = _attempt(repo, signal_id="done")
  repo.finalize_signal_attempt(settled, attempt_outcome="EXECUTED")

  # Nothing is old enough yet.
  assert repo.stale_signal_attempts(120) == []

  # Age every row past the cutoff.
  conn = sqlite3.connect(settings.database.file)
  conn.execute("UPDATE position_logs SET timestamp = datetime('now', '-10 minutes')")
  conn.commit()
  conn.close()

  ids = [r["signal_id"] for r in repo.stale_signal_attempts(120)]
  assert ids == ["fresh", "other-market"]  # the settled one is not unresolved
  # position_logs is shared by both markets, so a starting worker must only see
  # its own: the other market alone can say whether that order is live.
  forex = [r["signal_id"] for r in repo.stale_signal_attempts(120, "FOREX")]
  assert forex == ["fresh"]
