"""Tests for CryptoReconcileJob — the periodic durability backstop that marks a
DB-open position closed once it no longer exists on the exchange.

These exercise the detection/confirmation logic directly via ``_scan()`` (no
threads), pinning the two key safety properties: two-scan confirmation (so fill
lag never mis-reconciles a fresh entry) and skip-on-fetch-failure (so an API
blip can never mass-close the book).
"""

from types import SimpleNamespace

import pytest

from worker.gateways.crypto.reconcile_job import CryptoReconcileJob


class _FakeExecutor:
  """Resolves signal→exchange symbols and reports live exchange positions."""

  def __init__(self, live_symbols, raise_on_fetch=False):
    self._live = set(live_symbols)  # resolved exchange symbols (e.g. BTCUSDT)
    self._raise = raise_on_fetch

  def get_all_open_positions(self, strategy=None):
    if self._raise:
      raise RuntimeError("positionRisk fetch failed")
    return [SimpleNamespace(symbol=s) for s in self._live]

  @staticmethod
  def get_symbol(symbol):
    s = symbol.upper()
    if s.endswith("USDT"):
      return s
    if s.endswith("USD"):
      return s[:-3] + "USDT"
    return s + "USDT"


class _FakeDb:
  def __init__(self, rows):
    self._rows = list(rows)

  def get_open_positions_for_flat(self, strategy=None, symbol=None):
    return list(self._rows)

  def close(self, ref_source_id):
    self._rows = [r for r in self._rows if r["ref_source_id"] != ref_source_id]


def _row(ref, symbol="BTCUSD", strategy="strat-a"):
  return {"ref_source_id": ref, "symbol": symbol, "strategy": strategy, "volume": 0.01}


def _job(db, executor):
  handled = []

  def handler(row):
    handled.append(row)
    db.close(row["ref_source_id"])  # mimic the real handler closing the row

  return CryptoReconcileJob(db, executor, handler), handled


def test_missed_close_confirmed_after_two_scans():
  db = _FakeDb([_row("100")])
  job, handled = _job(db, _FakeExecutor(live_symbols=set()))  # exchange flat

  job._scan()
  assert handled == []  # first scan only suspects

  job._scan()
  assert len(handled) == 1 and handled[0]["ref_source_id"] == "100"


def test_no_reconcile_on_single_scan():
  db = _FakeDb([_row("100")])
  job, handled = _job(db, _FakeExecutor(live_symbols=set()))
  job._scan()
  assert handled == []


def test_live_position_never_reconciled():
  db = _FakeDb([_row("100", symbol="BTCUSD")])
  job, handled = _job(db, _FakeExecutor(live_symbols={"BTCUSDT"}))
  job._scan()
  job._scan()
  job._scan()
  assert handled == []


def test_fill_lag_does_not_mis_reconcile():
  # Scan 1: exchange hasn't reflected the just-opened position yet (looks flat).
  exec_lagging = _FakeExecutor(live_symbols=set())
  db = _FakeDb([_row("100", symbol="BTCUSD")])
  job, handled = _job(db, exec_lagging)
  job._scan()  # suspects 100

  # Scan 2: exchange now shows the position → suspicion cleared, no reconcile.
  exec_lagging._live = {"BTCUSDT"}
  job._scan()
  assert handled == []


def test_fetch_failure_skips_scan_and_keeps_suspects_untouched():
  db = _FakeDb([_row("100")])
  job, handled = _job(db, _FakeExecutor(live_symbols=set(), raise_on_fetch=True))
  with pytest.raises(RuntimeError):
    job._scan()
  assert handled == []
  assert job._suspected == set()  # never populated from a failed fetch


def test_only_stale_rows_reconciled_in_mixed_book():
  # ETHUSD is flat on the exchange; BTCUSD is still live → only ETH reconciles.
  db = _FakeDb([_row("100", symbol="BTCUSD"), _row("200", symbol="ETHUSD")])
  job, handled = _job(db, _FakeExecutor(live_symbols={"BTCUSDT"}))
  job._scan()
  job._scan()
  assert [r["ref_source_id"] for r in handled] == ["200"]
