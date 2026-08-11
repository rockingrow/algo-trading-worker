"""Tests for ForexReconcileJob — the periodic durability backstop that marks a
DB-open position closed once it no longer exists on the trading platform.

These exercise the detection/confirmation logic directly via ``_scan()`` (no
threads) and pin the properties that make the job safe on a live account:
two-scan confirmation, ticket **and** slot matching (so a re-ticketed position is
never mistaken for a closed one), per-strategy isolation via magic, and the
connection guards that stop an offline terminal's empty position list from being
read as a flat account.
"""

from types import SimpleNamespace

import pytest
from helpers import make_platform_position

from worker.gateways.config import ExecutionConfig
from worker.gateways.forex.base import Tick
from worker.gateways.forex.executor import ForexExecutor
from worker.gateways.forex.reconcile_job import ForexReconcileJob
from worker.gateways.forex.signal_processor import ForexSignalProcessor
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.settings import MT5_HEALTH_INTERVAL_WEEKEND

# Magic-number isolation, exactly as STRATEGY_MAGIC_MAP provides it.
_MAGICS = {"strat-a": 101, "strat-b": 202}


class _FakeExecutor:
  """Resolves signal→platform symbols and reports the worker's live positions."""

  def __init__(self, positions=(), raise_on_fetch=False, raise_on_symbol=False):
    self.positions = list(positions)
    self._raise_fetch = raise_on_fetch
    self._raise_symbol = raise_on_symbol

  def get_all_open_positions(self, strategy=None):
    if self._raise_fetch:
      raise RuntimeError("positions_get failed")
    return list(self.positions)

  def get_symbol(self, symbol):
    if self._raise_symbol:
      raise RuntimeError("symbol resolution failed")
    # Mirrors a broker's suffixed naming (XAUUSD → XAUUSDc).
    return symbol if symbol.endswith("c") else f"{symbol}c"


class _FakeGateway:
  """Connection state only — that is all the job asks a gateway for."""

  def __init__(self, connected=True):
    self.connected = connected

  def is_connected(self):
    return self.connected


class _FlakyGateway:
  """Reports connection state from a scripted sequence (last value repeats)."""

  def __init__(self, results):
    self._results = list(results)

  def is_connected(self):
    return self._results.pop(0) if len(self._results) > 1 else self._results[0]


class _FakeDb:
  def __init__(self, rows):
    self._rows = list(rows)

  def get_open_positions_for_flat(self, strategy=None, symbol=None):
    return list(self._rows)

  def close(self, ref_source_id):
    self._rows = [r for r in self._rows if r["ref_source_id"] != ref_source_id]


def _row(ref, symbol="XAUUSD", strategy="strat-a", ref_id=None, volume=0.01):
  """A DB-open row. ``ref_id`` defaults to ``ref_source_id``, as on a fresh entry."""
  return {
    "ref_source_id": ref,
    "ref_id": ref if ref_id is None else ref_id,
    "symbol": symbol,
    "strategy": strategy,
    "volume": volume,
  }


def _job(db, executor, gateway=None, market_closed=False):
  handled = []

  def handler(row):
    handled.append(row)
    db.close(row["ref_source_id"])  # mimic the real handler closing the row

  job = ForexReconcileJob(
    db_service=db,
    executor=executor,
    handler=handler,
    gateway=_FakeGateway() if gateway is None else gateway,
    # Raises KeyError on an unmapped strategy, like ForexExecutor._magic_for.
    magic_for=_MAGICS.__getitem__,
    market_closed_fn=lambda: market_closed,
  )
  return job, handled


# ── missed-close detection ──────────────────────────────────────────────────── #


def test_stuck_row_reconciled_after_two_scans():
  # The reported bug: a TP1 partial close whose volume equalled the whole 0.01-lot
  # position closed it outright, leaving the row stuck TP1 with nothing live.
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[]))

  job._scan()
  assert handled == []  # first scan only suspects

  job._scan()
  assert len(handled) == 1 and handled[0]["ref_source_id"] == "111"


def test_no_reconcile_on_single_scan():
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[]))
  job._scan()
  assert handled == []


def test_live_ticket_never_reconciled():
  live = make_platform_position(ticket=111, magic=101, symbol="XAUUSDc")
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[live]))
  job._scan()
  job._scan()
  job._scan()
  assert handled == []


def test_ref_id_match_keeps_row_live():
  # After a partial close the row's ref_id carries the newer ticket while
  # ref_source_id keeps the original — either side matching means it is live.
  live = make_platform_position(ticket=222, magic=101, symbol="XAUUSDc")
  db = _FakeDb([_row("111", ref_id="222")])
  job, handled = _job(db, _FakeExecutor(positions=[live]))
  job._scan()
  job._scan()
  assert handled == []


def test_reticketed_position_never_reconciled():
  # Broker re-ticketed the position after a partial close and the new ticket is
  # in neither DB column. The (symbol, magic) slot key still matches, so the
  # still-live position is never marked closed.
  live = make_platform_position(ticket=999, magic=101, symbol="XAUUSDc")
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[live]))
  job._scan()
  job._scan()
  assert handled == []


def test_another_strategys_position_does_not_keep_row_live():
  # strat-b holds the same symbol under its own magic; that must not shield
  # strat-a's stale row from reconciliation.
  live = make_platform_position(ticket=999, magic=_MAGICS["strat-b"], symbol="XAUUSDc")
  db = _FakeDb([_row("111", strategy="strat-a")])
  job, handled = _job(db, _FakeExecutor(positions=[live]))
  job._scan()
  job._scan()
  assert [r["ref_source_id"] for r in handled] == ["111"]


def test_only_stale_rows_reconciled_in_mixed_book():
  # strat-a's XAUUSD position is gone; strat-b's EURUSD one is still live.
  live = make_platform_position(ticket=222, magic=_MAGICS["strat-b"], symbol="EURUSDc")
  db = _FakeDb(
    [
      _row("111", symbol="XAUUSD", strategy="strat-a"),
      _row("222", symbol="EURUSD", strategy="strat-b"),
    ]
  )
  job, handled = _job(db, _FakeExecutor(positions=[live]))
  job._scan()
  job._scan()
  assert [r["ref_source_id"] for r in handled] == ["111"]


def test_unmapped_strategy_falls_back_to_symbol_match():
  # No magic for this strategy → the slot key degrades to symbol-only, so any
  # live position on the symbol keeps the row alive (conservative by design).
  live = make_platform_position(ticket=999, magic=777, symbol="XAUUSDc")
  db = _FakeDb([_row("111", strategy="strat-unmapped")])
  job, handled = _job(db, _FakeExecutor(positions=[live]))
  job._scan()
  job._scan()
  assert handled == []


def test_unmapped_strategy_still_reconciled_when_platform_flat():
  db = _FakeDb([_row("111", strategy="strat-unmapped")])
  job, handled = _job(db, _FakeExecutor(positions=[]))
  job._scan()
  job._scan()
  assert [r["ref_source_id"] for r in handled] == ["111"]


def test_entry_lag_does_not_mis_reconcile():
  # Scan 1: the just-opened position has not appeared in positions_get() yet.
  executor = _FakeExecutor(positions=[])
  db = _FakeDb([_row("111")])
  job, handled = _job(db, executor)
  job._scan()  # suspects 111

  # Scan 2: the platform now reports it → suspicion cleared, no reconcile.
  executor.positions = [make_platform_position(ticket=111, magic=101, symbol="XAUUSDc")]
  job._scan()
  assert handled == []


# ── safety guards ───────────────────────────────────────────────────────────── #


def test_fetch_failure_skips_scan_and_keeps_suspects_untouched():
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[], raise_on_fetch=True))
  with pytest.raises(RuntimeError):
    job._scan()
  assert handled == []
  assert job._suspected == set()  # never populated from a failed fetch


def test_symbol_resolution_failure_treats_row_as_live():
  # Keys cannot be computed → the row is left alone rather than closed on
  # unverifiable information.
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[], raise_on_symbol=True))
  job._scan()
  job._scan()
  assert handled == []
  assert job._suspected == set()


def test_weekend_still_scans_while_the_terminal_is_connected():
  # The reported bug: 24/7 instruments (crypto CFDs like BTCUSD) trade straight
  # through the FOREX weekend window with the terminal up. Gating the scan on the
  # calendar made a manual close there invisible — the row stayed OPENED all
  # weekend and blocked every later signal on the symbol.
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[]), market_closed=True)

  job._scan()
  job._scan()
  assert len(handled) == 1 and handled[0]["ref_source_id"] == "111"


def test_weekend_backs_off_only_once_the_connection_is_parked():
  # The health thread closes the connection a few minutes into the window; until
  # it does, the normal cadence must hold so a 24/7 close is caught in two scans.
  db = _FakeDb([])
  open_job, _ = _job(db, _FakeExecutor(), market_closed=False)
  weekend_connected, _ = _job(db, _FakeExecutor(), market_closed=True)
  weekend_parked, _ = _job(
    db, _FakeExecutor(), gateway=_FakeGateway(connected=False), market_closed=True
  )
  # A weekday disconnect keeps the fast cadence so the job resumes on reconnect.
  weekday_disconnected, _ = _job(
    db, _FakeExecutor(), gateway=_FakeGateway(connected=False), market_closed=False
  )

  assert open_job._next_interval() == open_job._poll_interval
  assert weekend_connected._next_interval() == weekend_connected._poll_interval
  assert weekend_parked._next_interval() == MT5_HEALTH_INTERVAL_WEEKEND
  assert weekday_disconnected._next_interval() == weekday_disconnected._poll_interval


def test_disconnected_platform_skips_scan():
  # positions_get() returns None while the terminal is offline and the gateway
  # maps that to [] — which must never be read as "the account is flat".
  db = _FakeDb([_row("111")])
  job, handled = _job(
    db, _FakeExecutor(positions=[]), gateway=_FakeGateway(connected=False)
  )
  job._scan()
  job._scan()
  assert handled == []
  assert job._suspected == set()


def test_connection_dropped_mid_scan_skips_scan():
  # Connected when the scan starts, gone by the time the (empty) position list
  # comes back → treated as "no data", not "no positions".
  db = _FakeDb([_row("111")])
  job, handled = _job(
    db, _FakeExecutor(positions=[]), gateway=_FlakyGateway([True, False])
  )
  job._scan()
  assert handled == []
  assert job._suspected == set()


def test_no_gateway_wired_still_scans():
  # The gateway is optional (unit tests / platforms without a connection probe).
  db = _FakeDb([_row("111")])
  job, handled = _job(db, _FakeExecutor(positions=[]))
  job._gateway = None
  job._scan()
  job._scan()
  assert [r["ref_source_id"] for r in handled] == ["111"]


# ── integration with the real ForexExecutor ─────────────────────────────────── #


class _ResolvingGateway:
  """Minimal platform gateway: resolves XAUUSD → XAUUSDc and reports positions."""

  def __init__(self, positions):
    self._positions = list(positions)

  def resolve_symbol(self, base_symbol):
    return base_symbol if base_symbol.endswith("c") else f"{base_symbol}c"

  def get_positions(self, symbol=None):
    return list(self._positions)

  def is_connected(self):
    return True


def test_real_executor_reconciles_row_whose_magic_is_no_longer_live():
  # Drives the real ForexExecutor: get_all_open_positions() filters to the magics
  # this worker owns, so strat-a's row is stale while strat-b's stays live.
  gateway = _ResolvingGateway(
    [
      make_platform_position(ticket=222, magic=_MAGICS["strat-b"], symbol="EURUSDc"),
    ]
  )
  executor = ForexExecutor(
    gateway=gateway,
    config=ExecutionConfig(
      volume_decision_enabled=True,
      capital=1000.0,
      risk_percentage=2.0,
      use_account_equity=False,
    ),
    strategy_magic_map=_MAGICS,
  )
  db = _FakeDb(
    [
      _row("111", symbol="XAUUSD", strategy="strat-a"),
      _row("222", symbol="EURUSD", strategy="strat-b"),
    ]
  )
  job, handled = _job(db, executor, gateway=gateway)
  job._magic_for = executor._magic_for

  job._scan()
  job._scan()
  assert [r["ref_source_id"] for r in handled] == ["111"]


# ── the handler the job calls: ForexSignalProcessor._on_missed_close ────────── #


_DEFAULT_TICK = Tick(bid=1999.0, ask=2001.0)


class _RecordingDb:
  def __init__(self):
    self.logs = []
    self.status_updates = []

  def log_position(self, **kwargs):
    self.logs.append(kwargs)

  def update_position_status(self, **kwargs):
    self.status_updates.append(kwargs)


class _FakeProc:
  """Drives the REAL missed-close handler with fakes for everything it touches."""

  _on_missed_close = ForexSignalProcessor._on_missed_close
  _best_effort_price = ForexSignalProcessor._best_effort_price
  _reconciled_pnl = ForexSignalProcessor._reconciled_pnl

  def __init__(self, tick=_DEFAULT_TICK, position_pnl=None):
    self.executor = _FakeExecutor()
    self.gateway = SimpleNamespace(
      get_tick=lambda symbol: tick,
      get_position_realized_pnl=lambda ticket: position_pnl,
    )
    self.db = _RecordingDb()
    self.notifications = []
    self._market_type = "forex"
    self.ctx = SimpleNamespace(
      db_service=self.db,
      channel_notifier=SimpleNamespace(
        send_message=lambda m: self.notifications.append(m)
      ),
    )

  def _account_footer(self):
    return "FOOTER"


def test_missed_close_marks_row_terminal_closed_and_notifies():
  proc = _FakeProc()

  proc._on_missed_close(_row("111", volume=0.01))

  upd = proc.db.status_updates[0]
  assert upd["ref_source_id"] == "111"
  assert upd["status"] == PositionStatusEnum.TERMINAL_CLOSED
  assert upd["closed_price"] == 2000.0  # mid of 1999.0 / 2001.0
  assert "Reconciled" in upd["comment"]

  log = proc.db.logs[0]
  assert log["author"] == LogAuthorEnum.TERMINAL.value
  assert log["action"] == PositionStatusEnum.TERMINAL_CLOSED.value
  assert log["volume"] == 0.01
  assert log["market_type"] == "forex"

  assert len(proc.notifications) == 1
  assert "Reconciled" in proc.notifications[0]


def test_missed_close_still_syncs_db_without_a_tick():
  # No market data (symbol suspended / terminal busy) — the row must still be
  # unstuck; only the approximate close price is missing.
  proc = _FakeProc(tick=None)

  proc._on_missed_close(_row("111"))

  assert proc.db.status_updates[0]["status"] == PositionStatusEnum.TERMINAL_CLOSED
  assert proc.db.status_updates[0]["closed_price"] is None
  assert len(proc.notifications) == 1


def test_missed_close_reports_the_positions_realized_pnl():
  # The reconciler never saw the close, so there is no TradeResult to read a
  # figure off — the platform's deal history is the only source, and it covers
  # the position's whole life (hence the "position total" label).
  proc = _FakeProc(position_pnl=13.85)

  proc._on_missed_close(_row("111"))

  assert "PnL (position total): <b>+13.85</b>" in proc.notifications[0]


def test_missed_close_reports_an_unreadable_pnl_as_unknown():
  proc = _FakeProc(position_pnl=None)

  proc._on_missed_close(_row("111"))

  assert "PnL (position total): <b>n/a</b>" in proc.notifications[0]


def test_missed_close_survives_a_platform_that_cannot_answer():
  # A failed history read costs the PnL line, never the reconcile — unsticking
  # the DB row is this job's actual purpose.
  proc = _FakeProc()

  def _boom(_ticket):
    raise RuntimeError("terminal busy")

  proc.gateway = SimpleNamespace(get_tick=lambda symbol: _DEFAULT_TICK)
  proc.gateway.get_position_realized_pnl = _boom

  proc._on_missed_close(_row("111"))

  assert proc.db.status_updates[0]["status"] == PositionStatusEnum.TERMINAL_CLOSED
  assert "PnL (position total): <b>n/a</b>" in proc.notifications[0]
