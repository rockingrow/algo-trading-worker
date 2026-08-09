"""Tests for MT5EventJob — the 5 s poll that reports terminal-side closes.

These pin the two properties that made it silently miss a manual close on a live
account: it must keep polling through the FOREX weekend window (24/7 instruments
trade then), and it must resolve the worker's magics per scan rather than hold a
snapshot taken when the job was built.
"""

import threading

from worker.gateways.forex.executor import ForexExecutor
from worker.gateways.forex.mt5.close_detector import (
  TerminalClosedEvent,
  TerminalCloseReason,
)
from worker.jobs import mt5_event_job
from worker.jobs.mt5_event_job import MT5EventJob
from worker.settings import MT5_HEALTH_INTERVAL_WEEKEND


class _OneShotEvent(threading.Event):
  """Lets exactly one loop iteration run, then stops the job."""

  def wait(self, timeout=None):
    self.set()
    return True


class _Notifier:
  def __init__(self):
    self.sent = []

  def send_message(self, msg, *_args, **_kwargs):
    self.sent.append(msg)


class _FakeDb:
  """Records the DB writes MT5EventJob makes for a terminal close."""

  def __init__(self, position=None):
    self._position = position
    self.statuses = []

  def get_position(self, ref_source_id):
    return self._position

  def log_position(self, **kwargs):
    pass

  def update_position_status(self, ref_source_id, status, **kwargs):
    self.statuses.append((ref_source_id, status))


def _job(magics, *, connected=True, db=None, notifier=None):
  return MT5EventJob(
    magic_numbers=magics,
    db_service=db,
    notifier=notifier or _Notifier(),
    is_connected=(None if connected is None else (lambda: connected)),
  )


def _event(ticket="555", *, sl=1890.0, tp=1950.0):
  return TerminalClosedEvent(
    source_ticket=ticket,
    deal_ticket=f"d{ticket}",
    symbol="XAUUSDm",
    close_reason=TerminalCloseReason.MANUAL,
    deal_reason=0,
    close_price=1920.123456,
    close_volume=0.01,
    close_time=None,
    entry_price=1900.0,
    sl=sl,
    tp=tp,
    account=None,
  )


def _run_once(job, monkeypatch, *, market_closed):
  """Drive exactly one iteration of the poll loop, returning the magics the scan
  was called with (or None when the scan was skipped)."""
  seen = []
  monkeypatch.setattr(mt5_event_job, "is_market_closed", lambda: market_closed)
  monkeypatch.setattr(
    mt5_event_job,
    "scan_terminal_closed_positions",
    lambda magics, tickets: seen.append(set(magics)) or [],
  )
  job._stop_event = _OneShotEvent()
  job._run()
  return seen[0] if seen else None


# ── weekend gating (the reported bug) ───────────────────────────────────────── #


def test_keeps_polling_over_the_weekend_while_connected(monkeypatch):
  # A crypto CFD (BTCUSD) trades straight through the FOREX weekend window with
  # the terminal connected. Skipping the scan on the calendar meant a manual
  # close there was never detected at all.
  job = _job({101}, connected=True)
  assert _run_once(job, monkeypatch, market_closed=True) == {101}


def test_skips_scan_while_the_terminal_is_parked(monkeypatch):
  # Once the health thread closes the connection, positions_get() returns None
  # and every scan would warn — skip quietly instead.
  job = _job({101}, connected=False)
  assert _run_once(job, monkeypatch, market_closed=True) is None


def test_scans_when_no_connection_probe_is_wired(monkeypatch):
  job = _job({101}, connected=None)
  assert _run_once(job, monkeypatch, market_closed=True) == {101}


def test_offline_backoff_is_slow_only_over_the_weekend(monkeypatch):
  job = _job({101}, connected=False)

  monkeypatch.setattr(mt5_event_job, "is_market_closed", lambda: True)
  assert job._offline_interval() == MT5_HEALTH_INTERVAL_WEEKEND

  # A weekday disconnect keeps the fast cadence so polling resumes on reconnect.
  monkeypatch.setattr(mt5_event_job, "is_market_closed", lambda: False)
  assert job._offline_interval() == job._poll_interval


# ── live magic map ──────────────────────────────────────────────────────────── #


def test_magics_are_resolved_per_scan_not_snapshotted(monkeypatch, config):
  # The broker re-pushes strategy_magic_map on every WORKER_CONNECTED_ACK — a
  # NATS reconnect re-runs the handshake. A snapshot taken at build time would
  # stop matching this worker's own tickets and blind close detection.
  executor = ForexExecutor(
    gateway=None, config=config, strategy_magic_map={"strat-a": 101}
  )
  job = _job(executor.owned_magics, connected=True)

  assert _run_once(job, monkeypatch, market_closed=False) == {101}

  executor.set_strategy_magic_map({"strat-a": 101, "strat-b": 202})
  assert _run_once(job, monkeypatch, market_closed=False) == {101, 202}


def test_a_plain_set_is_still_accepted_and_frozen(monkeypatch):
  magics = {101}
  job = _job(magics, connected=True)
  magics.add(202)  # mutating the caller's set must not leak into the job
  assert _run_once(job, monkeypatch, market_closed=False) == {101}


def test_empty_magic_map_warns_once(monkeypatch):
  # No magics means no ticket is ever tracked and the job is a silent no-op —
  # exactly the state a dropped strategy_magic_map leaves the worker in.
  # (The app logger does not propagate to root, so caplog cannot see it.)
  warnings: list = []
  monkeypatch.setattr(
    mt5_event_job.log, "warning", lambda msg, *a: warnings.append(msg % a if a else msg)
  )
  job = _job(set(), connected=True)

  assert _run_once(job, monkeypatch, market_closed=False) == set()
  assert _run_once(job, monkeypatch, market_closed=False) == set()

  assert len([w for w in warnings if "No owned magics" in w]) == 1


def test_warning_rearms_after_magics_come_back(monkeypatch):
  magics: set = set()
  job = _job(lambda: magics, connected=True)

  _run_once(job, monkeypatch, market_closed=False)
  assert job._warned_no_magics is True

  magics.add(101)
  _run_once(job, monkeypatch, market_closed=False)
  assert job._warned_no_magics is False


# ── loop resilience ─────────────────────────────────────────────────────────── #


def test_scan_error_does_not_kill_the_loop(monkeypatch):
  job = _job({101}, connected=True)
  monkeypatch.setattr(mt5_event_job, "is_market_closed", lambda: False)

  def _boom(magics, tickets):
    raise RuntimeError("history unavailable")

  monkeypatch.setattr(mt5_event_job, "scan_terminal_closed_positions", _boom)
  job._stop_event = _OneShotEvent()
  job._run()  # swallowed and retried next interval, not propagated


def test_one_failing_event_does_not_drop_the_rest_of_the_batch(monkeypatch):
  # scan_terminal_closed_positions has already advanced seen_tickets past the
  # whole batch, so an exception escaping the per-event loop would lose every
  # remaining close permanently — their tickets never look "disappeared" again.
  handled = []
  monkeypatch.setattr(mt5_event_job, "is_market_closed", lambda: False)
  monkeypatch.setattr(
    mt5_event_job,
    "scan_terminal_closed_positions",
    lambda magics, tickets: [_event("111"), _event("222"), _event("333")],
  )

  def _handle(event):
    if event.source_ticket == "111":
      raise RuntimeError("notification failed")
    handled.append(event.source_ticket)

  job = _job({101}, connected=True)
  monkeypatch.setattr(job, "_handle", _handle)
  job._stop_event = _OneShotEvent()
  job._run()

  assert handled == ["222", "333"]


# ── notification rendering ──────────────────────────────────────────────────── #


def test_missing_sl_tp_still_notifies_and_closes_the_row():
  # history_orders_get can return no opening order (pruned broker history, or a
  # position opened outside the worker), leaving sl/tp None. round(None, 2) used
  # to raise *after* the DB write, costing the notification and the rest of the
  # batch.
  db = _FakeDb(position={"strategy": "strat-a"})
  notifier = _Notifier()
  job = _job({101}, db=db, notifier=notifier)

  job._handle(_event("555", sl=None, tp=None))

  assert db.statuses == [("555", mt5_event_job.PositionStatusEnum.TERMINAL_CLOSED)]
  assert len(notifier.sent) == 1
  assert "SL: <b>—</b>" in notifier.sent[0]
  assert "TP: <b>—</b>" in notifier.sent[0]
  assert "Close Price: <b>1920.12</b>" in notifier.sent[0]


def test_present_sl_tp_are_rounded():
  db = _FakeDb(position={"strategy": "strat-a"})
  notifier = _Notifier()
  job = _job({101}, db=db, notifier=notifier)

  job._handle(_event("555"))

  assert "SL: <b>1890.0</b>" in notifier.sent[0]
  assert "TP: <b>1950.0</b>" in notifier.sent[0]


def test_unknown_position_is_skipped_without_writing():
  db = _FakeDb(position=None)
  notifier = _Notifier()
  job = _job({101}, db=db, notifier=notifier)

  job._handle(_event("999"))

  assert db.statuses == [] and notifier.sent == []
