from types import SimpleNamespace

import pytest
from helpers import make_signal

from worker.gateways.market_strategy import BaseMarketStrategy
from worker.gateways.signal_handler import SignalHandler
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalActionEnum


class FakeStrategy(BaseMarketStrategy):
  def __init__(
    self,
    open_positions=None,
    entry_ok=True,
    cleanup_ok=True,
    multi_strategy=False,
    multi_positions=False,
    cleanup_profit=None,
  ):
    self.calls = []
    self._open = open_positions if open_positions is not None else []
    self._entry_ok = entry_ok
    self._cleanup_ok = cleanup_ok
    self._multi_strategy = multi_strategy
    self._multi_positions = multi_positions
    self._cleanup_profit = cleanup_profit

  @property
  def allows_multi_strategy_per_symbol(self):
    return self._multi_strategy

  def entry(self, signal):
    self.calls.append("entry")
    return {
      "success": self._entry_ok,
      "retcode": 0,
      "ticket": 1,
      "volume": 1,
      "price": 2,
    }

  @property
  def allows_multi_positions_per_symbol(self):
    return self._multi_positions

  def handle_tp1(self, signal, position_ticket=None):
    self.calls.append(f"tp1:{position_ticket}" if position_ticket else "tp1")
    return {"success": True, "retcode": 0, "volume": 1, "price": 2}

  def handle_full_close(self, signal, position_ticket=None):
    self.calls.append(
      f"full_close:{position_ticket}" if position_ticket else "full_close"
    )
    return {"success": True, "retcode": 0, "volume": 1, "price": 2}

  def get_open_positions(self, symbol, strategy=None, position_ticket=None):
    self.calls.append(f"get_open:{strategy}")
    positions = list(self._open)
    if position_ticket is not None:
      positions = [
        p for p in positions if str(getattr(p, "ticket", None)) == str(position_ticket)
      ]
    return positions

  def close_all_positions(
    self, symbol, reason="CLOSE", strategy=None, position_ticket=None
  ):
    self.calls.append(
      f"close_all:{reason}:{strategy}:{position_ticket}"
      if position_ticket
      else f"close_all:{reason}:{strategy}"
    )
    return {
      "success": self._cleanup_ok,
      "retcode": 0,
      "price": 1999.0,
      "profit": self._cleanup_profit,
    }


class FakeStore:
  def __init__(self, positions=None, flat_positions=None):
    self._positions = positions if positions is not None else []
    self._flat = flat_positions if flat_positions is not None else []
    self.status_updates = []

  def get_open_positions_by_strategy(self, strategy, symbol, signal_uxid=None):
    rows = list(self._positions)
    if signal_uxid is not None:
      rows = [r for r in rows if r.get("signal_uxid") == signal_uxid]
    return rows

  def get_open_positions_for_flat(self, strategy=None, symbol=None):
    return list(self._flat)

  def update_position_status(self, **kwargs):
    self.status_updates.append(kwargs)


@pytest.mark.parametrize(
  "action,expected",
  [
    (SignalActionEnum.LONG, "entry"),
    (SignalActionEnum.SHORT, "entry"),
    (SignalActionEnum.TP1, "tp1"),
    (SignalActionEnum.TP2, "full_close"),
    (SignalActionEnum.SL, "full_close"),
    (SignalActionEnum.R_SL, "full_close"),
    (SignalActionEnum.FLAT, "close_all:FLAT:strat-1"),
  ],
)
def test_dispatch_routes_every_action(action, expected):
  open_pos = [SimpleNamespace(ticket=9)]
  strat = FakeStrategy(open_positions=open_pos)
  store = FakeStore(
    positions=[{"ref_source_id": "9", "ref_id": "9", "status": "OPENED"}]
  )
  handler = SignalHandler(strat, store)
  handler.handle(make_signal(action))
  assert strat.calls[-1] == expected


def test_flat_routes_through_handler():
  """Regression: FLAT must go through the handler, not a special pre-branch."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(
    positions=[{"ref_source_id": "9", "ref_id": "9", "status": "OPENED"}]
  )
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.FLAT))
  assert res["success"] is True
  assert res["source_ticket"] == "9"


def test_flat_closes_mt5_when_no_db_record():
  """FLAT must close MT5 positions even when the DB has no tracked record."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=99)])
  store = FakeStore(positions=[])  # empty DB
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.FLAT))
  assert res["success"] is True
  assert "close_all:FLAT:strat-1" in strat.calls


def test_flat_syncs_stale_db_record_when_no_mt5_positions():
  """FLAT must mark stale DB records as FLATTED when MT5 has no open positions."""
  strat = FakeStrategy(open_positions=[], cleanup_ok=False)
  store = FakeStore(
    positions=[{"ref_source_id": "7", "ref_id": "7", "status": "OPENED"}]
  )
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.FLAT))
  assert res["success"] is False
  assert store.status_updates[0]["ref_source_id"] == "7"
  assert store.status_updates[0]["status"] == PositionStatusEnum.FLATTED


def test_entry_no_stale_position():
  strat = FakeStrategy(open_positions=[])
  store = FakeStore()
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.LONG))
  assert res["success"] is True
  assert "close_all:STALE_CLEANUP" not in strat.calls


def test_entry_force_closes_stale_and_marks_db():
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(positions=[{"ref_source_id": "9", "ref_id": "9", "volume": 1.0}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.LONG))
  assert "close_all:STALE_CLEANUP:strat-1" in strat.calls
  assert store.status_updates[0]["status"] == PositionStatusEnum.FORCED_CLOSED
  assert res["forced_closed"][0]["ref_source_id"] == "9"


def test_entry_clears_orphaned_db_row_when_broker_has_no_position():
  """Regression: a prior position closed externally (SL/liquidation) leaves an
  OPENED DB row the broker no longer reports. The new entry must still mark it
  FORCED_CLOSED, otherwise insert_position collides on the one-active-per-
  (strategy,symbol) unique index and the new trade goes untracked."""
  strat = FakeStrategy(open_positions=[])  # broker reports nothing live
  store = FakeStore(positions=[{"ref_source_id": "9", "ref_id": "9", "volume": 1.0}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.LONG))
  assert res["success"] is True
  assert "entry" in strat.calls
  assert "close_all:STALE_CLEANUP:strat-1" not in strat.calls  # nothing to close
  assert store.status_updates[0]["status"] == PositionStatusEnum.FORCED_CLOSED
  assert res["forced_closed"][0]["ref_source_id"] == "9"


def test_entry_scopes_preflight_to_signal_strategy():
  """Stale check + force-close must be scoped to the signal's strategy so a
  concurrent strategy on the same symbol is never touched."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(positions=[{"ref_source_id": "9", "ref_id": "9", "volume": 1.0}])
  handler = SignalHandler(strat, store)
  handler.handle(make_signal(SignalActionEnum.SHORT, strategy="strat-short"))
  assert "get_open:strat-short" in strat.calls
  assert "close_all:STALE_CLEANUP:strat-short" in strat.calls


def test_entry_rejected_when_other_strategy_holds_symbol():
  """Entry must be rejected — and no orders touched — when another strategy
  already holds the same symbol. cancel_all_orders is symbol-scoped, so even
  a stale-cleanup call would silently wipe the other strategy's SL/TP."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(
    flat_positions=[{"strategy": "strat-A", "ref_source_id": "9", "ref_id": "9"}],
  )
  handler = SignalHandler(strat, store)
  res = handler.handle(
    make_signal(SignalActionEnum.LONG, strategy="strat-B", symbol="BTCUSD")
  )
  assert res["success"] is False
  assert "strat-A" in res["comment"]
  # No exchange calls — stale cleanup must never run when a conflict exists.
  assert not any("close_all" in c for c in strat.calls)
  assert "entry" not in strat.calls


def test_entry_allowed_alongside_other_strategy_when_multi_strategy_enabled():
  """With multi-strategy-per-symbol on (FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL),
  a second strategy may open its own position on a symbol another strategy
  already holds — each strategy keeps its own isolated order."""
  strat = FakeStrategy(open_positions=[], multi_strategy=True)
  store = FakeStore(
    flat_positions=[{"strategy": "strat-A", "ref_source_id": "9", "ref_id": "9"}],
  )
  handler = SignalHandler(strat, store)
  res = handler.handle(
    make_signal(SignalActionEnum.LONG, strategy="strat-B", symbol="XAUUSD")
  )
  assert res["success"] is True
  assert "entry" in strat.calls
  # The other strategy's position must not be touched: every close is scoped to
  # the entering strategy.
  assert not any(c.startswith("close_all") and c.endswith(":None") for c in strat.calls)


def test_entry_still_replaces_own_position_when_multi_strategy_enabled():
  """The toggle relaxes the *cross-strategy* rule only — a strategy re-entering
  its own symbol still replaces its position instead of stacking a second one."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)], multi_strategy=True)
  store = FakeStore(
    positions=[{"ref_source_id": "9", "ref_id": "9", "volume": 1.0}],
    flat_positions=[{"strategy": "strat-A", "ref_source_id": "1", "ref_id": "1"}],
  )
  handler = SignalHandler(strat, store)
  res = handler.handle(
    make_signal(SignalActionEnum.LONG, strategy="strat-B", symbol="XAUUSD")
  )
  assert res["success"] is True
  assert "close_all:STALE_CLEANUP:strat-B" in strat.calls
  assert store.status_updates[0]["status"] == PositionStatusEnum.FORCED_CLOSED


def test_flat_falls_back_to_unscoped_close_when_multi_strategy_disabled():
  """Default behaviour: a strategy-scoped FLAT that finds nothing retries
  unscoped, so a position missing from the DB is still closed."""
  strat = FakeStrategy(open_positions=[], cleanup_ok=False)
  store = FakeStore(positions=[])
  handler = SignalHandler(strat, store)
  handler.handle(make_signal(SignalActionEnum.FLAT, strategy="strat-1"))
  assert "close_all:FLAT:strat-1" in strat.calls
  assert "close_all:FLAT:None" in strat.calls


def test_flat_skips_unscoped_close_when_multi_strategy_enabled():
  """With several strategies sharing a symbol, the unscoped FLAT retry would
  flatten the other strategies' live positions — it must be skipped."""
  strat = FakeStrategy(open_positions=[], cleanup_ok=False, multi_strategy=True)
  store = FakeStore(positions=[])
  handler = SignalHandler(strat, store)
  handler.handle(make_signal(SignalActionEnum.FLAT, strategy="strat-1"))
  assert "close_all:FLAT:strat-1" in strat.calls
  assert "close_all:FLAT:None" not in strat.calls


def test_entry_aborts_when_cleanup_fails():
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)], cleanup_ok=False)
  store = FakeStore(positions=[{"ref_source_id": "9", "ref_id": "9"}])
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.LONG))
  assert res["success"] is False
  assert "entry" not in strat.calls  # never reached entry


def test_exit_returns_failure_when_no_db_record():
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)])
  store = FakeStore(positions=[])  # nothing tracked
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.TP2))
  assert res["success"] is False
  assert "full_close" not in strat.calls


def test_exit_returns_failure_when_no_live_mt5_position():
  strat = FakeStrategy(open_positions=[])  # gone from MT5
  store = FakeStore(
    positions=[{"ref_source_id": "9", "ref_id": "9", "status": "OPENED"}]
  )
  handler = SignalHandler(strat, store)
  res = handler.handle(make_signal(SignalActionEnum.SL))
  assert res["success"] is False
  assert "full_close" not in strat.calls


def test_get_db_position_heals_duplicate_active_rows():
  """If the DB has > 1 OPENED/TP1 row for the same strategy+symbol, the handler
  must keep the oldest and immediately mark the rest FORCED_CLOSED."""
  dup_rows = [
    {"ref_source_id": "10", "ref_id": "10", "status": "OPENED"},
    {"ref_source_id": "11", "ref_id": "11", "status": "OPENED"},  # duplicate
    {"ref_source_id": "12", "ref_id": "12", "status": "TP1"},  # duplicate
  ]
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=10)])
  store = FakeStore(positions=dup_rows)
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.TP2))

  # Only ref_source_id="10" (oldest) is used; "11" and "12" are healed.
  assert res.get("source_ticket") == "10"
  healed_tickets = {u["ref_source_id"] for u in store.status_updates}
  assert healed_tickets == {"11", "12"}
  assert all(
    u["status"] == PositionStatusEnum.FORCED_CLOSED
    for u in store.status_updates
    if u["ref_source_id"] in {"11", "12"}
  )


def test_force_closed_entry_carries_what_the_cleanup_booked():
  # Flattening a stale position is a close like any other, and its "Force Closed"
  # notification reads the amount from here.
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=9)], cleanup_profit=-4.5)
  store = FakeStore(positions=[{"ref_source_id": "9", "ref_id": "9", "volume": 1.0}])

  res = SignalHandler(strat, store).handle(make_signal(SignalActionEnum.LONG))

  assert res["forced_closed"][0]["profit"] == -4.5


def test_orphaned_db_row_reports_no_pnl():
  # Nothing was live on the broker, so nothing was closed and nothing was booked —
  # the row is only being cleared. "n/a" is the honest reading, not 0.00.
  strat = FakeStrategy(open_positions=[], cleanup_profit=99.0)
  store = FakeStore(positions=[{"ref_source_id": "9", "ref_id": "9", "volume": 1.0}])

  res = SignalHandler(strat, store).handle(make_signal(SignalActionEnum.LONG))

  assert res["forced_closed"][0]["profit"] is None


# ── Multiple positions per symbol (symbol + strategy + signal_uxid) ───────── #
#
# With FOREX_ALLOW_MULTI_POSITIONS_PER_SYMBOL the market reports
# allows_multi_positions_per_symbol, and every step the handler takes is scoped
# to the ticket tracked for the signal's own signal_uxid.


def _row(ref_source_id, signal_uxid, status="OPENED"):
  return {
    "ref_source_id": ref_source_id,
    "ref_id": ref_source_id,
    "status": status,
    "signal_uxid": signal_uxid,
  }


def test_exit_targets_the_ticket_tracked_for_this_signal_uxid():
  strat = FakeStrategy(
    open_positions=[SimpleNamespace(ticket=11), SimpleNamespace(ticket=22)],
    multi_positions=True,
  )
  store = FakeStore(positions=[_row("11", "uxid-A"), _row("22", "uxid-B")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.TP2, signal_uxid="uxid-B"))

  assert res["success"] is True
  # The sibling on ticket 11 is never named.
  assert "full_close:22" in strat.calls
  assert res["source_ticket"] == "22"


def test_exit_for_an_unknown_uxid_does_not_touch_a_sibling_position():
  strat = FakeStrategy(
    open_positions=[SimpleNamespace(ticket=11)], multi_positions=True
  )
  store = FakeStore(positions=[_row("11", "uxid-A")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.SL, signal_uxid="uxid-ZZZ"))

  assert res["success"] is False
  assert not any(c.startswith("full_close") for c in strat.calls)


def test_entry_leaves_a_sibling_signals_position_running():
  """A new signal on a symbol the strategy already holds opens alongside it —
  the stale-cleanup must not flatten the position another signal owns."""
  strat = FakeStrategy(
    open_positions=[SimpleNamespace(ticket=11)], multi_positions=True
  )
  store = FakeStore(positions=[_row("11", "uxid-A")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.LONG, signal_uxid="uxid-B"))

  assert res["success"] is True
  assert not any(c.startswith("close_all") for c in strat.calls)
  assert store.status_updates == []
  assert "forced_closed" not in res


def test_entry_still_replaces_the_position_of_the_same_signal_uxid():
  """Re-sending one signal replaces its own position, scoped to its ticket."""
  strat = FakeStrategy(
    open_positions=[SimpleNamespace(ticket=11), SimpleNamespace(ticket=22)],
    multi_positions=True,
  )
  store = FakeStore(positions=[_row("11", "uxid-A"), _row("22", "uxid-B")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.LONG, signal_uxid="uxid-A"))

  assert res["success"] is True
  assert "close_all:STALE_CLEANUP:strat-1:11" in strat.calls
  # Only uxid-A's row was reconciled; uxid-B's is untouched.
  assert [u["ref_source_id"] for u in store.status_updates] == ["11"]


def test_flat_closes_only_the_position_of_the_signal_that_sent_it():
  strat = FakeStrategy(
    open_positions=[SimpleNamespace(ticket=11), SimpleNamespace(ticket=22)],
    multi_positions=True,
  )
  store = FakeStore(positions=[_row("11", "uxid-A"), _row("22", "uxid-B")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.FLAT, signal_uxid="uxid-A"))

  assert res["success"] is True
  assert "close_all:FLAT:strat-1:11" in strat.calls
  assert res["source_ticket"] == "11"


def test_uxid_is_ignored_when_the_market_does_not_allow_multi_positions():
  """The toggle off keeps the (strategy, symbol) key, uxid or no uxid."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=11)])
  store = FakeStore(positions=[_row("11", "uxid-A")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.TP2, signal_uxid="uxid-B"))

  assert res["success"] is True
  assert "full_close" in strat.calls


def test_flat_for_an_unknown_uxid_closes_nothing():
  """Regression: the DB-out-of-sync fallback must not apply here. A FLAT whose
  signal owns no tracked row would otherwise close the strategy's whole book on
  the symbol — flattening every sibling signal's live position."""
  strat = FakeStrategy(
    open_positions=[SimpleNamespace(ticket=11), SimpleNamespace(ticket=22)],
    multi_positions=True,
  )
  store = FakeStore(positions=[_row("11", "uxid-A"), _row("22", "uxid-B")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.FLAT, signal_uxid="uxid-GONE"))

  assert res["success"] is False
  assert not any(c.startswith("close_all") for c in strat.calls)
  assert store.status_updates == []


def test_flat_without_multi_positions_keeps_the_out_of_sync_fallback():
  """The fallback still exists where it is safe: one position per key, so a
  strategy-scoped close can only reach the position the FLAT is about."""
  strat = FakeStrategy(open_positions=[SimpleNamespace(ticket=11)])
  store = FakeStore(positions=[])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.FLAT))

  assert res["success"] is True
  assert "close_all:FLAT:strat-1" in strat.calls


def test_entry_cleanup_closes_every_duplicate_row_of_one_uxid():
  """Two active rows for one uxid (a crash artifact the DB self-heals): closing
  only the first would leave the second live on the broker but FORCED_CLOSED in
  the DB — a live untracked position."""
  strat = FakeStrategy(
    open_positions=[SimpleNamespace(ticket=11), SimpleNamespace(ticket=22)],
    multi_positions=True,
  )
  store = FakeStore(positions=[_row("11", "uxid-A"), _row("22", "uxid-A")])
  handler = SignalHandler(strat, store)

  res = handler.handle(make_signal(SignalActionEnum.LONG, signal_uxid="uxid-A"))

  assert res["success"] is True
  assert "close_all:STALE_CLEANUP:strat-1:11" in strat.calls
  assert "close_all:STALE_CLEANUP:strat-1:22" in strat.calls
  assert sorted(u["ref_source_id"] for u in store.status_updates) == ["11", "22"]
