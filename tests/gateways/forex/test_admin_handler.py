"""
Tests for the *real* MT5 ADMIN FLAT match-key hooks
(``ForexSignalProcessor._flat_match_key`` / ``_flat_db_match_keys``).

The broker-agnostic FLAT skeleton itself is covered by
``tests/gateways/test_admin_flat.py``, but that suite *fakes* the per-market
match-key hooks. The bug those fakes could not see: a live MT5 ticket is an
``int`` while ``ref_id`` / ``ref_source_id`` are stored as ``str`` throughout the
app layer, so a *successfully closed* position never matched its DB row and was
mis-reconciled as "position not found on MT5". These tests pin the real hooks
(and one end-to-end reconcile) against that int↔str seam.
"""

import json
from types import SimpleNamespace

from worker.gateways.forex.signal_processor import ForexSignalProcessor
from worker.gateways.processor import BaseSignalProcessor
from worker.schemas.position_schema import PositionStatusEnum

# ── the real hooks: a live int ticket must match a str DB ref ──────────────── #


def _match_key(ticket):
  # The hooks don't touch self, so call them unbound with self=None.
  return ForexSignalProcessor._flat_match_key(None, SimpleNamespace(ticket=ticket))


def _db_keys(ref_id, ref_source_id):
  return ForexSignalProcessor._flat_db_match_keys(
    None, {"ref_id": ref_id, "ref_source_id": ref_source_id}
  )


def test_live_int_ticket_matches_str_db_ref():
  # Regression: int 4587420656 (live) vs str "4587420656" (DB) must compare equal.
  assert _match_key(4587420656) in _db_keys("4587420656", "4587420656")


def test_match_key_is_str():
  assert _match_key(4587420656) == "4587420656"


def test_db_keys_match_either_ref():
  # Re-ticketed after a partial close: live ticket equals ref_source_id only.
  assert _match_key(222) in _db_keys(ref_id="111", ref_source_id="222")


def test_db_keys_drops_none():
  keys = _db_keys(ref_id="111", ref_source_id=None)
  assert keys == {"111"}
  assert None not in keys


# ── end-to-end: a successful close reconciles, not "not found" ─────────────── #


class _FakeDb:
  def __init__(self, flat_positions):
    self.status_updates = []
    self._flat = list(flat_positions)

  def get_open_positions_for_flat(self, strategy=None, symbol=None):
    return list(self._flat)

  def update_position_status(self, **kwargs):
    self.status_updates.append(kwargs)


class _FakeExecutor:
  def __init__(self, positions, close_result):
    self._positions = list(positions)
    self._close_result = close_result
    self.closed = []

  def get_all_open_positions(self, strategy=None):
    return list(self._positions)

  def get_open_positions(self, symbol, strategy=None):
    return list(self._positions)

  def close_single_position(self, pos, reason="FLAT"):
    self.closed.append(pos)
    return dict(self._close_result)


class _FakeProc:
  """Drives the base FLAT handler through the REAL MT5 match-key hooks."""

  # Real per-market hooks — this is the seam the shared suite fakes.
  _flat_match_key = ForexSignalProcessor._flat_match_key
  _flat_db_match_keys = ForexSignalProcessor._flat_db_match_keys
  # Real broker-agnostic skeleton.
  _handle_admin_message = BaseSignalProcessor._handle_admin_message
  _close_live_positions_for_flat = BaseSignalProcessor._close_live_positions_for_flat
  _reconcile_flat_db = BaseSignalProcessor._reconcile_flat_db

  def __init__(self, db_positions, positions, close_result):
    self.name = "FOREX"
    self.presenter = SimpleNamespace(
      admin_flat_closed=lambda db_pos, result, footer: f"Admin FLAT {db_pos['symbol']}"
    )
    self._account_id = "100"
    self.executor = _FakeExecutor(positions, close_result)
    self.db = _FakeDb(db_positions)
    self.notifications = []
    self.ctx = SimpleNamespace(
      db_service=self.db,
      channel_notifier=SimpleNamespace(
        send_message=lambda m: self.notifications.append(m)
      ),
    )

  def _ensure_connected(self):
    return True

  def _account_footer(self):
    return "FOOTER"


def _payload(**fields):
  data = {"action": "FLAT", "timestamp": "2026-06-02T08:00:00+00:00"}
  data.update(fields)
  return json.dumps(data)


def test_closed_int_ticket_reconciles_str_db_row():
  """Reproduces the production log: live ticket is int, DB ref is str. The close
  succeeded, so the row must be marked FLATTED with the close result — NOT routed
  to the 'position not found on MT5' branch."""
  proc = _FakeProc(
    db_positions=[{
      "ref_id": "4587420656", "ref_source_id": "4587420656",
      "strategy": "MT5_BTC_M5_V1", "symbol": "BTCUSD", "status": "OPENED",
    }],
    positions=[SimpleNamespace(ticket=4587420656, symbol="BTCUSD", volume=0.01)],
    close_result={
      "success": True, "retcode": 10009, "ticket": "4587420656",
      "price": 64000.0, "volume": 0.01, "comment": "Closed [FLAT]",
    },
  )

  proc._handle_admin_message(_payload(account_id="100"))

  assert len(proc.executor.closed) == 1
  assert len(proc.db.status_updates) == 1
  upd = proc.db.status_updates[0]
  assert upd["status"] == PositionStatusEnum.FLATTED
  assert upd["closed_price"] == 64000.0           # set only on the matched branch
  assert upd["comment"] == "Closed [FLAT]"        # NOT "position not found on MT5"
  assert len(proc.notifications) == 1             # close notification was sent
