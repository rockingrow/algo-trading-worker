"""Tests for the MT5 deal-history close detector.

The detector's whole job is deciding, for a position that vanished from
``positions_get()``, whether the *terminal* closed it (SL / TP / stop-out /
manual → report it) or our own ``order_send`` did (``DEAL_REASON_EXPERT`` → stay
quiet so the pipeline never double-fires). These pin that decision against the
deal shapes a real position actually produces — in particular one with several
closing deals, where picking the wrong one silently drops a genuine close.
"""

from types import SimpleNamespace

import MetaTrader5 as _mt5
import pytest

from worker.gateways.forex.mt5 import close_detector
from worker.gateways.forex.mt5.close_detector import (
  TerminalCloseReason,
  scan_terminal_closed_positions,
)


def _deal(
  ticket,
  entry,
  reason,
  *,
  time=0,
  price=1900.0,
  volume=0.01,
  profit=0.0,
  commission=0.0,
  swap=0.0,
  fee=0.0,
):
  return SimpleNamespace(
    ticket=ticket,
    entry=entry,
    reason=reason,
    time=time,
    price=price,
    volume=volume,
    symbol="XAUUSDm",
    profit=profit,
    commission=commission,
    swap=swap,
    fee=fee,
  )


class _FakeMt5:
  """Real MT5 constants over scripted position/history lookups."""

  # Constants are copied off the real module so the lazily-built lookups in the
  # detector map exactly as they do in production.
  DEAL_ENTRY_IN = _mt5.DEAL_ENTRY_IN
  DEAL_ENTRY_OUT = _mt5.DEAL_ENTRY_OUT
  DEAL_ENTRY_OUT_BY = _mt5.DEAL_ENTRY_OUT_BY
  DEAL_REASON_SL = _mt5.DEAL_REASON_SL
  DEAL_REASON_TP = _mt5.DEAL_REASON_TP
  DEAL_REASON_SO = _mt5.DEAL_REASON_SO
  DEAL_REASON_CLIENT = _mt5.DEAL_REASON_CLIENT
  DEAL_REASON_MOBILE = _mt5.DEAL_REASON_MOBILE
  DEAL_REASON_WEB = _mt5.DEAL_REASON_WEB
  DEAL_REASON_EXPERT = _mt5.DEAL_REASON_EXPERT
  ORDER_TYPE_BUY = _mt5.ORDER_TYPE_BUY
  ORDER_TYPE_SELL = _mt5.ORDER_TYPE_SELL

  def __init__(self, positions=(), deals=()):
    self._positions = list(positions)
    self._deals = list(deals)

  def positions_get(self):
    return self._positions

  def history_deals_get(self, position=None):
    return list(self._deals)

  def history_orders_get(self, position=None):
    return [
      SimpleNamespace(type=self.ORDER_TYPE_BUY, sl=1890.0, tp=1950.0),
    ]

  def account_info(self):
    return None


@pytest.fixture
def fake_mt5(monkeypatch):
  """Swap the detector's mt5 module and reset its lazily-built lookups."""
  fake = _FakeMt5()
  monkeypatch.setattr(close_detector, "mt5", fake)
  monkeypatch.setattr(close_detector, "_REASON_MAP", {})
  monkeypatch.setattr(close_detector, "_CLOSING_ENTRIES", ())
  return fake


def _detect(fake, ticket=555):
  """Run one scan for a position already known (so it counts as disappeared)."""
  return scan_terminal_closed_positions(magic_numbers={101}, seen_tickets={ticket})


# ── closing-deal selection ──────────────────────────────────────────────────── #


def test_manual_close_after_our_tp1_partial_is_still_detected(fake_mt5):
  # The reported bug: the position had TWO closing deals — our own TP1 partial
  # (EXPERT) and then a manual close. Taking the *first* OUT deal let the EXPERT
  # partial mask the manual close: its reason maps to nothing, so the event was
  # discarded and the close was never reported.
  fake_mt5._deals = [
    _deal(1, fake_mt5.DEAL_ENTRY_IN, fake_mt5.DEAL_REASON_EXPERT, time=100),
    _deal(2, fake_mt5.DEAL_ENTRY_OUT, fake_mt5.DEAL_REASON_EXPERT, time=200),
    _deal(3, fake_mt5.DEAL_ENTRY_OUT, fake_mt5.DEAL_REASON_CLIENT, time=300),
  ]
  events = _detect(fake_mt5)

  assert len(events) == 1
  assert events[0].close_reason is TerminalCloseReason.MANUAL
  assert events[0].deal_ticket == "3"


def test_newest_closing_deal_wins_even_when_history_is_unordered(fake_mt5):
  # Ordering of history_deals_get() is not assumed — the newest OUT deal is
  # selected explicitly.
  fake_mt5._deals = [
    _deal(3, fake_mt5.DEAL_ENTRY_OUT, fake_mt5.DEAL_REASON_SL, time=300),
    _deal(1, fake_mt5.DEAL_ENTRY_IN, fake_mt5.DEAL_REASON_EXPERT, time=100),
    _deal(2, fake_mt5.DEAL_ENTRY_OUT, fake_mt5.DEAL_REASON_EXPERT, time=200),
  ]
  events = _detect(fake_mt5)

  assert len(events) == 1 and events[0].close_reason is TerminalCloseReason.SL


def test_close_by_deal_counts_as_a_closing_deal(fake_mt5):
  # A position closed *against* an opposite one produces DEAL_ENTRY_OUT_BY.
  fake_mt5._deals = [
    _deal(1, fake_mt5.DEAL_ENTRY_IN, fake_mt5.DEAL_REASON_EXPERT, time=100),
    _deal(2, fake_mt5.DEAL_ENTRY_OUT_BY, fake_mt5.DEAL_REASON_CLIENT, time=200),
  ]
  events = _detect(fake_mt5)

  assert len(events) == 1 and events[0].close_reason is TerminalCloseReason.MANUAL


# ── reason mapping ──────────────────────────────────────────────────────────── #


@pytest.mark.parametrize(
  "reason_attr,expected",
  [
    ("DEAL_REASON_SL", TerminalCloseReason.SL),
    ("DEAL_REASON_TP", TerminalCloseReason.TP),
    ("DEAL_REASON_SO", TerminalCloseReason.STOP_OUT),
    ("DEAL_REASON_CLIENT", TerminalCloseReason.MANUAL),
    ("DEAL_REASON_MOBILE", TerminalCloseReason.MANUAL),
    ("DEAL_REASON_WEB", TerminalCloseReason.MANUAL),
  ],
)
def test_terminal_initiated_reasons_are_reported(fake_mt5, reason_attr, expected):
  fake_mt5._deals = [
    _deal(1, fake_mt5.DEAL_ENTRY_IN, fake_mt5.DEAL_REASON_EXPERT, time=100),
    _deal(2, fake_mt5.DEAL_ENTRY_OUT, getattr(fake_mt5, reason_attr), time=200),
  ]
  events = _detect(fake_mt5)

  assert len(events) == 1 and events[0].close_reason is expected


def test_our_own_close_is_never_reported(fake_mt5):
  # DEAL_REASON_EXPERT is our order_send — the signal pipeline already handled it
  # and reporting it again would double-fire. (ForexReconcileJob is the backstop
  # for the case where that close flattened the position without us noticing.)
  fake_mt5._deals = [
    _deal(1, fake_mt5.DEAL_ENTRY_IN, fake_mt5.DEAL_REASON_EXPERT, time=100),
    _deal(2, fake_mt5.DEAL_ENTRY_OUT, fake_mt5.DEAL_REASON_EXPERT, time=200),
  ]
  assert _detect(fake_mt5) == []


# ── scan-level safety ───────────────────────────────────────────────────────── #


def test_offline_terminal_leaves_seen_tickets_untouched(fake_mt5, monkeypatch):
  # positions_get() → None means "no data", never "flat account".
  monkeypatch.setattr(fake_mt5, "positions_get", lambda: None)
  seen = {555}
  assert scan_terminal_closed_positions({101}, seen) == []
  assert seen == {555}  # not cleared → no false disappearance next scan


def test_seen_tickets_tracks_only_our_magics(fake_mt5):
  fake_mt5._positions = [
    SimpleNamespace(ticket=777, magic=101),
    SimpleNamespace(ticket=888, magic=999),  # another EA on the same account
  ]
  seen = set()
  scan_terminal_closed_positions({101}, seen)
  assert seen == {777}


# ── realized PnL ────────────────────────────────────────────────────────────── #


def test_event_carries_what_the_closing_deal_booked(fake_mt5):
  # The deal that flattened the position is also where MT5 booked the money, so
  # the terminal-close notification gets its PnL from the same history read that
  # detected the close — no second query.
  fake_mt5._deals = [
    _deal(1, fake_mt5.DEAL_ENTRY_IN, fake_mt5.DEAL_REASON_CLIENT, time=100),
    _deal(
      2,
      fake_mt5.DEAL_ENTRY_OUT,
      fake_mt5.DEAL_REASON_SL,
      time=200,
      profit=-14.0,
      commission=-0.7,
      swap=-0.3,
    ),
  ]
  events = _detect(fake_mt5)

  assert len(events) == 1
  assert events[0].profit == pytest.approx(-15.0)


def test_pnl_covers_only_the_close_that_flattened_the_position(fake_mt5):
  # An earlier TP1 partial booked its own result and was reported at the time;
  # counting it again here would double-report it.
  fake_mt5._deals = [
    _deal(1, fake_mt5.DEAL_ENTRY_IN, fake_mt5.DEAL_REASON_CLIENT, time=100),
    _deal(
      2, fake_mt5.DEAL_ENTRY_OUT, fake_mt5.DEAL_REASON_EXPERT, time=200, profit=9.0
    ),
    _deal(
      3, fake_mt5.DEAL_ENTRY_OUT, fake_mt5.DEAL_REASON_CLIENT, time=300, profit=4.0
    ),
  ]
  events = _detect(fake_mt5)

  assert len(events) == 1 and events[0].profit == pytest.approx(4.0)
