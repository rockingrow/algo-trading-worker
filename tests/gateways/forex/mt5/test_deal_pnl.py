"""Tests for the MT5 deal → realized-PnL arithmetic.

This is what every FOREX close notification's PnL line is built from, so the
properties pinned here are the ones an operator reads off the message: the
costs MT5 books separately from the price result are folded in, and an amount
that could not be read stays *unknown* rather than becoming a break-even 0.00.

Unlike its callers, this module imports no MetaTrader5 extension, so these run
on every platform.
"""

from types import SimpleNamespace

import pytest

from worker.gateways.forex.mt5.deal_pnl import deal_realized_pnl, deals_realized_pnl


def _deal(**fields):
  return SimpleNamespace(**fields)


def test_deal_pnl_is_the_price_result_net_of_its_costs():
  # MT5 books these on four separate fields; the account balance moved by their
  # sum, which is the only figure worth showing.
  deal = _deal(profit=12.50, commission=-0.70, swap=-0.30, fee=-0.05)
  assert deal_realized_pnl(deal) == 11.45


def test_deal_pnl_of_a_plain_profit():
  assert deal_realized_pnl(_deal(profit=8.0, commission=0.0, swap=0.0, fee=0.0)) == 8.0


def test_deal_pnl_treats_absent_cost_fields_as_no_charge():
  # Not every broker/terminal populates all four; a missing one must not raise
  # inside a notification path.
  assert deal_realized_pnl(_deal(profit=5.0)) == 5.0


def test_deal_pnl_ignores_a_non_numeric_field():
  assert deal_realized_pnl(_deal(profit=5.0, commission=None, swap="n/a")) == 5.0


def test_deals_pnl_sums_a_whole_position():
  # What the reconciler reports: every deal the position ever produced, so the
  # entry's commission and an earlier partial close are both counted.
  deals = [
    _deal(profit=0.0, commission=-0.35, swap=0.0, fee=0.0),  # entry
    _deal(profit=6.0, commission=-0.35, swap=0.0, fee=0.0),  # TP1 partial
    _deal(profit=9.0, commission=-0.35, swap=-0.10, fee=0.0),  # final exit
  ]
  assert deals_realized_pnl(deals) == pytest.approx(13.85)


def test_deals_pnl_is_unknown_rather_than_zero_when_history_is_empty():
  # The distinction the whole notification rests on: no deal history means the
  # amount is unknown ("n/a"), not that the trade broke even.
  assert deals_realized_pnl([]) is None
  assert deals_realized_pnl(None) is None


def test_deals_pnl_reports_a_genuine_break_even_as_zero():
  assert deals_realized_pnl([_deal(profit=0.0, commission=0.0)]) == 0.0
