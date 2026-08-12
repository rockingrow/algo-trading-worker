"""Tests for the shared close-notification fragments in ``BaseMessagePresenter``.

The PnL line is what makes "did that close make money?" answerable from Telegram
alone, and both markets render it through the one function pinned here — so its
format, its sign, and above all its unknown/zero distinction are fixed in one
place rather than per market.
"""

from helpers import make_signal

from worker.gateways.message_presenter import BaseMessagePresenter, pnl_line
from worker.icons import LOSS, PROFIT
from worker.schemas.signal_schema import SignalActionEnum

# ── The line itself ─────────────────────────────────────────────────────────── #


def test_a_profit_is_signed_and_marked():
  assert pnl_line(41.756) == f"PnL: <b>+41.76</b> {PROFIT}\n"


def test_a_loss_is_signed_and_marked():
  assert pnl_line(-18.2) == f"PnL: <b>-18.20</b> {LOSS}\n"


def test_break_even_is_zero_with_no_direction_icon():
  # A real result, just a flat one — distinct from "we could not read it".
  assert pnl_line(0.0) == "PnL: <b>+0.00</b>\n"


def test_an_unknown_amount_reads_n_a_rather_than_vanishing():
  # Dropping the line would read as break-even; printing 0.00 would be a lie.
  assert pnl_line(None) == "PnL: <b>n/a</b>\n"


def test_the_label_can_qualify_what_the_number_covers():
  assert pnl_line(5.0, label="PnL (est.)").startswith("PnL (est.): <b>+5.00</b>")


# ── Gating on exits ─────────────────────────────────────────────────────────── #


def test_exit_actions_all_carry_the_line():
  result = {"profit": 3.0}
  for action in (
    SignalActionEnum.TP1,
    SignalActionEnum.TP2,
    SignalActionEnum.SL,
    SignalActionEnum.R_SL,
    SignalActionEnum.FLAT,
  ):
    signal = make_signal(action)
    assert "PnL" in BaseMessagePresenter._exit_pnl_line(signal, result), action


def test_entries_carry_no_line():
  # An entry has booked nothing yet.
  for action in (SignalActionEnum.LONG, SignalActionEnum.SHORT):
    signal = make_signal(action)
    assert BaseMessagePresenter._exit_pnl_line(signal, {"profit": 3.0}) == ""


# ── The unprotected-position emergency close ────────────────────────────────── #


def test_emergency_close_reports_both_closes_it_performed():
  # Two closes happened: the TP1 partial, then the emergency close of the runner
  # after the breakeven SL failed. Netting them would hide what each cost.
  signal = make_signal(SignalActionEnum.TP1)
  result = {
    "profit": 6.0,
    "sl_update": {"success": False, "comment": "SL rejected"},
    "sl_failsafe_close": {
      "success": True,
      "volume": 0.07,
      "price": 2001.0,
      "profit": -2.5,
    },
  }

  msg = BaseMessagePresenter.position_unprotected_closed(signal, result, "FOOTER")

  assert "Emergency Closed" in msg
  assert "PnL (TP1 partial): <b>+6.00</b>" in msg
  assert "PnL (emergency close): <b>-2.50</b>" in msg


def test_a_failed_emergency_close_still_reports_the_partial_that_did_fill():
  # The remaining position is still live, so only the TP1 partial booked a result.
  signal = make_signal(SignalActionEnum.TP1)
  result = {
    "profit": 6.0,
    "sl_update": {"success": False, "comment": "SL rejected"},
    "sl_failsafe_close": {"success": False, "comment": "market closed"},
  }

  msg = BaseMessagePresenter.position_unprotected_closed(signal, result, "FOOTER")

  assert "STILL OPEN" in msg
  assert "PnL (TP1 partial): <b>+6.00</b>" in msg
  assert "PnL (emergency close)" not in msg
