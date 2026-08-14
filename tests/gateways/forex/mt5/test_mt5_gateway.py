"""Regression tests for MT5Gateway market-data reads.

``worker.gateways.forex.mt5.bridge`` imports the MetaTrader5 native extension at
module scope and it only ships for Windows, so an empty stand-in is registered
when the real extension is absent — nothing here touches the bridge, which only
reaches for ``mt5.*`` inside its connection methods. The real module is preferred
whenever it imports, so this never shadows it on Windows.
"""

import sys
from types import ModuleType, SimpleNamespace

import pytest

try:  # pragma: no cover - the native extension exists only on Windows
  import MetaTrader5  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - every non-Windows run
  sys.modules["MetaTrader5"] = ModuleType("MetaTrader5")

from helpers import (  # noqa: E402
  FakeMt5,
  make_deal,
  make_order_result,
  make_platform_position,
  make_tick,
)

from worker.gateways.forex.mt5 import gateway as gateway_module  # noqa: E402
from worker.gateways.forex.mt5.gateway import MT5Gateway  # noqa: E402


def _gateway(mt5):
  return MT5Gateway(server="TestServer", login=1000, password="pw", mt5_api=mt5)


def test_get_tick_returns_the_live_quote():
  gw = _gateway(FakeMt5(tick=make_tick(ask=2000.0, bid=1999.5)))
  tick = gw.get_tick("XAUUSDc")
  assert (tick.bid, tick.ask) == (1999.5, 2000.0)


def test_get_tick_rejects_a_zeroed_quote():
  """The terminal reports bid=ask=0.0 for a symbol it has no price for (freshly
  added to Market Watch, or outside its session).

  ``symbol_info_tick`` returns a namedtuple, which is truthy even when every
  field is zero, so a plain falsiness check let that tick through: the entry was
  then priced at 0.0 and the stop math pushed SL to -0.01, which the broker
  rejected with retcode 10016 (Invalid stops).
  """
  gw = _gateway(FakeMt5(tick=make_tick(ask=0.0, bid=0.0)))
  assert gw.get_tick("XPTUSDm") is None


def test_get_tick_rejects_a_half_populated_quote():
  gw = _gateway(FakeMt5(tick=make_tick(ask=0.0, bid=1732.55)))
  assert gw.get_tick("XPTUSDm") is None


def test_get_tick_returns_none_when_the_terminal_has_no_tick():
  gw = _gateway(FakeMt5(tick=None))
  # FakeMt5 substitutes a default tick for None, so blank the read directly.
  gw._mt5.symbol_info_tick = lambda name: None
  assert gw.get_tick("XAUUSDc") is None


def test_close_position_refuses_to_price_off_a_zeroed_quote():
  """A close is priced from the same tick — it must not send price=0.0 either."""
  mt5 = FakeMt5(tick=make_tick(ask=0.0, bid=0.0))
  gw = _gateway(mt5)
  result = gw.close_position(make_platform_position(symbol="XPTUSDm"))
  assert result["success"] is False
  assert mt5.sent_requests == []


def test_place_order_sends_the_resolved_stops():
  """Guard-rail check: a healthy quote still produces a normal order request."""
  mt5 = FakeMt5(tick=make_tick(ask=2000.0, bid=1999.5))
  gw = _gateway(mt5)
  gw.place_order(
    symbol="XAUUSDc",
    side="LONG",
    volume=0.01,
    price=2000.0,
    sl=1990.0,
    tp=2050.0,
    magic=42,
    comment="strat-1 04",
  )
  sent = mt5.sent_requests[0]
  assert (sent["price"], sent["sl"], sent["tp"]) == (2000.0, 1990.0, 2050.0)
  assert isinstance(sent["symbol"], str)


def test_symbol_spec_maps_broker_rules():
  gw = _gateway(FakeMt5())
  spec = gw.get_symbol_spec("XAUUSDc")
  assert (spec.point, spec.digits, spec.stops_level) == (0.01, 2, 10)


def test_symbol_spec_is_none_for_an_unknown_symbol():
  mt5 = FakeMt5()
  mt5.symbol_info = lambda name: None
  assert _gateway(mt5).get_symbol_spec("NOPE") is None


def test_positions_are_mapped_to_platform_positions():
  raw = SimpleNamespace(
    ticket=201,
    type=FakeMt5.ORDER_TYPE_BUY,
    volume=0.5,
    price_open=1999.0,
    sl=1990.0,
    tp=2050.0,
    magic=42,
    symbol="XAUUSDc",
  )
  positions = _gateway(FakeMt5(positions=[raw])).get_positions("XAUUSDc")
  assert [(p.ticket, p.side, p.volume) for p in positions] == [(201, "LONG", 0.5)]


# ── Realized PnL of a close ─────────────────────────────────────────────────── #
#
# ``order_send`` reports what happened to the *order* and carries no money
# figure, so the amount a close booked is read back out of deal history.
# Without it every worker-initiated FOREX close reports its PnL as "n/a".


def test_close_position_reports_what_the_deal_booked():
  mt5 = FakeMt5(
    order_results=[make_order_result(order=999, deal=777)],
    deals=[
      make_deal(
        ticket=777,
        order=999,
        position_id=111,
        profit=12.5,
        commission=-0.7,
        swap=-0.3,
      )
    ],
  )
  result = _gateway(mt5).close_position(make_platform_position(ticket=111))

  assert result["success"] is True
  assert result["profit"] == 11.5


def test_close_position_reads_history_by_position_and_narrows_to_its_own_order():
  """Regression, in two layers, for a close that reported ``PnL: n/a``.

  ``history_deals_get(ticket=…)`` filters on ``DEAL_ORDER``, never on a deal's own
  ticket, so the original lookup by ``OrderSendResult.deal`` matched nothing. But
  asking the terminal by order did not answer either on a live account, for a
  position the reconciler then read in full through ``position=``. So the query
  uses the position filter — the one with evidence behind it — and picks this
  close's own deals out of the result here.
  """
  mt5 = FakeMt5(
    order_results=[make_order_result(order=999, deal=777)],
    deals=[make_deal(ticket=777, order=999, position_id=111, profit=12.5)],
  )
  assert (
    _gateway(mt5).close_position(make_platform_position(ticket=111))["profit"] == 12.5
  )
  assert mt5.deal_lookups == [{"ticket": None, "position": 111}]


def test_tp1_partial_that_takes_the_whole_position_reports_its_pnl():
  """The exact shape an operator reported as ``PnL: n/a``, tickets and all.

  A TP1 whose close volume equals the position's whole volume — routine when the
  entry was already at the broker's minimum lot — is an ordinary partial close as
  far as the gateway is concerned (``ForexExecutor.partial_close_position`` passes
  it straight through), so it took the same broken lookup as every other
  worker-placed close. The reconciler that later picked the position up reported
  its total fine, which is what pointed at the lookup rather than the history.
  """
  mt5 = FakeMt5(
    order_results=[make_order_result(order=4911373684, deal=4911373999)],
    deals=[
      make_deal(
        ticket=4911373999,
        order=4911373684,
        position_id=4911373104,
        profit=8.4,
        commission=-0.9,
      )
    ],
  )
  position = make_platform_position(ticket=4911373104, volume=0.1, symbol="BTCUSD")
  result = _gateway(mt5).close_position(
    position, volume=0.1, comment="Partial Close TP1"
  )

  assert result["success"] is True
  assert result["profit"] == pytest.approx(7.5)


def test_close_position_reports_this_close_not_the_positions_running_total():
  """The position's other deals share the result the position filter returns.

  Reading by position is what makes the lookup work at all, so the narrowing that
  keeps an entry's commission — and any earlier TP1 — off *this* close's PnL line
  is done in Python. Without it a partial close would report the position total
  under a plain ``PnL:`` label, which is a different (and wrong) number.
  """
  mt5 = FakeMt5(
    order_results=[make_order_result(order=999, deal=777)],
    deals=[
      make_deal(ticket=1, order=500, position_id=111, commission=-0.35),  # entry
      make_deal(ticket=2, order=600, position_id=111, profit=6.0),  # earlier TP1
      make_deal(ticket=777, order=999, position_id=111, profit=3.0, commission=-0.2),
    ],
  )
  result = _gateway(mt5).close_position(make_platform_position(ticket=111))
  assert result["profit"] == pytest.approx(2.8)


def test_close_position_sums_a_close_the_broker_filled_in_several_deals():
  # One close order can be filled against several counter-orders. The
  # notification reports one close, so it reports their sum.
  mt5 = FakeMt5(
    order_results=[make_order_result(order=999, deal=777)],
    deals=[
      make_deal(ticket=777, order=999, position_id=111, profit=4.0, commission=-0.2),
      make_deal(ticket=778, order=999, position_id=111, profit=6.0, commission=-0.3),
      make_deal(ticket=900, order=1000, position_id=111, profit=99.0),  # not ours
    ],
  )
  result = _gateway(mt5).close_position(make_platform_position(ticket=111))
  assert result["profit"] == pytest.approx(9.5)


def test_close_position_pnl_is_unknown_when_history_has_nothing():
  # Reported as unknown rather than 0.0: a close whose amount we could not read
  # must not be shown as a break-even trade.
  mt5 = FakeMt5(order_results=[make_order_result(order=999, deal=777)], deals=[])
  assert _gateway(mt5).close_position(make_platform_position())["profit"] is None


def test_close_position_retries_a_history_read_that_has_not_landed_yet(monkeypatch):
  # The deal is registered server-side before order_send returns its ticket, but
  # the terminal's own history trails it — by long enough that a live close read
  # 0.3s later still found nothing, which is why the window is now ~1.25s.
  monkeypatch.setattr(gateway_module.time, "sleep", lambda _s: None)
  mt5 = FakeMt5(order_results=[make_order_result(order=999, deal=777)], deals=[])
  landed = make_deal(ticket=777, order=999, position_id=111, profit=4.0)
  responses = [[], [], [], [], [landed]]
  mt5.history_deals_get = lambda ticket=None, position=None: responses.pop(0)

  result = _gateway(mt5).close_position(make_platform_position(ticket=111))
  assert result["profit"] == 4.0


def test_close_position_keeps_retrying_while_only_the_entry_deal_is_there(monkeypatch):
  """History holding the position's *older* deals is not this close having landed.

  Stopping at the first non-empty read would report the entry's commission as the
  close's result — a plausible-looking wrong number, which is worse than n/a.
  """
  waits = []
  monkeypatch.setattr(gateway_module.time, "sleep", waits.append)
  mt5 = FakeMt5(order_results=[make_order_result(order=999, deal=777)])
  entry = make_deal(ticket=1, order=500, position_id=111, commission=-0.35)
  closed = make_deal(ticket=777, order=999, position_id=111, profit=7.0)
  responses = [[entry], [entry], [entry, closed]]
  mt5.history_deals_get = lambda ticket=None, position=None: responses.pop(0)

  result = _gateway(mt5).close_position(make_platform_position(ticket=111))

  assert result["profit"] == pytest.approx(7.0)
  assert len(waits) == 2  # waited past both entry-only reads


def test_close_position_pnl_is_unknown_without_an_order_ticket():
  mt5 = FakeMt5(
    order_results=[make_order_result(order=0)], deals=[make_deal(profit=9.0)]
  )
  result = _gateway(mt5).close_position(make_platform_position())

  assert result["profit"] is None
  assert mt5.deal_lookups == []  # nothing to look up


def test_close_position_survives_a_failing_history_read():
  # The close already succeeded; a terminal that cannot answer costs the PnL
  # line, never the trade result.
  mt5 = FakeMt5(order_results=[make_order_result(order=999, deal=777)])

  def _boom(ticket=None, position=None):
    raise RuntimeError("terminal busy")

  mt5.history_deals_get = _boom
  result = _gateway(mt5).close_position(make_platform_position())

  assert result["success"] is True and result["profit"] is None


def test_position_realized_pnl_sums_every_deal_of_the_position():
  # What the reconciler reads: it never saw the close, only the ticket, so the
  # figure spans the position's whole life.
  mt5 = FakeMt5(
    deals=[
      make_deal(ticket=1, position_id=4587420656, profit=0.0, commission=-0.35),
      make_deal(ticket=2, position_id=4587420656, profit=6.0, commission=-0.35),
      make_deal(
        ticket=3, position_id=4587420656, profit=9.0, commission=-0.35, swap=-0.1
      ),
      make_deal(ticket=4, position_id=999, profit=50.0),  # another position
    ]
  )
  assert _gateway(mt5).get_position_realized_pnl(4587420656) == pytest.approx(13.85)
  assert mt5.deal_lookups == [{"ticket": None, "position": 4587420656}]


def test_position_realized_pnl_accepts_the_tickets_text_form():
  """Regression: the reconciler's ticket comes out of the DB, where it is TEXT.

  ``history_deals_get`` is a native extension whose ``position`` argument must be
  an integer — a string raised inside the lookup, so every ``Position
  Reconciled`` notification reported ``PnL (position total): n/a``.
  """
  mt5 = FakeMt5(
    deals=[make_deal(ticket=1, position_id=798267992, profit=2.4, commission=-0.1)]
  )
  assert _gateway(mt5).get_position_realized_pnl("798267992") == pytest.approx(2.3)
  assert mt5.deal_lookups == [{"ticket": None, "position": 798267992}]


def test_position_realized_pnl_is_unknown_for_a_ticket_that_is_not_a_number():
  # A row whose reference is not a ticket at all (never seen in practice) must
  # leave the PnL unknown, not raise on the reconcile path.
  mt5 = FakeMt5(deals=[make_deal(position_id=111, profit=5.0)])
  assert _gateway(mt5).get_position_realized_pnl("not-a-ticket") is None
  assert mt5.deal_lookups == []  # never reached the terminal


def test_position_realized_pnl_is_unknown_when_history_is_empty():
  mt5 = FakeMt5(deals=[])
  assert _gateway(mt5).get_position_realized_pnl(111) is None
