"""Tests for CRYPTO-specific signal-processor hooks."""

from types import SimpleNamespace

from worker.gateways.crypto.signal_processor import CryptoSignalProcessor
from worker.schemas.position_schema import PositionStatusEnum


def _proc(settings: dict) -> CryptoSignalProcessor:
  # Bypass __init__ (which builds a live gateway); _account_id only reads .settings.
  proc = object.__new__(CryptoSignalProcessor)
  proc.settings = settings
  return proc


def test_account_id_returns_configured_binance_account_id():
  # _account_id is the explicit CRYPTO_ACCOUNT_ID (required for crypto), used
  # for ADMIN FLAT routing and the account footer.
  proc = _proc({"crypto_account_id": "acct-123"})
  assert proc._account_id == "acct-123"


def test_account_id_none_when_unset():
  # Settings validation requires CRYPTO_ACCOUNT_ID for crypto, so this is
  # unreachable in production; the property just surfaces what is configured.
  proc = _proc({})
  assert proc._account_id is None


# ── reconciler handler ─────────────────────────────────────────────────────── #


def _reconcile_proc():
  """A processor stubbed just enough to exercise _on_missed_close."""
  proc = object.__new__(CryptoSignalProcessor)
  proc._market_type = "CRYPTO"
  proc.executor = SimpleNamespace(get_symbol=lambda s: s.replace("USD", "USDT"))
  proc.gateway = SimpleNamespace(
    get_mark_price=lambda sym: 64000.0,
    get_account_footer=lambda account_id=None: "FOOTER",
  )
  updates, logs, notes = [], [], []
  proc.ctx = SimpleNamespace(
    db_service=SimpleNamespace(
      update_position_status=lambda **k: updates.append(k),
      log_position=lambda **k: logs.append(k),
    ),
    channel_notifier=SimpleNamespace(send_message=lambda m: notes.append(m)),
  )
  return proc, updates, logs, notes


def test_missed_close_marks_terminal_closed_with_mark_price():
  proc, updates, logs, notes = _reconcile_proc()
  proc._on_missed_close(
    {"ref_source_id": "55", "symbol": "BTCUSD", "strategy": "s", "volume": 0.01}
  )
  assert len(updates) == 1
  assert updates[0]["status"] == PositionStatusEnum.TERMINAL_CLOSED
  assert updates[0]["ref_source_id"] == "55"
  assert updates[0]["closed_price"] == 64000.0  # best-effort mark price
  assert len(logs) == 1 and len(notes) == 1  # audit log + operator alert


def test_missed_close_tolerates_missing_mark_price():
  proc, updates, _, notes = _reconcile_proc()
  proc.gateway.get_mark_price = lambda sym: (_ for _ in ()).throw(RuntimeError("down"))
  proc._on_missed_close({"ref_source_id": "55", "symbol": "BTCUSD", "strategy": "s"})
  assert updates[0]["closed_price"] is None  # falls back to None, still closes
  assert len(notes) == 1


def test_missed_close_estimates_the_pnl_from_the_row_and_mark_price():
  # The real fill was never delivered (that is why the reconciler is involved),
  # so the figure is measured against the same approximate close price the
  # message already shows — and is labelled an estimate for exactly that reason.
  proc, _, _, notes = _reconcile_proc()

  proc._on_missed_close(
    {
      "ref_source_id": "55",
      "symbol": "BTCUSD",
      "strategy": "s",
      "action": "long",
      "volume": 0.02,
      "opened_price": 63000.0,
    }
  )

  # 0.02 BTC from 63,000 to a 64,000 mark.
  assert "PnL (est.): <b>+20.00</b>" in notes[0]


def test_missed_close_estimate_inverts_for_a_short():
  proc, _, _, notes = _reconcile_proc()

  proc._on_missed_close(
    {
      "ref_source_id": "55",
      "symbol": "BTCUSD",
      "strategy": "s",
      "action": "short",
      "volume": 0.02,
      "opened_price": 63000.0,
    }
  )

  assert "PnL (est.): <b>-20.00</b>" in notes[0]


def test_missed_close_pnl_is_unknown_without_a_mark_price():
  # No price to measure against — the line still appears, saying so.
  proc, _, _, notes = _reconcile_proc()
  proc.gateway.get_mark_price = lambda sym: (_ for _ in ()).throw(RuntimeError("down"))

  proc._on_missed_close(
    {
      "ref_source_id": "55",
      "symbol": "BTCUSD",
      "strategy": "s",
      "action": "long",
      "volume": 0.02,
      "opened_price": 63000.0,
    }
  )

  assert "PnL (est.): <b>n/a</b>" in notes[0]


def test_missed_close_pnl_is_unknown_when_the_row_has_no_usable_side():
  # The side decides the sign, so guessing it would report a loss as a profit.
  proc, _, _, notes = _reconcile_proc()

  proc._on_missed_close(
    {
      "ref_source_id": "55",
      "symbol": "BTCUSD",
      "strategy": "s",
      "volume": 0.02,
      "opened_price": 63000.0,
    }
  )

  assert "PnL (est.): <b>n/a</b>" in notes[0]


# ── manual-position import handler ──────────────────────────────────────────── #


def _manual_proc():
  """A processor stubbed just enough to exercise _on_manual_position."""
  proc = object.__new__(CryptoSignalProcessor)
  proc._market_type = "CRYPTO"
  proc.gateway = SimpleNamespace(get_account_footer=lambda account_id=None: "FOOTER")
  inserts, logs, notes = [], [], []
  proc.ctx = SimpleNamespace(
    db_service=SimpleNamespace(
      insert_position=lambda **k: inserts.append(k),
      log_position=lambda **k: logs.append(k),
    ),
    channel_notifier=SimpleNamespace(send_message=lambda m: notes.append(m)),
  )
  return proc, inserts, logs, notes


def _exchange_position(
  symbol="BTCUSDT", side="LONG", quantity=0.02, entry_price=64000.0
):
  # Mirrors ExchangePosition: exposes .symbol/.side plus volume/price_open aliases.
  return SimpleNamespace(
    symbol=symbol,
    side=side,
    volume=abs(quantity),
    price_open=entry_price,
  )


def test_manual_position_imported_as_opened_row():
  proc, inserts, logs, notes = _manual_proc()
  proc._on_manual_position(_exchange_position(symbol="BTCUSDT", side="LONG"))

  assert len(inserts) == 1
  ins = inserts[0]
  assert ins["strategy"] == "MANUAL"
  assert ins["symbol"] == "BTCUSDT"  # resolved exchange symbol stored as-is
  assert ins["action"] == "long"  # lowercased, mirroring the signal-entry flow
  assert ins["volume"] == 0.02
  assert ins["opened_price"] == 64000.0
  assert ins["message"] is None  # no originating signal JSON
  assert ins["ref_id"].startswith("MANUAL-BTCUSDT-")  # unique synthetic id
  # Audit log + operator notification both fired.
  assert len(logs) == 1 and logs[0]["action"] == "LONG"
  assert len(notes) == 1 and "Manual Position Imported" in notes[0]


def test_manual_position_short_side_lowercased():
  proc, inserts, _, _ = _manual_proc()
  proc._on_manual_position(_exchange_position(symbol="ETHUSDT", side="SHORT"))
  assert inserts[0]["action"] == "short"
  assert inserts[0]["symbol"] == "ETHUSDT"


def test_manual_position_ref_source_id_unique_per_import():
  # Re-importing the same symbol (manual close then re-open) must not reuse the id,
  # so the broker never upserts onto the prior, already-closed trade.
  proc, inserts, _, _ = _manual_proc()
  proc._on_manual_position(_exchange_position(symbol="BTCUSDT"))
  proc._on_manual_position(_exchange_position(symbol="BTCUSDT"))
  assert inserts[0]["ref_id"] != inserts[1]["ref_id"]
