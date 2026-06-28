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
  # _account_id is the explicit BINANCE_ACCOUNT_ID (required for crypto), used
  # for ADMIN FLAT routing and the account footer.
  proc = _proc({"binance_account_id": "acct-123"})
  assert proc._account_id == "acct-123"


def test_account_id_none_when_unset():
  # Settings validation requires BINANCE_ACCOUNT_ID for crypto, so this is
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
