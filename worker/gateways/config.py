"""
worker/gateways/config.py
──────────────────────────
Immutable, explicitly-injected execution configuration.

Business-logic objects (the MT5 executor, lot sizer, market strategy) used to
reach for the global ``settings`` singleton directly, which hid their real
dependencies and made them hard to test in isolation. They now receive an
``ExecutionConfig`` through their constructor (Dependency Inversion): the
concrete global ``settings`` is read once, at the composition root, and turned
into this small value object.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Market → the settings key holding that market's multi-strategy-per-symbol
# toggle. Adding a market means adding an entry here, not a branch in from_dict.
_MULTI_STRATEGY_SETTING_BY_MARKET = {
  "FOREX": "forex_allow_multi_strategy_per_symbol",
  "CRYPTO": "crypto_allow_multi_strategy_per_symbol",
}


@dataclass(frozen=True)
class ExecutionConfig:
  """Risk / volume parameters needed to translate a signal into a lot size."""

  volume_decision_enabled: bool
  capital: float
  risk_percentage: float
  # Tri-state (see ``resolve_equity_sizing``): True/False are a worker-level
  # decision that wins over the signal, None defers to the signal's own
  # ``use_equity_sizing``.
  use_account_equity: Optional[bool]
  # Controls tp1_percent resolution at TP1 time:
  #   True  → always use position_tp1_percent (ignores signal.tp1_percent)
  #   False → use signal.tp1_percent if present, else fall back to position_tp1_percent
  use_custom_position_tp1_percent: bool = False
  position_tp1_percent: Optional[float] = None
  # When set, overrides the signal's move_sl_to_be; resolved at TP1 time.
  # Priority: this field > signal.move_sl_to_be > False (default).
  tp1_move_sl_to_breakeven: Optional[bool] = None
  # Allow multiple strategies to trade the same symbol simultaneously. Resolved
  # per market from FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL /
  # CRYPTO_ALLOW_MULTI_STRATEGY_PER_SYMBOL (see ``from_dict``); what it unlocks
  # differs per market:
  #   FOREX  — the real feature. Each strategy trades under its own magic
  #            number, so concurrent positions on one symbol stay isolated, and
  #            ``ForexMarket`` reports the capability to SignalHandler, which
  #            then permits the parallel entry (requires a hedging account).
  #   CRYPTO — relaxes ``CryptoExecutor._netting_conflict`` only. A CEX cannot
  #            isolate two strategies on one symbol, so ``CryptoMarket`` does
  #            NOT report the capability and the handler still rejects the
  #            entry. See CryptoMarket's docstring for why.
  # Default False enforces one-strategy-per-symbol. It never relaxes the
  # one-active-position-per-(strategy, symbol) invariant, which holds in both
  # markets.
  allow_multi_strategy_per_symbol: bool = False
  # When True, always use risk_percentage from settings regardless of signal.
  # When False (default): use signal.risk_percent if present, else risk_percentage.
  use_custom_risk_percentage: bool = False

  @classmethod
  def from_dict(cls, settings_dict: dict) -> "ExecutionConfig":
    """Build from the flat ``settings.flat_dump()`` dict passed across the
    multiprocessing fork boundary."""
    return cls(
      volume_decision_enabled=settings_dict.get("volume_decision_enabled", True),
      capital=settings_dict.get("capital", 1000.0),
      risk_percentage=settings_dict.get("risk_percentage", 1.0),
      use_account_equity=settings_dict.get("use_account_equity"),
      use_custom_position_tp1_percent=settings_dict.get(
        "use_custom_position_tp1_percent", False
      ),
      position_tp1_percent=settings_dict.get("position_tp1_percent"),
      tp1_move_sl_to_breakeven=settings_dict.get("tp1_move_sl_to_breakeven"),
      allow_multi_strategy_per_symbol=cls._resolve_multi_strategy(settings_dict),
      use_custom_risk_percentage=settings_dict.get("use_custom_risk_percentage", False),
    )

  def resolve_equity_sizing(self, signal_flag: Optional[bool]) -> Optional[bool]:
    """Decide how an entry is sized, by priority.

    *signal_flag* is ``position.use_equity_sizing`` from the payload.

      1. ``USE_ACCOUNT_EQUITY`` set in the environment wins outright — True or
         False is the worker's own decision and the payload flag is ignored.
      2. Otherwise the payload's ``use_equity_sizing`` decides.
      3. Neither expressed an opinion → ``None``: no explicit sizing mode, so
         the caller keeps the legacy behaviour driven by
         ``VOLUME_DECISION_ENABLED`` (risk sizing off the fixed ``CAPITAL``).

    ``True``  → size from the live account equity and ignore ``quantity``.
    ``False`` → size from the payload's ``quantity``.
    """
    if self.use_account_equity is not None:
      return bool(self.use_account_equity)
    if signal_flag is not None:
      return bool(signal_flag)
    return None

  def uses_payload_quantity(self, signal_flag: Optional[bool]) -> bool:
    """True when an entry carrying *signal_flag* must be sized from the payload's
    ``quantity`` rather than by the worker itself.

    Explicit equity sizing (env or payload) decides; with neither expressed the
    legacy ``VOLUME_DECISION_ENABLED`` switch does.
    """
    equity_sizing = self.resolve_equity_sizing(signal_flag)
    if equity_sizing is not None:
      return equity_sizing is False
    return not self.volume_decision_enabled

  @staticmethod
  def _resolve_multi_strategy(settings_dict: dict) -> bool:
    """Pick the multi-strategy-per-symbol toggle belonging to the active market.

    Each market has its own env var (they carry different risk — see the field
    docstring above), so exactly one of them is in play for a given worker and
    the other is ignored rather than silently leaking across markets.
    """
    raw = settings_dict.get("market_type") or "FOREX"
    market = str(getattr(raw, "value", raw)).upper()
    key = _MULTI_STRATEGY_SETTING_BY_MARKET.get(market)
    if key is None:
      return False
    return bool(settings_dict.get(key, False))
