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


@dataclass(frozen=True)
class ExecutionConfig:
  """Risk / volume parameters needed to translate a signal into a lot size."""

  volume_decision_enabled: bool
  capital: float
  risk_percentage: float
  use_account_equity: bool
  # Controls tp1_percent resolution at TP1 time:
  #   True  → always use position_tp1_percent (ignores signal.tp1_percent)
  #   False → use signal.tp1_percent if present, else fall back to position_tp1_percent
  use_custom_position_tp1_percent: bool = False
  position_tp1_percent: Optional[float] = None
  # When set, overrides the signal's move_sl_to_be; resolved at TP1 time.
  # Priority: this field > signal.move_sl_to_be > False (default).
  tp1_move_sl_to_breakeven: Optional[bool] = None
  # Crypto-only: allow multiple strategies to trade the same symbol simultaneously.
  # In Binance netting mode this merges positions at the exchange level; default False
  # enforces one-strategy-per-symbol and aborts new entries that would violate it.
  allow_multi_strategy_per_symbol: bool = False
  # When True, always use risk_percentage from settings regardless of signal.
  # When False (default): use signal.risk_percent if present, else risk_percentage.
  use_custom_risk_percentage: bool = False

  @classmethod
  def from_settings(cls, settings) -> "ExecutionConfig":
    """Build from the pydantic ``Settings`` singleton (or any object exposing
    the same attributes)."""
    return cls(
      volume_decision_enabled=settings.volume_decision_enabled,
      capital=settings.capital,
      risk_percentage=settings.risk_percentage,
      use_account_equity=settings.use_account_equity,
      use_custom_position_tp1_percent=getattr(settings, "use_custom_position_tp1_percent", False),
      position_tp1_percent=settings.position_tp1_percent,
      tp1_move_sl_to_breakeven=getattr(settings, "tp1_move_sl_to_breakeven", None),
      allow_multi_strategy_per_symbol=getattr(settings, "crypto_allow_multi_strategy_per_symbol", False),
      use_custom_risk_percentage=getattr(settings, "use_custom_risk_percentage", False),
    )

  @classmethod
  def from_dict(cls, settings_dict: dict) -> "ExecutionConfig":
    """Build from the plain ``settings.model_dump()`` dict passed across the
    multiprocessing fork boundary."""
    return cls(
      volume_decision_enabled=settings_dict.get("volume_decision_enabled", True),
      capital=settings_dict.get("capital", 1000.0),
      risk_percentage=settings_dict.get("risk_percentage", 1.0),
      use_account_equity=settings_dict.get("use_account_equity", False),
      use_custom_position_tp1_percent=settings_dict.get("use_custom_position_tp1_percent", False),
      position_tp1_percent=settings_dict.get("position_tp1_percent"),
      tp1_move_sl_to_breakeven=settings_dict.get("tp1_move_sl_to_breakeven"),
      allow_multi_strategy_per_symbol=settings_dict.get("crypto_allow_multi_strategy_per_symbol", False),
      use_custom_risk_percentage=settings_dict.get("use_custom_risk_percentage", False),
    )
