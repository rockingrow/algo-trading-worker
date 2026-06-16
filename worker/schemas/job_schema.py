from enum import Enum


class LogAuthorEnum(str, Enum):
  TERMINAL = "terminal"  # FOREX: closed by the MT5 terminal (hard SL/TP, stop-out)
  EXCHANGE = "exchange"  # CRYPTO: closed by the CEX (stop-loss/take-profit/liquidation)
  BROKER = "broker"
