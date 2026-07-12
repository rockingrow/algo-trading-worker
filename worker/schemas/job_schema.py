from enum import Enum


class LogAuthorEnum(str, Enum):
  TERMINAL = "terminal"  # FOREX: closed by the MT5 terminal (hard SL/TP, stop-out)
  EXCHANGE = "exchange"  # CRYPTO: closed by the CEX (stop-loss/take-profit/liquidation)
  BROKER = "broker"
  MANUAL = "manual"  # FOREX: opened by hand on the terminal (magic not owned), adopted by the sync job
