from enum import Enum


class PositionStatusEnum(str, Enum):
  OPENED = "OPENED"
  TP1 = "TP1"
  TP2 = "TP2"
  SL = "SL"
  R_SL = "R_SL"
  TERMINAL_CLOSED = "TERMINAL_CLOSED"
  FORCED_CLOSED = "FORCED_CLOSED"
  FLATTED = "FLATTED"
