from enum import Enum


class NatsSubjectEnum(str, Enum):
  SIGNAL = "SIGNAL"
  ADMIN = "ADMIN"
  TRADE = "TRADE"
