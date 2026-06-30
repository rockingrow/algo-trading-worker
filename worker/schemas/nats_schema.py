from enum import Enum


class NatsSubjectEnum(str, Enum):
  SIGNAL = "SIGNAL"
  ADMIN = "ADMIN"
  SYSTEM = "SYSTEM"
  TRADE = "TRADE"
  ACK = "ACK"
