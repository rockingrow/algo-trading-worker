from enum import Enum


class NotificationPlatformEnum(str, Enum):
  TELEGRAM = "TELEGRAM"

class NotificationChannelEnum(str, Enum):
  INDIVIDUAL = "INDIVIDUAL"
  COMMUNITY = "COMMUNITY"
