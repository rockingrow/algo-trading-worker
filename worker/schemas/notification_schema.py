from enum import Enum


class NotificationPlatformEnum(str, Enum):
  TELEGRAM = "TELEGRAM"

class NotificationChannelEnum(str, Enum):
  INDIVIDUAL = "INDIVIDUAL"
  COMMUNITY = "COMMUNITY"

class NotificationModeEnum(str, Enum):
  VERBOSE = "VERBOSE"
  SILENT = "SILENT"
  ERROR = "ERROR"
