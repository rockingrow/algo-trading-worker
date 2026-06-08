from typing import Optional


def int_to_string(value) -> Optional[str]:
  """App int id → DB TEXT ref (None stays None)."""
  return None if value is None else str(value)


def string_to_int(value) -> Optional[int]:
  """DB TEXT ref → app int id (None stays None; non-numeric falls back to raw)."""
  if value is None:
    return None
  try:
    return int(value)
  except (TypeError, ValueError):
    return value
