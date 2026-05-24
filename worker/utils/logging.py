from typing import Any, Callable, Dict, Optional


def build_account_footer(
  login: int,
  account_name: str,
  server: str,
  status: Optional[Dict[str, Any]] = None,
) -> str:
  s = status or {}
  return (
    f"\n"
    f"----------------------------------\n"
    f"<b>Account:</b> {login} ({account_name})\n"
    f"<b>Balance:</b> {s.get('balance', 0.0):.2f}\n"
    f"<b>Equity:</b> {s.get('equity', 0.0):.2f}\n"
    f"<b>Leverage:</b> {s.get('leverage', 0.0):.2f}\n"
    f"<b>Margin:</b> {s.get('margin', 0.0):.2f}\n"
    f"<b>Free Margin:</b> {s.get('free_margin', 0.0):.2f}\n"
    f"<b>Margin Level:</b> {s.get('margin_level', 0.0):.2f}%\n"
    f"<b>Server:</b> {server}\n"
    f"----------------------------------"
  )


def get_footer(footer_fn: Optional[Callable[[], str]]) -> str:
  """Call footer_fn if provided and return the result, else empty string."""
  if footer_fn is None:
    return ""
  try:
    return footer_fn()
  except Exception:
    return ""
