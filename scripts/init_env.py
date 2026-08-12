#!/usr/bin/env python3
"""Interactive initializer for the project's ``.env`` file.

Builds a ``.env`` from ``.env.example``. Re-running edits the existing file in
place: ``.env.example`` provides the canonical set of keys, while the values
already in ``.env`` become the prefilled defaults. Every key is written exactly
once, so nothing is duplicated and existing values are never silently lost.

``MARKET_TYPE`` decides which credential group you are prompted for:

  * ``FOREX``  -> "FOREX / MT5 Configuration"
  * ``CRYPTO`` -> "Crypto CEX Configuration"

The group that does not match the chosen market keeps its current/default
values untouched (it is written, just not prompted for).

Usage:
    uv run python scripts/init_env.py      # or:  make init
"""

from __future__ import annotations

import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Emit UTF-8 and never crash on a legacy console code page (e.g. Windows cp1252).
for _stream in (sys.stdout, sys.stderr):
  try:
    _stream.reconfigure(encoding="utf-8", errors="replace")
  except (AttributeError, ValueError):
    pass

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_PATH = ROOT / ".env.example"
ENV_PATH = ROOT / ".env"

# Market-specific groups gated by MARKET_TYPE. Keys in the selected market's
# group are prompted for; the other group is written with its existing/default
# value.
FOREX_KEYS = (
  "MT5_SERVER",
  "MT5_LOGIN",
  "MT5_PASSWORD",
  "MT5_PATH",
  "MT5_NAME",
  "FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL",
)
CRYPTO_KEYS = (
  "CRYPTO_EXCHANGE",
  "CRYPTO_QUOTE_ASSET",
  "CRYPTO_API_KEY",
  "CRYPTO_API_SECRET",
  "CRYPTO_ACCOUNT_ID",
  "CRYPTO_TESTNET",
  "CRYPTO_HEDGE_MODE",
  "CRYPTO_LEVERAGE_INIT_SYMBOLS",
  "MAX_LEVERAGE_CAP",
  "MIN_LEVERAGE_CAP",
)
# Variables presented as a fixed-choice menu instead of free text.
CHOICES = {
  "MARKET_TYPE": ("FOREX", "CRYPTO"),
  "NOTIFICATION_MODE": ("VERBOSE", "SILENT", "ERROR"),
}

# One-line hint shown above each prompt so the user knows what the value means.
DESCRIPTIONS = {
  "APP_HOST": "Host/interface the FastAPI server binds to",
  "APP_PORT": "Port the FastAPI server listens on",
  "BROKER_API_URL": "Base URL of the broker service this worker calls",
  "BROKER_API_KEY": "API key used to authenticate with the broker",
  "NATS_URL": "NATS server URL the worker connects to",
  "NATS_TOKEN": "Auth token for the NATS connection (blank if none)",
  "NATS_SUBJECTS": "Signal subjects to listen on, comma-separated",
  "MARKET_TYPE": "Which market this worker trades",
  "MT5_SERVER": "MT5 broker/trade server name",
  "MT5_LOGIN": "MT5 account login number",
  "MT5_PASSWORD": "MT5 account password",
  "MT5_PATH": "Path to terminal64.exe (blank -> auto-detect via registry)",
  "MT5_NAME": "Friendly label for this MT5 terminal/account",
  "FOREX_ALLOW_MULTI_STRATEGY_PER_SYMBOL": (
    "Let several strategies hold the same symbol (needs a HEDGING account "
    "+ STRATEGY_MAGIC_MAP)"
  ),
  "CRYPTO_EXCHANGE": "Crypto exchange gateway to use (currently BINANCE)",
  "CRYPTO_QUOTE_ASSET": "Quote asset appended to bare symbols (BTCUSD -> BTCUSDT)",
  "CRYPTO_API_KEY": "Exchange API key",
  "CRYPTO_API_SECRET": "Exchange API secret",
  "CRYPTO_ACCOUNT_ID": "Exchange account identifier (email)",
  "CRYPTO_TESTNET": "true -> use the exchange's futures testnet",
  "CRYPTO_HEDGE_MODE": "Position mode enforced at startup (true=Hedge, false=One-way)",
  "CRYPTO_LEVERAGE_INIT_SYMBOLS": "Comma-separated symbols to set leverage on at startup (empty = skip)",
  "MAX_LEVERAGE_CAP": "Upper bound applied by the leverage init job (e.g. 10)",
  "MIN_LEVERAGE_CAP": "Fallback floor when a -4421 cap can't be parsed (e.g. 5)",
  "SLIPPAGE_DEVIATION": "Max slippage in points (100 = $1.00)",
  "POSITION_TP1_PERCENT": "Percent of the position closed at the first take-profit",
  "TP1_MOVE_SL_TO_BREAKEVEN": "After TP1, move stop to breakeven (else keep entry SL)",
  "CAPITAL": "Initial capital used for risk sizing",
  "CAPITAL_CURRENCY": "Currency of the configured capital",
  "VOLUME_DECISION_ENABLED": "Enable automatic position-size calculation",
  "RISK_PERCENTAGE": "Percent of capital risked per trade",
  "USE_ACCOUNT_EQUITY": "Use live account equity instead of initial capital for sizing",
  "LOG_LEVEL": "Logging verbosity (DEBUG, INFO, WARNING, ERROR)",
  "NOTIFICATION_MODE": "How much the worker notifies",
  "TELEGRAM_ENABLED": "Enable Telegram notifications",
  "TELEGRAM_BOT_TOKEN": "Telegram bot token",
  "TELEGRAM_CHAT_ID": (
    "Chat IDs for management/service notifications, comma-separated "
    "(-100123_456 targets topic 456 of a group with Topics enabled)"
  ),
  "TELEGRAM_CHAT_CHANNEL_ID": (
    "Broadcast channel IDs, comma-separated (same _<topic id> syntax)"
  ),
  "TELEGRAM_LOG_CHAT_ID": (
    "Chat IDs for forwarded ERROR logs, comma-separated "
    "(blank -> falls back to TELEGRAM_CHAT_ID)"
  ),
}

KEY_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
SECTION_RULE = "# " + "─" * 65

# Values for these keys are always written with double quotes in .env
# so special characters (spaces, commas, colons, JSON braces) are protected.
QUOTED_KEYS = {
  "BROKER_API_KEY",
  "NATS_TOKEN",
  "NATS_SUBJECTS",
  "CRYPTO_API_KEY",
  "CRYPTO_API_SECRET",
  "CRYPTO_ACCOUNT_ID",
  "TELEGRAM_BOT_TOKEN",
  "TELEGRAM_CHAT_ID",
  "TELEGRAM_CHAT_CHANNEL_ID",
  "TELEGRAM_LOG_BOT_TOKEN",
  "TELEGRAM_LOG_CHAT_ID",
  "CRYPTO_LEVERAGE_INIT_SYMBOLS",
}


# ── Parsing ──────────────────────────────────────────────────────────────────
def split_value_comment(raw: str) -> tuple[str, str]:
  """Split the text after ``KEY=`` into ``(value, inline_comment)``.

  Quotes stay part of the value and a ``#`` only counts as a comment when it
  sits outside quotes, so JSON such as ``'{"MT5_GOLD": 1}'`` and quoted ids
  survive intact.
  """
  raw = raw.strip()
  if raw[:1] == "#":  # empty value, comment only (e.g. `KEY=  # note`)
    return "", raw
  if raw[:1] in ("'", '"'):
    quote = raw[0]
    end = raw.find(quote, 1)
    if end != -1:
      value = raw[: end + 1]
      rest = raw[end + 1 :]
      comment = rest[rest.find("#") :].strip() if "#" in rest else ""
      return value, comment
  match = re.search(r"\s+#", raw)
  if match:
    return raw[: match.start()].strip(), raw[match.start() :].strip()
  return raw, ""


def parse_env(path: Path) -> tuple[list, dict]:
  """Parse a dotenv file into ordered entries plus a ``key -> value`` map.

  Each entry is ``("raw", text)`` for comments/blank lines or
  ``("kv", key, value, comment)`` for assignments, so the original layout can
  be reproduced verbatim while values are updated.
  """
  entries: list = []
  values: dict = {}
  for line in path.read_text(encoding="utf-8").splitlines():
    match = KEY_RE.match(line)
    if match:
      key = match.group(1)
      value, comment = split_value_comment(match.group(2))
      entries.append(("kv", key, value, comment))
      values[key] = value
    else:
      entries.append(("raw", line))
  return entries, values


# ── Prompting ────────────────────────────────────────────────────────────────
def ask(key: str, default: str) -> str:
  """Prompt for ``key``: a choice menu when it has fixed options, else free text."""
  desc = DESCRIPTIONS.get(key)
  if key in CHOICES:
    return prompt_choice(key, default, CHOICES[key], desc)
  return prompt(key, default, desc)


def prompt(label: str, default: str, desc: str | None = None) -> str:
  """Ask for a single value, returning ``default`` on a blank line or EOF."""
  shown = default if default != "" else "(empty)"
  if desc:
    print(f"  # {desc}")
  try:
    answer = input(f"  {label} [{shown}]: ").strip()
  except EOFError:
    print()
    return default
  return answer if answer else default


def prompt_choice(
  label: str, default: str, options: tuple, desc: str | None = None
) -> str:
  """Ask the user to pick one of ``options`` (by number or name)."""
  default = default if default in options else options[0]
  if desc:
    print(f"  # {desc}")
  print(f"  {label} — choose one:")
  for i, choice in enumerate(options, 1):
    marker = "  <- current" if choice == default else ""
    print(f"    {i}) {choice}{marker}")
  while True:
    try:
      answer = input(f"  Select 1-{len(options)} or name [{default}]: ").strip()
    except EOFError:
      print()
      return default
    if not answer:
      return default
    if answer.isdigit() and 1 <= int(answer) <= len(options):
      return options[int(answer) - 1]
    if answer.upper() in options:
      return answer.upper()
    print(f"    ! Enter 1-{len(options)} or one of: {', '.join(options)}")


def confirm(question: str, default: bool = True) -> bool:
  suffix = "[Y/n]" if default else "[y/N]"
  try:
    answer = input(f"{question} {suffix} ").strip().lower()
  except EOFError:
    print()
    return default
  if not answer:
    return default
  return answer in ("y", "yes")


def section(title: str) -> None:
  print()
  print(f"-- {title} ".ljust(70, "-"))


# ── Rendering ────────────────────────────────────────────────────────────────
def format_kv(key: str, value: str, comment: str) -> str:
  already_quoted = value[:1] in ('"', "'") and value[-1:] == value[:1]
  if not already_quoted and key in QUOTED_KEYS:
    value = f'"{value}"'
  line = f"{key}={value}"
  return f"{line}  {comment}" if comment else line


def render(base_entries: list, result: dict, example_only: list) -> str:
  """Reproduce ``base_entries`` with updated values, then append new keys.

  ``example_only`` carries keys present in ``.env.example`` but missing from the
  current ``.env`` so upgrades pick up newly introduced settings without
  disturbing the existing layout.
  """
  lines: list[str] = []
  for entry in base_entries:
    if entry[0] == "raw":
      lines.append(entry[1])
    else:
      _, key, old_value, comment = entry
      lines.append(format_kv(key, result.get(key, old_value), comment))
  if example_only:
    lines.append("")
    lines.append(SECTION_RULE)
    lines.append("# Added from .env.example (new keys not in your previous .env)")
    lines.append(SECTION_RULE)
    for _, key, ex_value, comment in example_only:
      lines.append(format_kv(key, result.get(key, ex_value), comment))
  return "\n".join(lines).rstrip("\n") + "\n"


# ── Orchestration ────────────────────────────────────────────────────────────
def build_plan(updating: bool):
  """Assemble the editing plan: base layout, ordered keys, and a default lookup.

  ``base_entries`` is the file we edit in place (the current ``.env`` when it
  exists, otherwise ``.env.example``). ``example_only`` are keys that exist in
  ``.env.example`` but not yet in ``.env`` so upgrades pick them up.
  """
  example_entries, example_values = parse_env(EXAMPLE_PATH)
  base_entries, base_values = parse_env(ENV_PATH) if updating else (example_entries, {})

  def default_for(key: str) -> str:
    # Prefer what the current .env already has; fall back to .env.example.
    return base_values.get(key, example_values.get(key, ""))

  base_kv = [e for e in base_entries if e[0] == "kv"]
  base_key_set = {e[1] for e in base_kv}
  example_only = [
    e for e in example_entries if e[0] == "kv" and e[1] not in base_key_set
  ]
  ordered_keys = [e[1] for e in base_kv] + [e[1] for e in example_only]
  return base_entries, example_only, ordered_keys, default_for


def collect_values(ordered_keys, default_for):
  """Prompt for every value, gating the market-specific credential group."""
  result: dict = {}
  group_keys = set(FOREX_KEYS) | set(CRYPTO_KEYS)

  # Phase 1 — everything outside the two market credential groups.
  section("General configuration")
  market_type = default_for("MARKET_TYPE") or CHOICES["MARKET_TYPE"][0]
  for key in ordered_keys:
    if key in group_keys:
      continue
    result[key] = ask(key, default_for(key))
    if key == "MARKET_TYPE":
      market_type = result[key]

  # Phase 2 — only the credential group the chosen market requires; the other
  # group keeps its current/default value untouched.
  if market_type == "FOREX":
    section("FOREX / MT5 Configuration  (MARKET_TYPE=FOREX)")
    prompt_keys, keep_keys = FOREX_KEYS, CRYPTO_KEYS
  else:
    section("Crypto CEX Configuration  (MARKET_TYPE=CRYPTO)")
    prompt_keys, keep_keys = CRYPTO_KEYS, FOREX_KEYS
  for key in prompt_keys:
    result[key] = ask(key, default_for(key))
  for key in keep_keys:
    result[key] = default_for(key)
  return result, market_type


def write_env(base_entries, result, example_only, updating: bool) -> None:
  """Back up any existing file and write the rendered .env, preserving newlines."""
  if updating:
    backup = ENV_PATH.parent / f".env.bak-{datetime.now():%Y%m%d-%H%M%S}"
    shutil.copy2(ENV_PATH, backup)
    print(f"  Backup : {backup.name}")

  # Match the source file's line endings so we don't flip the whole file
  # (Python would otherwise translate "\n" to CRLF on Windows).
  source = ENV_PATH if updating else EXAMPLE_PATH
  newline = "\r\n" if b"\r\n" in source.read_bytes() else "\n"
  content = render(base_entries, result, example_only)
  if newline != "\n":
    content = content.replace("\n", newline)
  ENV_PATH.write_text(content, encoding="utf-8", newline="")
  print(f"  Written: {ENV_PATH.name}")


def main() -> int:
  if not EXAMPLE_PATH.exists():
    print(f"error: {EXAMPLE_PATH} not found — cannot derive the template.")
    return 1

  updating = ENV_PATH.exists()
  base_entries, example_only, ordered_keys, default_for = build_plan(updating)

  print()
  print("================ .env initializer ================")
  print(f"  Mode    : {'update existing .env' if updating else 'create new .env'}")
  print(f"  Template: {EXAMPLE_PATH.name}")
  print("  Press Enter to keep the [default] (current .env value, else .env.example).")

  result, market_type = collect_values(ordered_keys, default_for)

  section("Summary")
  print(f"  MARKET_TYPE             = {result.get('MARKET_TYPE')}")
  print(
    f"  Active credential group = {'MT5' if market_type == 'FOREX' else 'Crypto / Binance'}"
  )
  print()
  if not confirm(f"Write these values to {ENV_PATH.name}?", default=True):
    print("Aborted — nothing written.")
    return 1

  write_env(base_entries, result, example_only, updating)
  return 0


if __name__ == "__main__":
  try:
    sys.exit(main())
  except KeyboardInterrupt:
    print("\nAborted.")
    sys.exit(130)
