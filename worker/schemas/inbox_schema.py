"""
worker/schemas/inbox_schema.py
───────────────────────────────
The WORKER_CONNECTED handshake — a NATS **request/reply**, not a SYSTEM
broadcast. ``BaseSignalProcessor._announce_worker_connected`` sends
:class:`WorkerConnectedSchema` as an ``nc.request`` (on the SYSTEM subject, but
resolved via a private reply inbox NATS creates for the request) and the broker
answers on that inbox with exactly one message — either
:class:`WorkerConnectedAckSchema` or :class:`WorkerConnectedErrorSchema`.

That one-message-only delivery is why ``WorkerConnectedAckSchema`` bundles
every piece of connect-time config in a single payload, unlike the runtime
SYSTEM broadcasts in ``system_schema.py`` (which the worker stays subscribed to
for its whole lifetime and which can therefore be split across several
messages).

Both envelopes below reuse :class:`~worker.schemas.system_schema.SystemSchema`
(the ``action``/``timestamp``/``account_id`` envelope) and
:class:`~worker.schemas.system_schema.CryptoLeverageInitSchema` (the
``crypto_leverage_init`` section, shared with the runtime broadcast) from
``system_schema.py``.
"""

from typing import Any, Callable, Optional

from pydantic import BaseModel, Field, TypeAdapter, ValidationError, model_validator

from worker.schemas.signal_schema import SignalSchema
from worker.schemas.system_schema import (
  CryptoLeverageInitSchema,
  SystemActionEnum,
  SystemSchema,
)

_MAGIC_ADAPTER = TypeAdapter(int)


def _why(err: ValidationError) -> str:
  """Compact one-line reason from a ValidationError — a dropped ACK entry is
  logged per line, so a full multi-line pydantic report would drown the log."""
  first = err.errors()[0]
  loc = ".".join(str(p) for p in first.get("loc", ())) or "value"
  more = f" (+{err.error_count() - 1} more)" if err.error_count() > 1 else ""
  return f"{loc}: {first.get('msg', 'invalid')}{more}"


def _salvage_section(
  raw: Any, validate: Callable[[Any], Any], label: str, dropped: list[str]
):
  """Validate one whole ACK section, dropping only that section on failure."""
  try:
    return validate(raw)
  except ValidationError as err:
    dropped.append(f"{label} — {_why(err)}")
    return None


def _salvage_magic_map(raw: Any, dropped: list[str]) -> Optional[dict[str, int]]:
  """Keep every parseable strategy→magic entry, discarding only the bad ones.

  Salvaging nothing from a *non-empty* push returns ``None`` — leave the worker's
  current map alone — rather than ``{}``: an empty map is the broker's explicit
  "clear my magics" instruction, and a parse failure must never be mistaken for
  one (that would strip the magics the worker is actively trading under and make
  it reject every signal as an unknown strategy)."""
  if not isinstance(raw, dict):
    dropped.append("strategy_magic_map — not an object")
    return None
  kept: dict[str, int] = {}
  for strategy, magic in raw.items():
    try:
      kept[str(strategy)] = _MAGIC_ADAPTER.validate_python(magic)
    except ValidationError as err:
      dropped.append(f"strategy_magic_map[{strategy!r}] — {_why(err)}")
  return kept if (kept or not raw) else None


def _salvage_signals(raw: Any, dropped: list[str]) -> list[SignalSchema]:
  """Keep every parseable replay entry, discarding only the bad ones — a batch
  exists to fill gaps, so one malformed signal must not cancel the rest."""
  if raw is None:
    return []
  if not isinstance(raw, list):
    dropped.append("retry_signals — not a list")
    return []
  kept: list[SignalSchema] = []
  for index, entry in enumerate(raw):
    try:
      kept.append(SignalSchema.model_validate(entry))
    except ValidationError as err:
      dropped.append(f"retry_signals[{index}] — {_why(err)}")
  return kept


class WorkerSettingsSchema(BaseModel):
  """The ``settings`` section of a WORKER_CONNECTED_ACK — the runtime toggles the
  broker holds for this account, re-pushed on every connect.

  These are the same switches the runtime ADMIN directives flip
  (``BLOCK_SIGNAL`` / ``ALLOW_SIGNAL`` → ``signal_blocked``). The worker keeps
  them in memory only, so a restart would otherwise silently drop back to the
  defaults and start trading again under a block an operator never lifted. The
  broker owns the state, so it replays it here and the worker re-applies it
  through the very same code path the ADMIN action uses — one behaviour, one
  notification, whichever way the value arrives.

  Every field is ``Optional`` and defaults to ``None`` meaning **"not pushed —
  leave this setting as it is"**, so a broker that knows nothing about a given
  toggle simply omits it. That is why ``signal_blocked`` is a nullable bool
  rather than a plain one: ``false`` is the broker explicitly saying "unblocked",
  which is not the same statement as saying nothing at all.

  Fields are salvaged one by one (see :func:`_salvage_settings`), so a malformed
  value costs only its own setting.
  """

  #: Whether signal execution is suspended for this worker. True == the account
  #: is under a BLOCK_SIGNAL; False == explicitly allowed; None == not pushed.
  signal_blocked: Optional[bool] = None


def _salvage_settings(raw: Any, dropped: list[str]) -> Optional[WorkerSettingsSchema]:
  """Validate the ``settings`` section one field at a time.

  Each toggle is independent live-trading state, so they are salvaged like the
  magic map's entries rather than as one block: a broker that sends a malformed
  value for one setting must not cost the worker every *other* setting in the
  same push (losing a valid ``signal_blocked: true`` that way would resume
  trading on an account an operator had stopped).

  A key the worker does not know is reported too — the broker believes that
  setting is in force while this worker silently ignores it, which is exactly the
  kind of version skew an operator needs to see. Returns ``None`` when nothing
  usable survived, which reads downstream as "no settings pushed".
  """
  if not isinstance(raw, dict):
    dropped.append("settings — not an object")
    return None

  known = WorkerSettingsSchema.model_fields
  kept: dict[str, Any] = {}
  for key, value in raw.items():
    field = known.get(str(key))
    if field is None:
      dropped.append(f"settings[{key!r}] — unknown setting, this worker ignores it")
      continue
    try:
      kept[str(key)] = TypeAdapter(field.annotation).validate_python(value)
    except ValidationError as err:
      dropped.append(f"settings[{key!r}] — {_why(err)}")

  return WorkerSettingsSchema(**kept) if kept else None


class WorkerConnectedSchema(SystemSchema):
  """Handshake a worker publishes as a NATS **request** right after it connects
  to NATS. ``account_id`` is the worker identity in "<market>-<gateway>-<id>"
  form; ``market`` and ``gateway`` are also sent so the broker can decide,
  without parsing the identity string, which init sections belong in the ACK
  (e.g. ``crypto_leverage_init``) for this worker.

  ``strategies`` is the list of strategy names this worker is subscribed to
  (derived from ``NATS_SUBJECTS`` minus the always-listened control subjects
  ADMIN/SYSTEM). The broker uses it to scope the ``strategy_magic_map`` and the
  ``retry_signals`` replay it sends back in the ACK."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED
  market: str
  gateway: str
  strategies: list[str] = Field(default_factory=list)


class WorkerConnectedAckSchema(SystemSchema):
  """The broker's **single** reply to WORKER_CONNECTED, carrying all of this
  worker's connect-time config in one payload.

  NATS request/reply resolves on the *first* message delivered to the request's
  reply inbox and then unsubscribes it, so a broker that publishes several
  messages there has all but the first silently dropped — never received, never
  logged. Everything the broker wants to push on connect therefore rides in this
  one envelope rather than as separate SYSTEM messages.

  ``strategy_magic_map`` maps each strategy name (a subject in the worker's
  ``NATS_SUBJECTS``) to its dedicated MT5 magic number, replacing the static
  ``STRATEGY_MAGIC_MAP`` .env config: the broker owns the mapping centrally and
  the worker keeps only the entries for the strategies it actually subscribes to,
  storing them in settings from where the executor and ``PositionCDC`` read them
  exactly as they read the old .env value. Omitting the field (or ``null``)
  leaves the worker's current map untouched; an explicit ``{}`` clears it.

  ``settings`` restores the runtime toggles an operator set on this account in a
  previous session (currently ``signal_blocked``, the ADMIN
  ``BLOCK_SIGNAL``/``ALLOW_SIGNAL`` gate) — see :class:`WorkerSettingsSchema`.
  The worker holds those in memory only, so without this replay a restart would
  quietly resume trading under a block nobody lifted. Omitting the section (or
  any single field in it) leaves the corresponding setting at its current value.

  ``crypto_leverage_init`` (CRYPTO only) asks the worker to re-initialise
  per-symbol leverage on the exchange before it trades — see
  :class:`~worker.schemas.system_schema.CryptoLeverageInitSchema`. Omitting it
  skips the connect-time pass; a later change is broadcast separately as a
  SYSTEM ``CRYPTO_LEVERAGE_INIT``.

  ``retry_signals`` replays recent signals for those same strategies so a worker
  reconnecting after an outage catches up. Each entry is a full
  :class:`SignalSchema`; the worker dedupes by ``signal_id`` against its local DB
  and only executes signals still within ``MAX_RETRY_TIMEOUT`` of their own
  ``timestamp`` — an older replay is dropped rather than fired late. Omitted,
  ``null`` or ``[]`` all mean "nothing to replay".

  A worker that needs no init config at all receives this envelope with every
  section absent — the handshake is complete on receipt, as before.

  Sections are **salvaged independently** — see :meth:`_salvage_sections`.
  Whatever survives lands in the typed fields; ``dropped`` explains the rest."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED_ACK
  settings: Optional[WorkerSettingsSchema] = None
  strategy_magic_map: Optional[dict[str, int]] = None
  crypto_leverage_init: Optional[CryptoLeverageInitSchema] = None
  retry_signals: list[SignalSchema] = Field(default_factory=list)
  # Why any section or entry above was discarded, for the worker to log. Not part
  # of the wire format — it describes this parse, so it never serialises back out.
  dropped: list[str] = Field(default_factory=list, exclude=True)

  @model_validator(mode="before")
  @classmethod
  def _salvage_sections(cls, data: Any) -> Any:
    """Validate each section — and each of their entries — independently, so one
    fault never costs more than itself.

    Every section carries live trading config, and merging them into a single
    payload means a strict all-or-nothing parse would let any one take the others
    down: an unparseable replay would stop the magic map from landing, and
    without a magic map a FOREX worker rejects *every* signal as an unknown
    strategy. One malformed signal likewise must not cancel the gap-fill the rest
    of the batch provides. So each entry is validated on its own and only the bad
    ones are discarded, each with a reason recorded in ``dropped``.

    Each section's salvage rule lives in its own ``_salvage_*`` helper above.
    """
    if not isinstance(data, dict):
      return data

    data = dict(data)
    dropped: list[str] = []

    if data.get("settings") is not None:
      data["settings"] = _salvage_settings(data["settings"], dropped)
    if data.get("strategy_magic_map") is not None:
      data["strategy_magic_map"] = _salvage_magic_map(
        data["strategy_magic_map"], dropped
      )
    if data.get("crypto_leverage_init") is not None:
      data["crypto_leverage_init"] = _salvage_section(
        data["crypto_leverage_init"],
        CryptoLeverageInitSchema.model_validate,
        "crypto_leverage_init",
        dropped,
      )
    data["retry_signals"] = _salvage_signals(data.get("retry_signals"), dropped)

    data["dropped"] = dropped
    return data


class WorkerConnectedErrorSchema(SystemSchema):
  """Broker reply to WORKER_CONNECTED when it received the handshake but could
  not process it (e.g. missing settings, invalid leverage config)."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED_ERROR
  reason: Optional[str] = None
