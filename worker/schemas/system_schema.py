from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, ValidationError, model_validator


class SystemActionEnum(str, Enum):
  # Inbound (broker → worker), broadcast on the shared SYSTEM subject: re-run
  # per-symbol leverage init on a CEX after an admin changes the account's
  # leverage settings. This is the *runtime* path only — the connect-time push
  # rides in the WORKER_CONNECTED_ACK's ``crypto_leverage_init`` section, since
  # a request/reply inbox delivers exactly one message.
  CRYPTO_LEVERAGE_INIT = "CRYPTO_LEVERAGE_INIT"
  # Inbound (broker → worker), broadcast on the shared SYSTEM subject: apply the
  # account's settings after its owner changed one from the Telegram bot, or an
  # admin changed a broker-wide default, while this worker was already
  # connected. Same block and same code path as the ACK's ``settings`` section,
  # which is the connect-time half of the same flow.
  WORKER_SETTINGS_UPDATE = "WORKER_SETTINGS_UPDATE"
  # Outbound (worker → broker): announced right after the worker connects to
  # NATS so the broker can push any initial config targeted at this worker.
  # Sent as a request/reply, not a broadcast — see worker.schemas.inbox_schema.
  WORKER_CONNECTED = "WORKER_CONNECTED"
  # Inbound (broker → worker): the single reply to WORKER_CONNECTED, carrying
  # every piece of init config this worker needs — see
  # worker.schemas.inbox_schema.WorkerConnectedAckSchema. NATS request/reply
  # delivers exactly one message, so everything the broker wants to push on
  # connect rides in here.
  WORKER_CONNECTED_ACK = "WORKER_CONNECTED_ACK"
  # Inbound (broker → worker): reply to WORKER_CONNECTED when the broker
  # received the handshake but could not process it (missing settings,
  # invalid leverage, ...).
  WORKER_CONNECTED_ERROR = "WORKER_CONNECTED_ERROR"


def _why(err: ValidationError) -> str:
  """Compact one-line reason from a ValidationError — a dropped entry is logged
  per line, so a full multi-line pydantic report would drown the log."""
  first = err.errors()[0]
  loc = ".".join(str(p) for p in first.get("loc", ())) or "value"
  more = f" (+{err.error_count() - 1} more)" if err.error_count() > 1 else ""
  return f"{loc}: {first.get('msg', 'invalid')}{more}"


class SystemSchema(BaseModel):
  action: SystemActionEnum
  timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
  # Target worker identity in NATS-name format "<market>-<gateway>-<account_id>".
  # Required: every SYSTEM message is addressed to a specific worker, and only
  # the worker whose identity matches executes it.
  account_id: str


class WorkerSettingsSchema(BaseModel):
  """The account's settings as the broker holds them, delivered on **two**
  paths:

  * **connect-time** — nested as the ``settings`` section of
    :class:`~worker.schemas.inbox_schema.WorkerConnectedAckSchema`, re-pushed on
    every connect;
  * **runtime** — nested in :class:`SystemWorkerSettingsUpdateSchema` and
    broadcast on the shared ``SYSTEM`` subject when an owner changes a value
    from the Telegram bot (or an admin changes a broker-wide default) after the
    worker is already connected.

  Both carry the same block and are applied through the same code path, so a
  value cannot mean one thing live and another on the next reconnect.

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

  #: How many positions this account may hold open at once — the broker's value
  #: for MAX_OPEN_ORDERS, which is no longer an .env setting. 0 disables the cap
  #: and is a real value, not an absence, hence ``ge=0``: the broker resolves an
  #: account that set no cap of its own against its own global default before
  #: sending, so what arrives here is always a concrete number. None == not
  #: pushed, which leaves the worker on its built-in default (5).
  max_open_orders: Optional[int] = Field(default=None, ge=0)


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
    name = str(key)
    if name not in known:
      dropped.append(f"settings[{name!r}] — unknown setting, this worker ignores it")
      continue
    try:
      # Validated through the model itself, one field at a time, rather than
      # against the bare annotation: a field's own constraints live on the field
      # and not on its type (``max_open_orders`` is ``ge=0``), so an
      # annotation-only check would accept a value the model then rejects — and
      # that rejection, raised from the construction below, would escape this
      # salvage and fail the whole envelope, which is the one outcome salvaging
      # exists to prevent. Every other field keeps its default here; only this
      # one is under test.
      WorkerSettingsSchema(**{name: value})
    except ValidationError as err:
      dropped.append(f"settings[{name!r}] — {_why(err)}")
      continue
    kept[name] = value

  # Cannot raise: every key left has already validated on its own.
  return WorkerSettingsSchema(**kept) if kept else None


class SystemWorkerSettingsUpdateSchema(SystemSchema):
  """Standalone SYSTEM broadcast carrying the account's settings — the runtime
  counterpart of the ACK's ``settings`` section.

  Sent when an owner changes a value from the Telegram bot, or an admin changes
  a broker-wide default, after the handshake has already happened. The worker is
  subscribed to ``SYSTEM`` for its whole lifetime, so a broadcast here always
  lands (unlike the one-shot request inbox), and ``account_id`` scopes it to a
  single worker.

  The broker sends the whole block rather than only the field that changed, and
  the worker applies it through the same helper the ACK's section goes through —
  one behaviour, one notification, whichever path a value arrives on.

  ``settings`` is salvaged field by field like the ACK's (see
  :func:`_salvage_settings`), so a malformed value costs only its own setting;
  nothing usable leaves ``settings`` ``None``, which reads as "nothing pushed".
  """

  action: SystemActionEnum = SystemActionEnum.WORKER_SETTINGS_UPDATE
  settings: Optional[WorkerSettingsSchema] = None
  # Why any field was discarded, for the worker to log. Not part of the wire
  # format — it describes this parse, so it never serialises back out.
  dropped: list[str] = Field(default_factory=list, exclude=True)

  @model_validator(mode="before")
  @classmethod
  def _salvage(cls, data: Any) -> Any:
    if not isinstance(data, dict):
      return data
    data = dict(data)
    dropped: list[str] = []
    if data.get("settings") is not None:
      data["settings"] = _salvage_settings(data["settings"], dropped)
    data["dropped"] = dropped
    return data


class CryptoLeverageInitSchema(BaseModel):
  """The crypto leverage-init payload, delivered on **two** paths:

  * **connect-time** — nested as the ``crypto_leverage_init`` section of
    :class:`~worker.schemas.inbox_schema.WorkerConnectedAckSchema`, because the
    handshake's reply inbox delivers exactly one message;
  * **runtime** — flattened into :class:`SystemCryptoLeverageInitSchema` and
    broadcast on the shared ``SYSTEM`` subject when an admin changes the
    account's leverage settings after the worker is already connected.

  Both fields are optional overrides: omitted, each falls back to the worker's
  own configured default (``CRYPTO_LEVERAGE_INIT_SYMBOLS`` / ``MAX_LEVERAGE_CAP``).
  ``default_leverage`` acts as a **cap** — each symbol is set to
  ``min(exchange_max, default_leverage)``."""

  symbols: Optional[list[str]] = None
  default_leverage: Optional[int] = None


class SystemCryptoLeverageInitSchema(SystemSchema, CryptoLeverageInitSchema):
  """Standalone SYSTEM broadcast asking a connected worker to re-run the
  leverage-init pass — the runtime counterpart of the ACK's
  ``crypto_leverage_init`` section, carrying the same
  :class:`CryptoLeverageInitSchema` fields flattened into the envelope.

  Sent when an admin edits a sub-account's leverage cap or onboards new symbols,
  i.e. after the handshake has already happened. The worker is subscribed to
  ``SYSTEM`` for its whole lifetime, so a broadcast here always lands (unlike the
  one-shot request inbox), and ``account_id`` scopes it to a single worker."""

  action: SystemActionEnum = SystemActionEnum.CRYPTO_LEVERAGE_INIT
