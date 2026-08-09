from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SystemActionEnum(str, Enum):
  # Inbound (broker → worker), broadcast on the shared SYSTEM subject: re-run
  # per-symbol leverage init on a CEX after an admin changes the account's
  # leverage settings. This is the *runtime* path only — the connect-time push
  # rides in the WORKER_CONNECTED_ACK's ``crypto_leverage_init`` section, since
  # a request/reply inbox delivers exactly one message.
  CRYPTO_LEVERAGE_INIT = "CRYPTO_LEVERAGE_INIT"
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


class SystemSchema(BaseModel):
  action: SystemActionEnum
  timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
  # Target worker identity in NATS-name format "<market>-<gateway>-<account_id>".
  # Required: every SYSTEM message is addressed to a specific worker, and only
  # the worker whose identity matches executes it.
  account_id: str


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
