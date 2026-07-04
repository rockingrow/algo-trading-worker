from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SystemActionEnum(str, Enum):
  # Inbound (broker → worker): re-initialise per-symbol leverage on a CEX.
  CRYPTO_LEVERAGE_INIT = "CRYPTO_LEVERAGE_INIT"
  # Outbound (worker → broker): announced right after the worker connects to
  # NATS so the broker can push any initial config targeted at this worker.
  WORKER_CONNECTED = "WORKER_CONNECTED"
  # Inbound (broker → worker): reply to WORKER_CONNECTED for a worker that
  # needs no init config (e.g. non-crypto) — handshake is complete as-is.
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


class SystemCryptoLeverageInitSchema(SystemSchema):
  action: SystemActionEnum = SystemActionEnum.CRYPTO_LEVERAGE_INIT
  symbols: Optional[list[str]] = None
  default_leverage: Optional[int] = None


class SystemWorkerConnectedSchema(SystemSchema):
  """Handshake a worker publishes on the SYSTEM subject right after it connects
  to NATS. ``account_id`` is the worker identity in "<market>-<gateway>-<id>"
  form; ``market`` and ``gateway`` are also sent so the broker can decide,
  without parsing the identity string, whether to reply with an init action
  (e.g. CRYPTO_LEVERAGE_INIT) for this worker."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED
  market: str
  gateway: str


class SystemWorkerConnectedAckSchema(SystemSchema):
  """Broker reply to WORKER_CONNECTED for a worker that needs no init config
  (e.g. non-crypto) — the handshake is complete on receipt."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED_ACK


class SystemWorkerConnectedErrorSchema(SystemSchema):
  """Broker reply to WORKER_CONNECTED when it received the handshake but could
  not process it (e.g. missing settings, invalid leverage config)."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED_ERROR
  reason: Optional[str] = None
