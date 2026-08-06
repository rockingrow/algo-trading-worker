from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from worker.schemas.signal_schema import SignalSchema


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
  # Inbound (broker → worker): replay a batch of recent signals for the
  # strategies this worker is subscribed to. Each signal is deduped by
  # signal_id and only executed when still within MAX_RETRY_TIMEOUT of its
  # own timestamp — a stale replay is dropped rather than fired late.
  RETRY_SIGNALS = "RETRY_SIGNALS"
  # Inbound (broker → worker): push the per-strategy magic-number map. Replaces
  # the static STRATEGY_MAGIC_MAP .env config — the broker owns the mapping and
  # pushes it (as the WORKER_CONNECTED reply, or standalone on SYSTEM), scoped to
  # the strategies this worker subscribes to. The worker stores it in settings so
  # the executor / PositionCDC resolve magics from it exactly as before.
  STRATEGY_MAGIC_MAP = "STRATEGY_MAGIC_MAP"


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
  (e.g. CRYPTO_LEVERAGE_INIT) for this worker.

  ``strategies`` is the list of strategy names this worker is subscribed to
  (derived from ``NATS_SUBJECTS`` minus the always-listened control subjects
  ADMIN/SYSTEM). The broker uses it to decide which recent signals to replay
  in a RETRY_SIGNALS response after the handshake."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED
  market: str
  gateway: str
  strategies: list[str] = Field(default_factory=list)


class SystemWorkerConnectedAckSchema(SystemSchema):
  """Broker reply to WORKER_CONNECTED for a worker that needs no init config
  (e.g. non-crypto) — the handshake is complete on receipt."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED_ACK


class SystemWorkerConnectedErrorSchema(SystemSchema):
  """Broker reply to WORKER_CONNECTED when it received the handshake but could
  not process it (e.g. missing settings, invalid leverage config)."""

  action: SystemActionEnum = SystemActionEnum.WORKER_CONNECTED_ERROR
  reason: Optional[str] = None


class SystemRetrySignalsSchema(SystemSchema):
  """Broker → worker replay of recent signals for the strategies this worker
  subscribes to. Sent either as a WORKER_CONNECTED reply (fill any gap the
  worker missed while offline) or on-demand on the SYSTEM subject.

  Each entry in ``signals`` is a full :class:`SignalSchema`. The worker dedupes
  by ``signal_id`` against its local DB and only executes signals still within
  ``MAX_RETRY_TIMEOUT`` of their own ``timestamp`` — an older replay is dropped
  rather than fired late."""

  action: SystemActionEnum = SystemActionEnum.RETRY_SIGNALS
  signals: list[SignalSchema] = Field(default_factory=list)


class SystemStrategyMagicMapSchema(SystemSchema):
  """Broker → worker push of the per-strategy magic-number map.

  ``strategy_magic_map`` maps each strategy name (a subject in the worker's
  ``NATS_SUBJECTS``) to its dedicated MT5 magic number. It replaces the static
  ``STRATEGY_MAGIC_MAP`` .env config: the broker owns the mapping centrally and
  pushes it — normally as the reply to this worker's ``WORKER_CONNECTED``
  handshake (before trading starts), or standalone on the ``SYSTEM`` subject when
  the mapping changes. The worker keeps only the entries for the strategies it
  actually subscribes to and stores them in settings, from where the executor and
  ``PositionCDC`` read them exactly as they read the old .env value. Sending an
  empty map clears the worker's magics."""

  action: SystemActionEnum = SystemActionEnum.STRATEGY_MAGIC_MAP
  strategy_magic_map: dict[str, int] = Field(default_factory=dict)
