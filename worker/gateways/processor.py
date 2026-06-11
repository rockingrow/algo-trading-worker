"""
worker/gateways/processor.py
────────────────────────────
Market-agnostic signal-processor skeleton (Template Method pattern).

Both the FOREX (MT5) and CRYPTO (CEX) gateways run the *same* algorithm:

    connect → subscribe to NATS → for each message:
        ADMIN  → validate + reconcile a FLAT
        SIGNAL → handle, persist the position, notify

Only a handful of steps actually differ per broker (how to connect, how to read
the account footer, which jobs to start, how a FLAT reconciles against the
broker's live positions, …). :class:`BaseSignalProcessor` implements the
invariant skeleton once and declares those variation points as ``@abstractmethod``
hooks, so every concrete ``<Market>SignalProcessor`` *must* define them and can
share everything else.

The base imports no broker SDK (no MetaTrader5, no exchange client), so it is
safe to import on any market's path.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pydantic import ValidationError

from worker.context import WorkerContext
from worker.gateways.config import ExecutionConfig
from worker.gateways.market_strategy import MarketStrategyFactory
from worker.gateways.signal_handler import SignalHandler
from worker.interfaces.trade_presenter_protocol import TradePresenterProtocol
from worker.logger import get_logger
from worker.schemas.job_schema import LogAuthorEnum
from worker.schemas.nats_schema import NatsSubjectEnum
from worker.schemas.position_schema import PositionStatusEnum
from worker.schemas.signal_schema import SignalSchema
from worker.services.nats_service import NATSPublisher, NATSSubscriber
from worker.settings import NATS_REQUIRED_LISTENING_SUBJECTS, MarketTypeEnum

log = get_logger("worker.gateways.processor")

# Exit action → DB status, shared by every market.
_CLOSE_STATUS_MAP: Dict[str, PositionStatusEnum] = {
  "TP1": PositionStatusEnum.TP1,
  "TP2": PositionStatusEnum.TP2,
  "SL": PositionStatusEnum.SL,
  "R_SL": PositionStatusEnum.R_SL,
  "FLAT": PositionStatusEnum.FLATTED,
}


def parse_nats_subjects(raw: str) -> list[str | NatsSubjectEnum]:
  """Parse a comma-separated subject string, always including the required ones."""
  parsed: set[str | NatsSubjectEnum] = set()
  for s in raw.split(","):
    s = s.strip()
    if not s:
      continue
    try:
      parsed.add(NatsSubjectEnum(s))
    except ValueError:
      parsed.add(s)
  return list(NATS_REQUIRED_LISTENING_SUBJECTS | parsed)


class BaseSignalProcessor(ABC):
  """Template-method base for every ``<Market>SignalProcessor``.

  Subclasses bind a concrete presenter via the ``presenter`` class attribute and
  implement the abstract hooks below. The shared methods (``connect``,
  ``shutdown``, ``run``, ``start_market_jobs``, ``_process_message``,
  startup/shutdown notifications) are final in spirit and should not be
  overridden.
  """

  #: Short label used in log lines, e.g. ``"MT5"`` / ``"CRYPTO"``.
  name: str = "BASE"
  #: Concrete presenter (conforms to :class:`TradePresenterProtocol`).
  presenter: type[TradePresenterProtocol]

  def __init__(self, ctx: WorkerContext, settings_dict: dict) -> None:
    self.ctx = ctx
    self.settings = settings_dict
    self.config = ExecutionConfig.from_dict(settings_dict)

    # Build the broker executor (hook), then the shared strategy/handler stack.
    self.executor = self._build_executor()
    self.strategy = MarketStrategyFactory.create(
      settings_dict.get("market_type", MarketTypeEnum.FOREX),
      executor=self.executor,
      config=self.config,
    )
    self.handler = SignalHandler(self.strategy, ctx.db_service)

    self.subscriber: Optional[NATSSubscriber] = None
    self.publisher: Optional[NATSPublisher] = None
    self._footer: str = ""
    self._market_type: str = self._market_type_value(settings_dict.get("market_type"))

  # ── Shared lifecycle ──────────────────────────────────────────────────── #

  def connect(self) -> bool:
    if not self._connect_broker():
      log.error("[%s Process] Could not connect to broker. Exiting.", self.name)
      return False

    self.subscriber = NATSSubscriber(
      url=self.settings["nats_url"],
      subjects=parse_nats_subjects(self.settings.get("nats_subjects", "")),
      publish_subjects=[NatsSubjectEnum.TRADE],
      token=self.settings.get("nats_token"),
      account_footer_fn=self._account_footer,
      enqueue_fn=self.ctx.nats_enqueue,
    )
    self.subscriber.connect()

    self.publisher = NATSPublisher(
      url=self.settings["nats_url"],
      publish_subjects=[NatsSubjectEnum.TRADE],
      token=self.settings.get("nats_token"),
    )
    self.publisher.connect()

    self._footer = self._account_footer()
    return True

  def shutdown(self) -> None:
    if self.subscriber is not None:
      self.subscriber.close()
    if self.publisher is not None:
      self.publisher.close()
    self._disconnect_broker()

  def send_startup_notification(self) -> None:
    self.ctx.direct_notifier.send_message(
      self.presenter.startup(self.settings, self._footer)
    )

  def send_shutdown_notification(self) -> None:
    self.ctx.direct_notifier.send_message(self.presenter.shutdown(self._footer))

  def start_market_jobs(self, stop_event) -> None:
    from worker.jobs.cdc_job import PositionCDC

    # Change-data-capture → NATS TRADE is shared by every market.
    PositionCDC(
      account_id=self._account_id,
      publisher=self.publisher,
      db_service=self.ctx.db_service,
      market_type=self.settings.get("market_type"),
      **self._position_cdc_kwargs(),
    ).start(stop_event=stop_event)

    # Broker-specific jobs (health thread / terminal-close / user data stream).
    self._start_broker_jobs(stop_event)

  def run(self, stop_event) -> None:
    for subject, raw in self.subscriber.listen(stop_event=stop_event):
      self._process_message(subject, raw)

  # ── Shared message processing ─────────────────────────────────────────── #

  def _process_message(self, subject, raw) -> None:
    if subject == NatsSubjectEnum.ADMIN:
      self._handle_admin_message(raw)
      return

    try:
      signal = SignalSchema(**json.loads(raw))
    except json.JSONDecodeError as err:
      log.error("[%s Process] Malformed JSON: %s", self.name, err)
      return
    except ValidationError as err:
      log.error("[%s Process] Signal validation failed: %s", self.name, err)
      return

    if not self._ensure_connected():
      return

    log.info(
      "[%s Process] Processing Signal: %s | %s | TV Time: %s",
      self.name, signal.symbol, signal.action.value, signal.timestamp,
    )

    result = self.handler.handle(signal)
    footer = self._account_footer()

    self.ctx.db_service.log_position(
      strategy=signal.strategy,
      ref_id=result.get("ticket"),
      ref_source_id=result.get("source_ticket") or result.get("ticket"),
      symbol=signal.symbol,
      action=signal.action.value,
      volume=result.get("volume", signal.quantity),
      price=result.get("price", signal.price),
      sl=getattr(signal, "sl", None),
      tp1=getattr(signal, "tp1", None),
      gateway_return_code=result.get("retcode", -1),
      comment=result.get("comment", ""),
      message=signal.model_dump_json(),
      author=LogAuthorEnum.BROKER.value,
      market_type=self._market_type,
    )

    if result.get("success"):
      self._persist_success(signal, result, footer)
      msg = self.presenter.order_filled(
        signal, result, result.get("source_ticket") or result.get("ticket"), footer
      )
    else:
      msg = self.presenter.order_failed(signal, result, footer)
    self.ctx.channel_notifier.send_message(msg)

  def _persist_success(self, signal: SignalSchema, result: dict, footer: str) -> None:
    action_val = signal.action.value
    pos_ticket = result.get("source_ticket") or result.get("ticket")
    signal_json = signal.model_dump_json()

    for fc in result.get("forced_closed", []):
      self.ctx.channel_notifier.send_message(
        self.presenter.force_closed(signal.symbol, signal.strategy, fc, footer)
      )

    if action_val in ("LONG", "SHORT"):
      self.ctx.db_service.insert_position(
        ref_id=pos_ticket,
        strategy=signal.strategy,
        symbol=signal.symbol,
        action=action_val.lower(),
        volume=result.get("volume", signal.quantity),
        opened_price=result.get("price", signal.price),
        gateway_return_code=result.get("retcode"),
        comment=result.get("comment", ""),
        message=signal_json,
        strategy_code=self._magic_for(signal.strategy),
        market_type=self._market_type,
      )
    else:
      status = _CLOSE_STATUS_MAP.get(action_val)
      if status:
        self.ctx.db_service.update_position_status(
          ref_source_id=pos_ticket,
          status=status,
          ref_id=result.get("ticket"),
          closed_price=result.get("price"),
          gateway_return_code=result.get("retcode"),
          comment=result.get("comment", ""),
          message=signal_json,
        )

  # ── Helpers ───────────────────────────────────────────────────────────── #

  @staticmethod
  def _market_type_value(mt) -> str:
    return mt.value if isinstance(mt, MarketTypeEnum) else str(mt or "")

  def _ensure_connected(self) -> bool:
    """Verify/restore the broker connection before acting. Default: always ready.

    Overridden by brokers that hold a persistent connection (MT5) to reconnect.
    """
    return True

  # ── Abstract hooks (each concrete market must define these) ───────────── #

  @abstractmethod
  def _build_executor(self) -> Any:
    """Build and return the broker executor (and any client/bridge it needs)."""

  @abstractmethod
  def _connect_broker(self) -> bool:
    """Establish the broker connection. Return True on success."""

  @abstractmethod
  def _disconnect_broker(self) -> None:
    """Tear down the broker connection."""

  @abstractmethod
  def _account_footer(self) -> str:
    """Return the live account footer appended to notifications."""

  @property
  @abstractmethod
  def _account_id(self) -> str:
    """Stable identifier for this worker's account (used for ADMIN routing)."""

  @abstractmethod
  def _magic_for(self, strategy: str) -> Optional[int]:
    """Resolve the broker isolation handle for *strategy* (None where N/A)."""

  @abstractmethod
  def _position_cdc_kwargs(self) -> Dict[str, Any]:
    """Return ``account_info_fn`` / ``account_name`` / ``strategy_magic_map``."""

  @abstractmethod
  def _start_broker_jobs(self, stop_event) -> None:
    """Start broker-specific background jobs (health, close detection, …)."""

  @abstractmethod
  def _handle_admin_message(self, raw: str) -> None:
    """Handle a NATS ADMIN message (e.g. FLAT): parse, route, reconcile the DB.

    Reconciliation is broker-specific (MT5 matches live tickets; a CEX matches
    by symbol), so each market defines it. Implementations should validate the
    action, honor ``account_id`` routing against :attr:`_account_id`, and call
    :meth:`_ensure_connected` before touching the broker.
    """
