from enum import Enum


class LogAuthorEnum(str, Enum):
  TERMINAL = "terminal"  # FOREX: closed by the MT5 terminal (hard SL/TP, stop-out)
  EXCHANGE = "exchange"  # CRYPTO: closed by the CEX (stop-loss/take-profit/liquidation)
  BROKER = "broker"


class AttemptOutcomeEnum(str, Enum):
  """How a logged signal attempt ended — ``position_logs.attempt_outcome``.

  The signal path writes its audit row *before* calling the broker, so the row
  proves an attempt exists even if the worker dies mid-order. This column is the
  verdict, and it is the only thing that decides whether the signal may be sent
  again (see :meth:`worker.db.repository.PositionRepository.signal_exists`).
  """

  #: The broker call is in flight, or the worker died before it could record the
  #: answer. Nothing may be concluded, so an **entry** is not re-sent: it could
  #: double a live position. An **exit** is, because a duplicate close resolves
  #: against the tracked row while a refused retry would leave a position live
  #: with no stop. The startup scan alerts on rows left this way.
  ATTEMPTING = "ATTEMPTING"
  #: The broker confirmed. A second delivery of this signal is a duplicate,
  #: whichever action it carries.
  EXECUTED = "EXECUTED"
  #: Definitively refused — a policy rejection or a broker rejection — so
  #: nothing is live and the signal stays retryable. This is the one outcome a
  #: replay is always allowed to act on.
  FAILED = "FAILED"


#: Outcomes that block a re-send of an entry. ``FAILED`` is deliberately absent:
#: a failed signal is exactly the one the broker's retry channel exists for. An
#: exit blocks on ``EXECUTED`` alone — see ``signal_exists(include_unsettled=)``.
BLOCKING_ATTEMPT_OUTCOMES = (
  AttemptOutcomeEnum.ATTEMPTING.value,
  AttemptOutcomeEnum.EXECUTED.value,
)
