"""
worker/crypto_worker.py
───────────────────────
CRYPTO worker entry points.

The crypto gateway is pure Python (signed REST + a websocket user-data stream,
both in daemon threads), so — unlike MT5 — it needs **no** GIL-isolating child
process. Two ways to run it:

  * :func:`crypto_worker_main` — the worker lifecycle, used by the in-process
    *thread* orchestrator when launched through the FastAPI/uvicorn app.
  * :func:`main` — a standalone single-process entry point
    (``python -m worker.crypto_worker``) used by the container: no FastAPI, no
    uvicorn, no multiprocessing — just the worker loop in this very process.

Nothing here imports ``worker.gateways.mt5.*`` / MetaTrader5 — the CRYPTO path
carries no Forex dependencies.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path

from worker.logger import get_logger
from worker.worker_runtime import run_worker

log = get_logger("worker.crypto_worker")

_HEARTBEAT_INTERVAL = 15  # seconds


def crypto_worker_main(settings_dict: dict, stop_event) -> None:
  """Run the crypto worker lifecycle until *stop_event* is set."""
  from worker.gateways.crypto.signal_processor import CryptoSignalProcessor

  run_worker(CryptoSignalProcessor, settings_dict, stop_event, label="Crypto")


def _start_heartbeat(stop_event) -> None:
  """Touch ``HEARTBEAT_FILE`` periodically so the container healthcheck can spot a
  hung worker (there is no HTTP server in standalone mode). No-op when unset."""
  path = os.environ.get("HEARTBEAT_FILE")
  if not path:
    return

  target = Path(path)
  tmp = target.with_suffix(target.suffix + ".tmp")

  def _beat() -> None:
    while not stop_event.is_set():
      try:
        # Write+rename so a healthcheck reading concurrently never sees a
        # truncated/empty file (os.replace is atomic on the same filesystem).
        tmp.write_text(str(time.time()))
        os.replace(tmp, target)
      except OSError as exc:
        log.warning("[Crypto] heartbeat write failed: %s", exc)
      stop_event.wait(_HEARTBEAT_INTERVAL)

  threading.Thread(target=_beat, name="heartbeat", daemon=True).start()


def main() -> None:
  """Standalone single-process entry point (the container's CMD).

  Runs the worker loop directly in this process and turns SIGTERM/SIGINT (e.g.
  ``docker stop``) into a graceful shutdown.
  """
  from worker.settings import settings

  stop_event = threading.Event()

  def _request_stop(signum, _frame) -> None:
    log.info("[Crypto] Received signal %s — shutting down.", signum)
    stop_event.set()

  signal.signal(signal.SIGTERM, _request_stop)
  signal.signal(signal.SIGINT, _request_stop)

  _start_heartbeat(stop_event)
  crypto_worker_main(settings.model_dump(), stop_event)


if __name__ == "__main__":
  main()
