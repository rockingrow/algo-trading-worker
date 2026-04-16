import json
import threading
from typing import Generator

import zmq
from pydantic import ValidationError

from worker.logger import get_logger
from worker.schemas.broker_schema import SignalSchema

logger = get_logger("worker.zmq_service")


class ZMQ:
  def __init__(self, host: str):
    self.host = host
    self.context = zmq.Context()
    self.socket = self._create_socket()
    self._stop_event = threading.Event()

  def _create_socket(self):
    socket = self.context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics/signals

    # Configure TCP Keep-Alive to detect dead connections
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(
      zmq.TCP_KEEPALIVE_IDLE, 60
    )  # Wait 60s without data -> Start sending pings
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 10)  # Send ping every 10s
    socket.setsockopt(
      zmq.TCP_KEEPALIVE_CNT, 3
    )  # Fail 3 times -> Report dead connection (ZMQError)
    return socket

  def connect(self):
    """Connect to Broker via ZeroMQ SUB."""
    try:
      logger.info(f"Connecting to ZMQ Broker at {self.host}...")
      # Use connect instead of bind because this is a SUB worker
      self.socket.connect(self.host)
      logger.info("ZMQ Connected successfully.")
    except Exception as e:
      logger.exception(f"Failed to connect to ZMQ Broker: {e}")
      raise

  def close(self):
    """Signal the listener to stop and close the socket."""
    logger.info("ZMQ stop requested. Closing socket...")
    self._stop_event.set()
    try:
      self.socket.close(linger=0)
    except Exception:
      pass
    try:
      self.context.term()
    except Exception:
      pass

  def listen(self, stop_event=None) -> Generator[SignalSchema, None, None]:
    """Continuously listen for data and parse JSON using Pydantic."""
    logger.info("Started listening for signals...")
    while not self._stop_event.is_set():
      if stop_event is not None and stop_event.is_set():
        return
      try:
        # Use a short timeout so we can check the stop event periodically
        if not self.socket.poll(timeout=500):  # 500ms
          continue
        message = self.socket.recv_string(zmq.NOBLOCK)
        logger.debug(f"Received raw message: {message}")

        # Handle "TOPIC|PAYLOAD" format
        if "|" not in message:
          logger.error(f"Invalid message format (missing '|'): {message}")
          continue

        _, payload = message.split("|", 1)

        # Parse JSON
        raw_data = json.loads(payload)

        # Validate type safety
        signal = SignalSchema(**raw_data)
        logger.info(
          f"Signal validated successfully for {signal.symbol} [{signal.action}]"
        )

        yield signal

      except json.JSONDecodeError as err:
        logger.error(f"Malformed JSON received: {err}")
      except ValidationError as err:
        logger.error(f"Pydantic Validation failed: {err}")
      except zmq.ZMQError as err:
        if self._stop_event.is_set():
          break  # Expected shutdown, not a real error
        logger.error(f"ZMQ Error: {err}")
        # Basic reconnect logic if disconnected:
        # Reset socket
        try:
          self.socket.close(linger=0)
        except Exception:
          pass
        self.socket = self._create_socket()
        self.connect()
      except Exception as err:
        logger.exception(f"Unexpected error in ZMQ listener: {err}")
