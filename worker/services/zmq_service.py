import json
from typing import Generator

import zmq
from worker.logger import get_logger

logger = get_logger("worker.zmq_service")
from pydantic import ValidationError
from worker.schemas.broker_schema import SignalSchema


class ZMQ:
  def __init__(self, host: str):
    self.host = host
    self.context = zmq.Context()
    self.socket = self._create_socket()

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

  def listen(self) -> Generator[SignalSchema, None, None]:
    """Continuously listen for data and parse JSON using Pydantic."""
    logger.info("Started listening for signals...")
    while True:
      try:
        # Wait for message
        message = self.socket.recv_string()
        logger.debug(f"Received raw message: {message}")

        # Parse JSON
        raw_data = json.loads(message)

        # Validate type safety
        signal = SignalSchema(**raw_data)
        logger.info(
          f"Signal validated successfully for {signal.symbol} [{signal.position.action}]"
        )

        yield signal

      except json.JSONDecodeError as err:
        logger.error(f"Malformed JSON received: {err}")
      except ValidationError as err:
        logger.error(f"Pydantic Validation failed: {err}")
      except zmq.ZMQError as err:
        logger.error(f"ZMQ Error: {err}")
        # Basic reconnect logic if disconnected:
        # Reset socket
        self.socket.close()
        self.socket = self._create_socket()
        self.connect()
      except Exception as err:
        logger.exception(f"Unexpected error in ZMQ listener: {err}")
