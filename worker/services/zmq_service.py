import json
import threading
import time
from typing import Callable, Generator, Optional

import zmq
import zmq.utils.monitor
from pydantic import ValidationError
from zmq.utils import z85

from worker.helpers.logging import get_footer
from worker.logger import get_logger
from worker.schemas.broker_schema import SignalSchema
from worker.services.notification_service import TelegramNotification

logger = get_logger("worker.zmq_service")


class ZMQ:
  def __init__(
    self,
    host: str,
    curve_server_public_key: Optional[str] = None,
    curve_client_public_key: Optional[str] = None,
    curve_client_secret_key: Optional[str] = None,
    account_footer_fn: Optional[Callable[[], str]] = None,
  ):
    self.host = host
    self._curve_server_public_key = curve_server_public_key
    self._curve_client_public_key = curve_client_public_key
    self._curve_client_secret_key = curve_client_secret_key
    self._account_footer_fn = account_footer_fn
    self.context = zmq.Context()
    self._stop_event = threading.Event()
    self._curve_enabled = False
    self._monitor_count = 0
    self._monitor_thread: Optional[threading.Thread] = None
    self._retry_delay = 0
    self.socket = self._create_socket()

  def _create_socket(self):
    socket = self.context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics/signals

    # ── CURVE security (applied before connect) ────────────────────────── #
    # Filter out None, empty strings, or placeholder text
    curve_keys = [
      self._curve_server_public_key,
      self._curve_client_public_key,
      self._curve_client_secret_key,
    ]

    def clean_key(k):
      if not k:
        return None
      return k.strip().strip('"').strip("'").strip()

    def is_valid_key(k):
      cleaned = clean_key(k)
      return cleaned and len(cleaned) == 40  # Z85 keys are exactly 40 chars

    if all(is_valid_key(k) for k in curve_keys):
      try:
        # Z85 keys (40 chars) must be decoded to 32-byte binary
        server_pub = clean_key(self._curve_server_public_key)
        client_pub = clean_key(self._curve_client_public_key)
        client_sec = clean_key(self._curve_client_secret_key)

        socket.curve_serverkey = z85.decode(server_pub)
        socket.curve_publickey = z85.decode(client_pub)
        socket.curve_secretkey = z85.decode(client_sec)
        self._curve_enabled = True
        logger.info("ZMQ CURVE security enabled and keys decoded successfully.")
      except Exception as e:
        logger.error(f"Failed to apply ZMQ CURVE keys: {e}")
        raise
    else:
      self._curve_enabled = False
      logger.warning(
        "ZMQ CURVE security is DISABLED (keys missing or invalid). Connection is unencrypted."
      )

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

  def _handle_monitor_event(
    self,
    event: int,
    endpoint: str,
    notification: TelegramNotification,
    curve_enabled: bool,
  ) -> bool:
    """Handle a single monitor event. Returns False when the monitor loop should stop."""
    footer = get_footer(self._account_footer_fn)

    if event == zmq.EVENT_CONNECTED:
      logger.info("ZMQ Broker TCP connection established: %s", endpoint)
      if not curve_enabled:
        self._retry_delay = 0
        notification.send_message(
          f"<pre>🔌 ZMQ Worker Connected to Broker\nEndpoint: {endpoint}{footer}</pre>"
        )

    elif event == zmq.EVENT_HANDSHAKE_SUCCEEDED:
      logger.info("ZMQ Broker CURVE handshake succeeded: %s", endpoint)
      self._retry_delay = 0
      notification.send_message(
        f"<pre>🔌 ZMQ Worker Connected to Broker\n🔐 Authenticated (CURVE)\nEndpoint: {endpoint}{footer}</pre>"
      )

    elif event == zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL:
      logger.warning("ZMQ Broker CURVE handshake failed: %s", endpoint)
      notification.send_message(
        f"<pre>⚠️ ZMQ Worker Auth Failed (CURVE)\nEndpoint: {endpoint}{footer}</pre>"
      )

    elif event == zmq.EVENT_DISCONNECTED:
      logger.warning("ZMQ Broker disconnected: %s. Retrying...", endpoint)
      notification.send_message(
        f"<pre>🔴 ZMQ Broker Disconnected\nEndpoint: {endpoint}\n⏳ Retrying connection...{footer}</pre>"
      )

    elif event == zmq.EVENT_CONNECT_RETRIED:
      logger.info("ZMQ retrying connection to Broker: %s", endpoint)
      notification.send_message(
        f"<pre>🔄 ZMQ Retrying connection to Broker\nEndpoint: {endpoint}{footer}</pre>"
      )

    elif event == zmq.EVENT_CONNECT_DELAYED:
      logger.warning("ZMQ connection to Broker delayed (pending): %s", endpoint)
      notification.send_message(
        f"<pre>⏳ ZMQ Connection to Broker delayed\nEndpoint: {endpoint}{footer}</pre>"
      )

    elif event == zmq.EVENT_MONITOR_STOPPED:
      return False

    return True

  def _start_monitor(self) -> None:
    """Spin up a daemon thread that watches for broker connect/disconnect events."""
    monitor_addr = f"inproc://worker-sub-monitor-{self._monitor_count}"
    self._monitor_count += 1

    events = (
      zmq.EVENT_CONNECTED
      | zmq.EVENT_CONNECT_DELAYED
      | zmq.EVENT_CONNECT_RETRIED
      | zmq.EVENT_DISCONNECTED
      | zmq.EVENT_MONITOR_STOPPED
    )
    if self._curve_enabled:
      events |= zmq.EVENT_HANDSHAKE_SUCCEEDED | zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL

    self.socket.monitor(monitor_addr, events)
    curve_enabled = self._curve_enabled

    def _watch() -> None:
      # ZMQ sockets are not thread-safe — create monitor_sock inside the thread
      monitor_sock = self.context.socket(zmq.PAIR)
      monitor_sock.connect(monitor_addr)
      notification = TelegramNotification()
      while not self._stop_event.is_set():
        if not monitor_sock.poll(timeout=500):
          continue
        try:
          event_data = zmq.utils.monitor.recv_monitor_message(monitor_sock)
          event = event_data["event"]
          endpoint = event_data.get("endpoint", b"").decode(errors="replace")
          logger.debug("ZMQ monitor raw event: 0x%x endpoint=%s", event, endpoint)
          if not self._handle_monitor_event(
            event, endpoint, notification, curve_enabled
          ):
            break
        except Exception as exc:
          logger.exception("ZMQ monitor error: %s", exc)
      monitor_sock.close()

    self._monitor_thread = threading.Thread(
      target=_watch, name="zmq-monitor", daemon=True
    )
    self._monitor_thread.start()

  def connect(self):
    """Connect to Broker via ZeroMQ SUB."""
    try:
      logger.info(f"Connecting to ZMQ Broker at {self.host}...")
      # Use connect instead of bind because this is a SUB worker
      self.socket.connect(self.host)
      self._start_monitor()
      logger.info("ZMQ socket connected. Monitoring for broker state changes...")
    except Exception as e:
      logger.exception(f"Failed to connect to ZMQ Broker: {e}")
      raise

  def close(self):
    """Signal the listener to stop and close the socket."""
    logger.info("ZMQ stop requested. Closing socket...")
    self._stop_event.set()
    try:
      self.socket.disable_monitor()
    except Exception:
      pass
    if self._monitor_thread is not None:
      self._monitor_thread.join(timeout=2)
    try:
      self.socket.close(linger=0)
    except Exception:
      pass
    try:
      self.context.term()
    except Exception:
      pass

  def _parse_message(self, message: str) -> Optional[SignalSchema]:
    if "|" not in message:
      logger.error(f"Invalid message format (missing '|'): {message}")
      return None
    _, payload = message.split("|", 1)
    raw_data = json.loads(payload)
    signal = SignalSchema(**raw_data)
    logger.info(f"Signal validated successfully for {signal.symbol} [{signal.action}]")
    return signal

  def _reconnect(self):
    self._retry_delay += 10
    logger.error("ZMQ Error: reconnecting in %ds...", self._retry_delay)
    TelegramNotification().send_message(
      f"<pre>🔄 ZMQ Worker reconnecting to Broker\nEndpoint: {self.host}\n⏳ Retrying in {self._retry_delay}s...{get_footer(self._account_footer_fn)}</pre>"
    )
    time.sleep(self._retry_delay)
    try:
      self.socket.disable_monitor()
    except Exception:
      pass
    if self._monitor_thread is not None:
      self._monitor_thread.join(timeout=2)
    try:
      self.socket.close(linger=0)
    except Exception:
      pass
    self.socket = self._create_socket()
    self.connect()

  def listen(self, stop_event=None) -> Generator[SignalSchema, None, None]:
    """Continuously listen for data and parse JSON using Pydantic."""
    logger.info("Started listening for signals...")
    while not self._stop_event.is_set():
      if stop_event is not None and stop_event.is_set():
        return
      try:
        if not self.socket.poll(timeout=500):  # 500ms
          continue
        message = self.socket.recv_string(zmq.NOBLOCK)
        logger.debug(f"Received raw message: {message}")
        signal = self._parse_message(message)
        if signal is not None:
          yield signal
      except json.JSONDecodeError as err:
        logger.error(f"Malformed JSON received: {err}")
      except ValidationError as err:
        logger.error(f"Pydantic Validation failed: {err}")
      except zmq.ZMQError as err:
        if self._stop_event.is_set():
          break
        logger.error(f"ZMQ Error: {err}")
        self._reconnect()
      except Exception as err:
        logger.exception(f"Unexpected error in ZMQ listener: {err}")
