import time
from typing import Any, Dict, Optional

import MetaTrader5 as mt5

from worker.logger import get_logger

logger = get_logger("worker.mt5")


class MT5:
  def __init__(
    self, server: str, login: int, password: str, path: Optional[str] = None
  ):
    self.server = server
    self.login = login
    self.password = password
    self.path = path
    self.account_name: Optional[str] = None

  def connect(self) -> bool:
    """Initialize and connect to the MT5 Terminal."""
    try:
      # Explicitly yield the GIL before calling the MT5 C extension.
      # mt5.initialize() on Windows communicates with the terminal via COM/pipes
      # and can hold the GIL, freezing the asyncio event loop.
      time.sleep(0)

      logger.info("Initializing MT5 Connection...")

      # Initialize with credentials and path
      init_args = {
        "login": self.login,
        "password": self.password,
        "server": self.server,
      }
      if self.path:
        init_args["path"] = self.path

      if not mt5.initialize(**init_args):
        logger.error(f"MT5 initialize() failed, error code: {mt5.last_error()}")
        return False

      # Log in to the account
      authorized = mt5.login(
        login=self.login, password=self.password, server=self.server
      )

      if authorized:
        account_info = mt5.account_info()
        if account_info:
          self.account_name = account_info.name

        logger.info(
          f"Connected to MT5 Server: {self.server} (Account: {self.login}, Name: {self.account_name})"
        )
        return True
      else:
        logger.error(
          f"MT5 login failed for account {self.login}, error log: {mt5.last_error()}"
        )
        return False

    except Exception as e:
      logger.exception(f"Unexpected error during MT5 connection: {e}")
      return False

  def get_account_status(self) -> Optional[Dict[str, Any]]:
    """Retrieve account status: Balance, Equity, etc."""
    try:
      account_info = mt5.account_info()
      if account_info is None:
        logger.error(f"Failed to get account info, error code: {mt5.last_error()}")
        return None

      account_dict = account_info._asdict()
      logger.debug(
        f"Account Info - Balance: {account_dict['balance']}, Equity: {account_dict['equity']}"
      )
      return account_dict

    except Exception as e:
      logger.exception(f"Error fetching account status: {e}")
      return None

  def get_account_footer(self) -> str:
    """Return a formatted footer string with account details."""
    return (
      f"\n\n-----------------------------\n"
      f"<b>Account:</b> {self.login} ({self.account_name})\n"
      f"<b>Server:</b> {self.server}\n"
      f"-----------------------------\n"
    )

  def is_connected(self) -> bool:
    """Return True if the MT5 terminal is currently reachable."""
    try:
      info = mt5.terminal_info()
      return info is not None and info.connected
    except Exception:
      return False

  def reconnect(self, max_attempts: int = 10, delay_seconds: float = 5.0) -> bool:
    """Attempt to reconnect to MT5 with retries.

    Args:
      max_attempts: Maximum number of connection attempts (0 = unlimited).
      delay_seconds: Seconds to wait between each attempt.

    Returns:
      True if reconnected successfully, False if all attempts exhausted.
    """
    attempt = 0
    while max_attempts == 0 or attempt < max_attempts:
      attempt += 1
      logger.info(
        f"MT5 reconnect attempt {attempt}"
        + (f"/{max_attempts}" if max_attempts else "")
        + "..."
      )
      # Yield GIL before each C extension call so the asyncio event loop
      # can continue processing requests (e.g. /health) between retry attempts.
      time.sleep(0)
      try:
        mt5.shutdown()  # Clean up any stale state
      except Exception:
        pass

      if self.connect():
        return True

      logger.warning(
        f"MT5 reconnect attempt {attempt} failed. Retrying in {delay_seconds}s..."
      )
      time.sleep(delay_seconds)

    logger.error("MT5 reconnection exhausted all attempts.")
    return False

  def shutdown(self):
    """Close the MT5 connection."""
    mt5.shutdown()
    logger.info("MT5 Connection closed.")


# Instantiate a global bridge later or in worker
