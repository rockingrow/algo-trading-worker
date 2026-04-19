"""
worker/helpers/http_request.py
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from worker.logger import get_logger

log = get_logger("worker.http_request")

_DEFAULT_TIMEOUT = 10


def post(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> requests.Response:
  log.debug("POST %s payload=%s", url, payload)
  response = requests.post(url, json=payload, headers=headers or {}, timeout=_DEFAULT_TIMEOUT)
  response.raise_for_status()
  return response


def patch(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> requests.Response:
  log.debug("PATCH %s payload=%s", url, payload)
  response = requests.patch(url, json=payload, headers=headers or {}, timeout=_DEFAULT_TIMEOUT)
  response.raise_for_status()
  return response
