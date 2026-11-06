"""
tests/test_fake_protocol_conformance.py
───────────────────────────────────────
Keeps the hand-rolled DB doubles honest against the protocols they stand in for.

Each suite builds its own small fake of the position store, shaped to what that
suite needs. The cost of that freedom is drift: when a protocol method gains a
parameter, a fake that does not accept it fails only at runtime, deep inside the
code under test, as a ``TypeError`` that reads like a production bug — which is
how a ``ref_id`` filter once broke two dozen tests at once. This check fails
fast, names the fake and the missing parameter, and costs nothing to run.

Only the methods a fake actually defines are checked: a fake is free to omit
what its suite never calls.
"""

from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path

import pytest

from worker.interfaces.db_protocol import (
  PositionStoreProtocol,
  TerminalCloseStoreProtocol,
)

_TESTS = Path(__file__).parent

# (test module, fake class name) — every DB double that stands in for the
# position store.
_FAKES = [
  ("gateways/test_admin_flat.py", "FakeDbService"),
  ("gateways/test_processor.py", "FakeDb"),
  ("gateways/test_signal_handler.py", "FakeStore"),
  ("gateways/forex/test_admin_handler.py", "_FakeDb"),
  ("gateways/forex/test_forex_reconcile_job.py", "_FakeDb"),
  ("gateways/crypto/test_reconcile_job.py", "_FakeDb"),
]

_PROTOCOLS = (PositionStoreProtocol, TerminalCloseStoreProtocol)


def _load_class(rel_path: str, name: str) -> type:
  path = _TESTS / rel_path
  spec = importlib.util.spec_from_file_location(f"_conformance_{path.stem}", path)
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return getattr(module, name)


def _protocol_params() -> dict[str, set[str]]:
  params: dict[str, set[str]] = {}
  for proto in _PROTOCOLS:
    for attr, fn in vars(proto).items():
      if attr.startswith("_") or not callable(fn):
        continue
      names = set(inspect.signature(fn).parameters) - {"self"}
      params.setdefault(attr, set()).update(names)
  return params


@pytest.mark.parametrize(("rel_path", "name"), _FAKES)
def test_fake_accepts_every_protocol_parameter(rel_path, name):
  fake = _load_class(rel_path, name)
  missing = []
  for method, expected in _protocol_params().items():
    impl = getattr(fake, method, None)
    if impl is None:
      continue
    sig = inspect.signature(impl).parameters
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.values()):
      continue
    lacking = expected - set(sig)
    if lacking:
      missing.append(f"{method}: {sorted(lacking)}")
  assert not missing, f"{rel_path}::{name} drifted from the protocol — " + "; ".join(
    missing
  )
