"""FOREX market integration package.

Platform-agnostic by design: the FOREX market is served by a trading *platform*
gateway. MT5 is the first such platform and lives under
:mod:`worker.gateways.forex.mt5`; future platforms (e.g. mt6) slot in beside it
as ``worker.gateways.forex.<platform>``.

This mirrors the layout of :mod:`worker.gateways.crypto`, where the market-level
package hosts one subpackage per concrete venue.
"""
