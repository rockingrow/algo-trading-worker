"""Crypto (CEX) integration package.

Exchange-agnostic by design: business logic depends on
:class:`~worker.gateways.crypto.base.BaseExchangeGateway`, and
:class:`~worker.gateways.crypto.factory.ExchangeFactory` selects the concrete exchange
(Binance first) from settings.
"""
