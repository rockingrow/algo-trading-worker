"""Crypto (CEX) integration package.

Exchange-agnostic by design: business logic depends on
:class:`~worker.crypto.base.BaseExchangeGateway`, and
:class:`~worker.crypto.factory.ExchangeFactory` selects the concrete exchange
(Binance first) from settings.
"""
