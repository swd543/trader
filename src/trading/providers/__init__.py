from collections.abc import Callable
from typing import Any

from trading.providers.base import HistoricalTradeProvider, MarketDataFile, TradeRow
from trading.providers.bybit import BybitPublicDataClient

type ProviderFactory = Callable[..., HistoricalTradeProvider]

PROVIDERS: dict[str, ProviderFactory] = {
    BybitPublicDataClient.slug: BybitPublicDataClient,
}


def provider_names() -> list[str]:
    return sorted(PROVIDERS)


def create_provider(slug: str, **kwargs: Any) -> HistoricalTradeProvider:
    provider_class = PROVIDERS[slug]
    return provider_class(**kwargs)


__all__ = [
    "BybitPublicDataClient",
    "HistoricalTradeProvider",
    "MarketDataFile",
    "TradeRow",
    "create_provider",
    "provider_names",
]
