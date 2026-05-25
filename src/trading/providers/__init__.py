"""Provider registry.

Add a provider implementation by subclassing HistoricalTradeProvider and
registering the class in PROVIDERS with its stable slug.
"""

from typing import Any

from trading.providers.base import HistoricalTradeProvider, MarketDataFile, ProviderOption, TradeRow
from trading.providers.bybit import BybitPublicDataClient

type ProviderFactory = type[HistoricalTradeProvider]

PROVIDERS: dict[str, ProviderFactory] = {
    BybitPublicDataClient.slug: BybitPublicDataClient,
}


def provider_names() -> list[str]:
    return sorted(PROVIDERS)


def provider_display_name(slug: str) -> str:
    return PROVIDERS[slug].display_name


def provider_default_output_dir(slug: str) -> str:
    return PROVIDERS[slug].default_output_dir


def provider_option_specs(slug: str) -> tuple[ProviderOption, ...]:
    return PROVIDERS[slug].option_specs()


def create_provider(slug: str, **kwargs: Any) -> HistoricalTradeProvider:
    provider_class = PROVIDERS[slug]
    return provider_class(**kwargs)


__all__ = [
    "BybitPublicDataClient",
    "HistoricalTradeProvider",
    "MarketDataFile",
    "ProviderOption",
    "TradeRow",
    "create_provider",
    "provider_default_output_dir",
    "provider_display_name",
    "provider_names",
    "provider_option_specs",
]
