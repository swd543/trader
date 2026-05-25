# trading

Small trading-data utilities for downloading historical trades, ingesting them
into TimescaleDB, and exploring rolled-up OHLCV charts.

## Run

Start TimescaleDB and pgAdmin:

```sh
docker compose up -d
```

Start the Streamlit app:

```sh
uv run trading-app
```

Use the CLI:

```sh
uv run trading symbols --limit 10
uv run trading files BTCUSDT --start-date 2020-03-25 --end-date 2020-03-26
uv run trading -v download BTCUSDT --start-date 2020-03-25 --limit 1
```

Use the client library directly:

```python
import asyncio

from trading import BybitPublicDataClient


async def main() -> None:
    client = BybitPublicDataClient()
    files = await client.list_trade_files(
        "BTCUSDT",
        start_date="2020-03-25",
        end_date="2020-03-26",
    )
    for trade_file in files[:1]:
        path = await client.download_trade_file(trade_file, "data/bybit")
        print(path)


asyncio.run(main())
```

## Provider Interface

Provider implementations live under `src/trading/providers`. A provider is a
subclass of `HistoricalTradeProvider` from
`src/trading/providers/base.py`.

The app expects a provider to implement:

- `slug`: stable lowercase id used in table names, CLI flags, cache keys, and the registry.
- `display_name`: human-readable label for the UI.
- `default_output_dir`: default local download directory.
- `option_specs()`: optional constructor options exposed automatically in Streamlit and the CLI.
- `list_symbols()`: async ticker discovery.
- `list_trade_files(symbol, start_date=None, end_date=None)`: async dated raw trade-file discovery.
- `download_trade_file(...)`: async download of one file, with progress and cancellation callbacks.
- `iter_trade_rows(path, source_file)`: parser that yields the common `TradeRow` tuple.

`TradeRow` order is:

```text
ts, symbol, side, size, price, tick_direction, trade_id,
gross_value, home_notional, foreign_notional, source_file
```

Provider symbols should be normalized to the exchange symbol format accepted by
`normalize_symbol`, currently uppercase letters, digits, `_`, and `-`.

## Adding A Provider

1. Create `src/trading/providers/<provider>.py`.
2. Subclass `HistoricalTradeProvider`.
3. Return `MarketDataFile` objects from `list_trade_files`.
4. Download files into `output_dir / symbol / filename` or another stable path, and return the parseable local file path.
5. Implement `iter_trade_rows` so the database importer does not need provider-specific parsing.
6. Register the class in `PROVIDERS` in `src/trading/providers/__init__.py`.

Minimal shape:

```python
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from trading.providers.base import (
    CancelCheck,
    DownloadProgress,
    HistoricalTradeProvider,
    MarketDataFile,
    ProviderOption,
    TradeRow,
)


class ExampleProvider(HistoricalTradeProvider):
    slug = "example"
    display_name = "Example"
    default_output_dir = "data/example"

    @classmethod
    def option_specs(cls) -> tuple[ProviderOption, ...]:
        return (
            ProviderOption(
                name="base_url",
                label="Example base URL",
                default="https://example.invalid/data/",
                value_type="str",
            ),
        )

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    async def list_symbols(self) -> list[str]:
        return ["BTCUSDT"]

    async def list_trade_files(
        self,
        symbol: str,
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[MarketDataFile]:
        return []

    async def download_trade_file(
        self,
        trade_file: MarketDataFile,
        output_dir: str | Path,
        *,
        extract: bool = True,
        overwrite: bool = False,
        keep_archive: bool = False,
        progress_callback: DownloadProgress | None = None,
        cancel_callback: CancelCheck | None = None,
    ) -> Path:
        raise NotImplementedError

    def iter_trade_rows(self, path: Path, source_file: str) -> Iterator[TradeRow]:
        raise NotImplementedError
```

Then register it:

```python
from trading.providers.example import ExampleProvider

PROVIDERS = {
    BybitPublicDataClient.slug: BybitPublicDataClient,
    ExampleProvider.slug: ExampleProvider,
}
```

Once registered, the Streamlit provider selector and CLI `--provider` option
pick it up automatically.
