# trading

Small trading-data utilities.

## Bybit public data

Use the client library directly:

```python
from trading import BybitPublicDataClient

client = BybitPublicDataClient()
files = client.list_trade_files("BTCUSDT", start_date="2020-03-25", end_date="2020-03-26")
paths = client.download_trades(["BTCUSDT"], "data/bybit", start_date="2020-03-25", limit=1)
```

Or experiment with the CLI:

```sh
uv run trading symbols --limit 10
uv run trading files BTCUSDT --start-date 2020-03-25 --end-date 2020-03-26
uv run trading -v download BTCUSDT --start-date 2020-03-25 --limit 1
```

Start the Streamlit app:

```sh
uv run trading-app
```

Provider implementations live under `trading.providers`. A provider supplies ticker discovery, dated trade-file discovery, downloading, and a parser that converts source files into the common trade-row shape used by the database importer.
