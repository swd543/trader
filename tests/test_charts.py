from __future__ import annotations

from trading.charts import build_price_figure


def test_build_price_figure_can_overlay_trade_markers() -> None:
    figure = build_price_figure(
        [
            {
                "bucket": "2024-01-01T00:00:00",
                "symbol": "BTCUSDT",
                "open": 100.0,
                "high": 110.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 12.0,
                "trade_count": 3,
            }
        ],
        [
            {
                "ts": "2024-01-01T00:00:30",
                "symbol": "BTCUSDT",
                "side": "Buy",
                "size": 0.25,
                "price": 104.0,
                "trade_id": "trade-1",
                "source_file": "BTCUSDT2024-01-01.csv",
            }
        ],
        y_axis="Linear",
        layout_mode="Overlay",
    )

    assert len(figure.data) == 2
    assert figure.data[0].type == "candlestick"
    assert figure.data[1].type == "scattergl"


def test_build_price_figure_can_stack_multiple_symbols() -> None:
    figure = build_price_figure(
        [
            {
                "bucket": "2024-01-01T00:00:00",
                "symbol": "BTCUSDT",
                "open": 100.0,
                "high": 110.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 12.0,
                "trade_count": 3,
            },
            {
                "bucket": "2024-01-01T00:00:00",
                "symbol": "ETHUSDT",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "volume": 20.0,
                "trade_count": 5,
            },
        ],
        [],
        y_axis="Log",
        layout_mode="Stacked",
    )

    assert len(figure.data) == 2
    assert figure.layout.height == 640
    assert figure.layout.yaxis.type == "log"
    assert figure.layout.yaxis2.type == "log"
