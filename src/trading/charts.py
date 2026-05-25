from __future__ import annotations

from datetime import datetime
from typing import TypedDict, cast

import plotly.graph_objects as go  # type: ignore[import-untyped]
from plotly.subplots import make_subplots  # type: ignore[import-untyped]


class OhlcvChartRow(TypedDict):
    bucket: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int


class TradeMarkerChartRow(TypedDict):
    ts: str
    symbol: str
    side: str
    size: float
    price: float
    trade_id: str
    source_file: str


def build_price_figure(
    rows: list[OhlcvChartRow],
    trade_rows: list[TradeMarkerChartRow],
    *,
    y_axis: str,
    layout_mode: str,
) -> go.Figure:
    symbols = sorted({row["symbol"] for row in rows})
    stacked = layout_mode == "Stacked" and len(symbols) > 1
    y_axis_type = "log" if y_axis == "Log" else "linear"

    if stacked:
        figure = make_subplots(
            rows=len(symbols),
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            subplot_titles=symbols,
        )
        symbol_axis = {symbol: index + 1 for index, symbol in enumerate(symbols)}
    else:
        figure = go.Figure()
        symbol_axis = {symbol: 1 for symbol in symbols}

    for symbol in symbols:
        symbol_rows = [row for row in rows if row["symbol"] == symbol]
        row_index = symbol_axis[symbol]
        candlestick = go.Candlestick(
            x=[row["bucket"] for row in symbol_rows],
            open=[row["open"] for row in symbol_rows],
            high=[row["high"] for row in symbol_rows],
            low=[row["low"] for row in symbol_rows],
            close=[row["close"] for row in symbol_rows],
            name=symbol,
            increasing_line_color="#1f9d55",
            decreasing_line_color="#d64545",
        )
        if stacked:
            figure.add_trace(candlestick, row=row_index, col=1)
        else:
            figure.add_trace(candlestick)

        symbol_trade_rows = [row for row in trade_rows if row["symbol"] == symbol]
        for side, color, marker_symbol in (("Buy", "#2563eb", "triangle-up"), ("Sell", "#dc2626", "triangle-down")):
            side_rows = [row for row in symbol_trade_rows if row["side"].lower() == side.lower()]
            if not side_rows:
                continue
            marker_trace = go.Scattergl(
                x=[row["ts"] for row in side_rows],
                y=[row["price"] for row in side_rows],
                mode="markers",
                name=f"{symbol} {side}",
                marker={
                    "color": color,
                    "symbol": marker_symbol,
                    "size": [max(7.0, min(18.0, row["size"] ** 0.5 * 8.0)) for row in side_rows],
                    "opacity": 0.78,
                },
                customdata=[
                    [row["size"], row["trade_id"], row["source_file"]]
                    for row in side_rows
                ],
                hovertemplate=(
                    "%{x}<br>"
                    f"{symbol} {side}<br>"
                    "price=%{y:,.6f}<br>"
                    "size=%{customdata[0]:,.6f}<br>"
                    "trade=%{customdata[1]}<br>"
                    "file=%{customdata[2]}"
                    "<extra></extra>"
                ),
            )
            if stacked:
                figure.add_trace(marker_trace, row=row_index, col=1)
            else:
                figure.add_trace(marker_trace)

    figure.update_layout(
        dragmode="pan",
        height=max(480, 320 * len(symbols)) if stacked else 560,
        margin={"l": 40, "r": 24, "t": 48, "b": 40},
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        xaxis_rangeslider_visible=True,
    )
    if stacked:
        for index in range(1, len(symbols) + 1):
            figure.update_yaxes(type=y_axis_type, title_text="Price", row=index, col=1)
    else:
        figure.update_yaxes(type=y_axis_type, title_text="Price")
    figure.update_xaxes(title_text="Time")
    return figure


def normalize_ohlcv_chart_row(row: dict[str, object]) -> OhlcvChartRow:
    bucket = row["bucket"]
    return {
        "bucket": bucket.isoformat() if isinstance(bucket, datetime) else str(bucket),
        "symbol": str(row["symbol"]),
        "open": float(cast(str | int | float, row["open"])),
        "high": float(cast(str | int | float, row["high"])),
        "low": float(cast(str | int | float, row["low"])),
        "close": float(cast(str | int | float, row["close"])),
        "volume": float(cast(str | int | float, row["volume"])),
        "trade_count": int(cast(str | int | float, row["trade_count"])),
    }


def normalize_trade_marker_chart_row(row: dict[str, object]) -> TradeMarkerChartRow:
    ts = row["ts"]
    return {
        "ts": ts.isoformat() if isinstance(ts, datetime) else str(ts),
        "symbol": str(row["symbol"]),
        "side": str(row["side"]),
        "size": float(cast(str | int | float, row["size"])),
        "price": float(cast(str | int | float, row["price"])),
        "trade_id": str(row["trade_id"]),
        "source_file": str(row["source_file"]),
    }
