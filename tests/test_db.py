from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from trading.providers.bybit import iter_trade_rows


def test_iter_bybit_trade_rows_parses_bybit_csv(tmp_path: Path) -> None:
    path = tmp_path / "BTCUSDT2020-03-25.csv"
    path.write_text(
        "\n".join(
            [
                "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional",
                "1585180700.0647,BTCUSDT,Buy,0.042,6698.5,PlusTick,trade-1,28133700000.0,0.042,281.337",
            ]
        )
        + "\n"
    )

    rows = list(iter_trade_rows(path, path.name))

    assert rows == [
        (
            datetime(2020, 3, 25, 23, 58, 20, 64700, tzinfo=UTC),
            "BTCUSDT",
            "Buy",
            0.042,
            6698.5,
            "PlusTick",
            "trade-1",
            28133700000.0,
            0.042,
            281.337,
            "BTCUSDT2020-03-25.csv",
        )
    ]
