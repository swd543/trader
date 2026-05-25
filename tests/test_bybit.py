from __future__ import annotations

import gzip
from io import BytesIO
from pathlib import Path

from trading.bybit import BybitPublicDataClient


class FakeBybitClient(BybitPublicDataClient):
    def __init__(self, pages: dict[str, bytes]) -> None:
        super().__init__(base_url="https://example.test/trading/")
        self.pages = pages

    def _open_binary(self, url: str) -> BytesIO:
        return BytesIO(self.pages[url])


def test_list_symbols_reads_directory_links() -> None:
    client = FakeBybitClient(
        {
            "https://example.test/trading/": b"""
                <a href="../">Parent</a>
                <a href="BTCUSDT/">BTCUSDT</a>
                <a href="ETHUSDT/">ETHUSDT</a>
                <a href="/absolute/">absolute</a>
            """,
        }
    )

    assert client.list_symbols() == ["BTCUSDT", "ETHUSDT"]


def test_list_trade_files_filters_by_date() -> None:
    client = FakeBybitClient(
        {
            "https://example.test/trading/BTCUSDT/": b"""
                <a href="BTCUSDT2020-03-24.csv.gz">old</a>
                <a href="BTCUSDT2020-03-25.csv.gz">start</a>
                <a href="BTCUSDT2020-03-26.csv.gz">end</a>
                <a href="README.txt">readme</a>
            """,
        }
    )

    files = client.list_trade_files("btcusdt", start_date="2020-03-25", end_date="2020-03-26")

    assert [item.filename for item in files] == ["BTCUSDT2020-03-25.csv.gz", "BTCUSDT2020-03-26.csv.gz"]
    assert files[0].url == "https://example.test/trading/BTCUSDT/BTCUSDT2020-03-25.csv.gz"


def test_download_trade_file_extracts_gzip(tmp_path: Path) -> None:
    archive = gzip.compress(b"timestamp,symbol,price\n1,BTCUSDT,100\n")
    client = FakeBybitClient(
        {
            "https://example.test/trading/BTCUSDT/": b'<a href="BTCUSDT2020-03-25.csv.gz">file</a>',
            "https://example.test/trading/BTCUSDT/BTCUSDT2020-03-25.csv.gz": archive,
        }
    )
    trade_file = client.list_trade_files("BTCUSDT")[0]

    path = client.download_trade_file(trade_file, tmp_path)

    assert path == tmp_path / "BTCUSDT" / "BTCUSDT2020-03-25.csv"
    assert path.read_text() == "timestamp,symbol,price\n1,BTCUSDT,100\n"
    assert not (tmp_path / "BTCUSDT" / "BTCUSDT2020-03-25.csv.gz").exists()
