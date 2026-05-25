from __future__ import annotations

import gzip
import asyncio
from pathlib import Path

import httpx
import pytest

from trading.providers.base import DownloadCancelled
from trading.providers.bybit import BybitPublicDataClient


class FakeBybitClient(BybitPublicDataClient):
    def __init__(self, pages: dict[str, bytes]) -> None:
        super().__init__(base_url="https://example.test/trading/")
        self.pages = pages

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._handle_request))

    async def _handle_request(self, request: httpx.Request) -> httpx.Response:
        content = self.pages[str(request.url)]
        return httpx.Response(200, content=content, headers={"Content-Length": str(len(content))})


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

    assert asyncio.run(client.list_symbols()) == ["BTCUSDT", "ETHUSDT"]


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

    files = asyncio.run(client.list_trade_files("btcusdt", start_date="2020-03-25", end_date="2020-03-26"))

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
    trade_file = asyncio.run(client.list_trade_files("BTCUSDT"))[0]

    path = asyncio.run(client.download_trade_file(trade_file, tmp_path))

    assert path == tmp_path / "BTCUSDT" / "BTCUSDT2020-03-25.csv"
    assert path.read_text() == "timestamp,symbol,price\n1,BTCUSDT,100\n"
    assert not (tmp_path / "BTCUSDT" / "BTCUSDT2020-03-25.csv.gz").exists()


def test_download_trade_file_cleans_partial_file_when_cancelled(tmp_path: Path) -> None:
    archive = gzip.compress(b"timestamp,symbol,price\n1,BTCUSDT,100\n")
    client = FakeBybitClient(
        {
            "https://example.test/trading/BTCUSDT/": b'<a href="BTCUSDT2020-03-25.csv.gz">file</a>',
            "https://example.test/trading/BTCUSDT/BTCUSDT2020-03-25.csv.gz": archive,
        }
    )
    trade_file = asyncio.run(client.list_trade_files("BTCUSDT"))[0]

    with pytest.raises(DownloadCancelled):
        asyncio.run(client.download_trade_file(trade_file, tmp_path, cancel_callback=lambda: True))

    assert not list((tmp_path / "BTCUSDT").glob("*.part"))
    assert not (tmp_path / "BTCUSDT" / "BTCUSDT2020-03-25.csv.gz").exists()


def test_download_trade_file_recreates_symbol_dir_for_partial_file(tmp_path: Path) -> None:
    archive = gzip.compress(b"timestamp,symbol,price\n1,BTCUSDT,100\n")
    client = FakeBybitClient(
        {
            "https://example.test/trading/BTCUSDT/": b'<a href="BTCUSDT2020-03-25.csv.gz">file</a>',
            "https://example.test/trading/BTCUSDT/BTCUSDT2020-03-25.csv.gz": archive,
        }
    )
    trade_file = asyncio.run(client.list_trade_files("BTCUSDT"))[0]
    symbol_dir = tmp_path / "BTCUSDT"
    symbol_dir.mkdir()
    symbol_dir.rmdir()

    path = asyncio.run(client.download_trade_file(trade_file, tmp_path))

    assert path.exists()
    assert not list(symbol_dir.glob("*.part"))
