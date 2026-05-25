from __future__ import annotations

import csv
import gzip
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urljoin

import httpx

from trading.models import normalize_symbol
from trading.providers.base import (
    CancelCheck,
    DownloadCancelled,
    DownloadProgress,
    HistoricalTradeProvider,
    MarketDataFile,
    ProviderOption,
    TradeRow,
)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://public.bybit.com/trading/"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_USER_AGENT = "trading-bybit-client/0.1"
TRADE_FILE_PATTERN = re.compile(r"(?P<symbol>.+?)(?P<date>\d{4}-\d{2}-\d{2})\.csv(?:\.gz)?$")


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return

        for name, value in attrs:
            if name == "href" and value:
                self.links.append(value)


class BybitPublicDataClient(HistoricalTradeProvider):
    """Client for Bybit's public trade-data archive."""

    slug = "bybit"
    display_name = "Bybit"
    default_output_dir = "data/bybit"

    @classmethod
    def option_specs(cls) -> tuple[ProviderOption, ...]:
        return (
            ProviderOption(
                name="base_url",
                label="Bybit base URL",
                default=DEFAULT_BASE_URL,
                value_type="str",
                help="Public Bybit trade archive root URL.",
            ),
            ProviderOption(
                name="timeout",
                label="HTTP timeout",
                default=DEFAULT_TIMEOUT_SECONDS,
                value_type="float",
                min_value=5,
                max_value=180,
                step=5,
                help="HTTP timeout in seconds.",
            ),
        )

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.base_url = self._directory_url(base_url)
        self.timeout = timeout
        self.user_agent = user_agent

    async def list_symbols(self) -> list[str]:
        logger.debug(
            "Fetching Bybit symbol index",
            extra={"event": "provider.symbols.fetch", "provider": self.slug, "url": self.base_url},
        )
        links = await self._list_links(self.base_url)
        symbols = sorted({link.rstrip("/") for link in links if self._is_directory_link(link)})
        logger.info(
            "Found Bybit symbols",
            extra={"event": "provider.symbols.found", "provider": self.slug, "symbol_count": len(symbols)},
        )
        return symbols

    async def list_trade_files(
        self,
        symbol: str,
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[MarketDataFile]:
        symbol = normalize_symbol(symbol)
        start = self._parse_optional_date(start_date)
        end = self._parse_optional_date(end_date)
        symbol_url = self._symbol_url(symbol)

        logger.debug(
            "Fetching Bybit file index",
            extra={"event": "provider.files.fetch", "provider": self.slug, "symbol": symbol, "url": symbol_url},
        )
        files: list[MarketDataFile] = []
        for link in await self._list_links(symbol_url):
            trade_file = self._trade_file_from_link(symbol, symbol_url, link)
            if trade_file is None:
                continue
            if start is not None and trade_file.trade_date < start:
                continue
            if end is not None and trade_file.trade_date > end:
                continue
            files.append(trade_file)

        files.sort(key=lambda item: (item.trade_date, item.filename))
        logger.info(
            "Found Bybit trade files",
            extra={"event": "provider.files.found", "provider": self.slug, "symbol": symbol, "file_count": len(files)},
        )
        return files

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
        symbol_dir = Path(output_dir) / trade_file.symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        final_path = symbol_dir / (trade_file.csv_filename if extract else trade_file.filename)
        if final_path.exists() and not overwrite:
            logger.info(
                "Skipping existing trade file",
                extra={
                    "event": "provider.download.skip_existing",
                    "provider": self.slug,
                    "symbol": trade_file.symbol,
                    "source_file": trade_file.filename,
                    "path": str(final_path),
                },
            )
            return final_path

        archive_path = symbol_dir / trade_file.filename
        if archive_path.exists() and not overwrite:
            logger.debug(
                "Using existing archive",
                extra={
                    "event": "provider.download.use_existing_archive",
                    "provider": self.slug,
                    "symbol": trade_file.symbol,
                    "source_file": trade_file.filename,
                    "path": str(archive_path),
                },
            )
        else:
            logger.info(
                "Downloading trade file",
                extra={
                    "event": "provider.download.start",
                    "provider": self.slug,
                    "symbol": trade_file.symbol,
                    "source_file": trade_file.filename,
                    "url": trade_file.url,
                    "path": str(archive_path),
                },
            )
            await self._download_to_path(trade_file.url, archive_path, progress_callback, cancel_callback)

        if not extract or not trade_file.compressed:
            return archive_path

        logger.info(
            "Extracting trade archive",
            extra={
                "event": "provider.download.extract",
                "provider": self.slug,
                "symbol": trade_file.symbol,
                "source_file": trade_file.filename,
                "archive_path": str(archive_path),
                "path": str(final_path),
            },
        )
        self._extract_gzip_to_path(archive_path, final_path, cancel_callback)

        if not keep_archive:
            archive_path.unlink()
            logger.debug(
                "Removed trade archive",
                extra={
                    "event": "provider.download.archive_removed",
                    "provider": self.slug,
                    "symbol": trade_file.symbol,
                    "source_file": trade_file.filename,
                    "path": str(archive_path),
                },
            )

        return final_path

    def iter_trade_rows(self, path: Path, source_file: str) -> Iterator[TradeRow]:
        with path.open(newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield (
                    datetime.fromtimestamp(float(row["timestamp"]), UTC),
                    normalize_symbol(row["symbol"]),
                    row["side"],
                    float(row["size"]),
                    float(row["price"]),
                    row.get("tickDirection") or "",
                    row["trdMatchID"],
                    self._optional_float(row.get("grossValue")),
                    self._optional_float(row.get("homeNotional")),
                    self._optional_float(row.get("foreignNotional")),
                    source_file,
                )

    async def _list_links(self, url: str) -> list[str]:
        parser = _LinkParser()
        html = (await self._get(url)).text
        parser.feed(html)
        return parser.links

    async def _get(self, url: str) -> httpx.Response:
        async with self._client() as client:
            response = await client.get(url)
            response.raise_for_status()
            return response

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout,
            follow_redirects=True,
        )

    async def _download_to_path(
        self,
        url: str,
        path: Path,
        progress_callback: DownloadProgress | None,
        cancel_callback: CancelCheck | None,
    ) -> None:
        partial_path = path.with_name(f"{path.name}.part")
        with remove_partial_on_error(partial_path):
            async with self._client() as client:
                async with client.stream("GET", url) as response:
                    with partial_path.open("wb") as output:
                        response.raise_for_status()
                        await self._copy_response(response, output, progress_callback, cancel_callback)
            partial_path.replace(path)

    @staticmethod
    async def _copy_response(
        response: httpx.Response,
        output: BinaryIO,
        progress_callback: DownloadProgress | None,
        cancel_callback: CancelCheck | None,
    ) -> None:
        content_length = response.headers.get("Content-Length")
        total_bytes = int(content_length) if content_length else None
        downloaded_bytes = 0

        async for chunk in response.aiter_bytes(1024 * 1024):
            raise_if_cancelled(cancel_callback)
            output.write(chunk)
            downloaded_bytes += len(chunk)
            if progress_callback is not None:
                progress_callback(downloaded_bytes, total_bytes)

    @staticmethod
    def _extract_gzip_to_path(
        archive_path: Path,
        final_path: Path,
        cancel_callback: CancelCheck | None,
    ) -> None:
        partial_path = final_path.with_name(f"{final_path.name}.part")
        with remove_partial_on_error(partial_path):
            with gzip.open(archive_path, "rb") as compressed, partial_path.open("wb") as output:
                while chunk := compressed.read(1024 * 1024):
                    raise_if_cancelled(cancel_callback)
                    output.write(chunk)
            partial_path.replace(final_path)

    def _symbol_url(self, symbol: str) -> str:
        return urljoin(self.base_url, f"{symbol}/")

    @staticmethod
    def _directory_url(url: str) -> str:
        return url if url.endswith("/") else f"{url}/"

    @staticmethod
    def _is_directory_link(link: str) -> bool:
        return link.endswith("/") and not link.startswith("../") and not link.startswith("/")

    @staticmethod
    def _trade_file_from_link(symbol: str, symbol_url: str, link: str) -> MarketDataFile | None:
        filename = link.rsplit("/", 1)[-1]
        match = TRADE_FILE_PATTERN.fullmatch(filename)
        if match is None or match.group("symbol").upper() != symbol:
            return None

        trade_date = date.fromisoformat(match.group("date"))
        return MarketDataFile(
            provider=BybitPublicDataClient.slug,
            symbol=symbol,
            trade_date=trade_date,
            filename=filename,
            url=urljoin(symbol_url, filename),
        )

    @staticmethod
    def _parse_optional_date(value: str | date | None) -> date | None:
        if value is None or isinstance(value, date):
            return value
        return date.fromisoformat(value)

    @staticmethod
    def _optional_float(value: str | None) -> float | None:
        if value is None or value == "":
            return None
        return float(value)


def raise_if_cancelled(cancel_callback: CancelCheck | None) -> None:
    if cancel_callback is not None and cancel_callback():
        raise DownloadCancelled("Download job cancelled")


@contextmanager
def remove_partial_on_error(path: Path) -> Iterator[None]:
    try:
        yield
    except Exception:
        path.unlink(missing_ok=True)
        raise
