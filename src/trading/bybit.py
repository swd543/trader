from __future__ import annotations

import gzip
import logging
import re
import shutil
import time
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import BinaryIO, Callable
from urllib.parse import urljoin
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://public.bybit.com/trading/"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_USER_AGENT = "trading-bybit-client/0.1"
TRADE_FILE_PATTERN = re.compile(r"(?P<symbol>.+?)(?P<date>\d{4}-\d{2}-\d{2})\.csv(?:\.gz)?$")


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return

        for name, value in attrs:
            if name == "href" and value:
                self.links.append(value)


@dataclass(frozen=True, slots=True)
class TradeFile:
    symbol: str
    trade_date: date
    filename: str
    url: str

    @property
    def compressed(self) -> bool:
        return self.filename.endswith(".gz")

    @property
    def csv_filename(self) -> str:
        if self.filename.endswith(".gz"):
            return self.filename[:-3]
        return self.filename


class BybitPublicDataClient:
    """Client for Bybit's public trade-data archive."""

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

    def list_symbols(self) -> list[str]:
        """Return symbols available in the public trading archive."""
        logger.debug("Fetching Bybit symbol index from %s", self.base_url)
        links = self._list_links(self.base_url)
        symbols = sorted({link.rstrip("/") for link in links if self._is_directory_link(link)})
        logger.info("Found %d Bybit symbols", len(symbols))
        return symbols

    def list_trade_files(
        self,
        symbol: str,
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[TradeFile]:
        """Return dated CSV trade files for a symbol, optionally filtered by date range."""
        symbol = symbol.upper()
        start = self._parse_optional_date(start_date)
        end = self._parse_optional_date(end_date)
        symbol_url = self._symbol_url(symbol)

        logger.debug("Fetching Bybit file index for %s from %s", symbol, symbol_url)
        files: list[TradeFile] = []
        for link in self._list_links(symbol_url):
            trade_file = self._trade_file_from_link(symbol, symbol_url, link)
            if trade_file is None:
                continue
            if start is not None and trade_file.trade_date < start:
                continue
            if end is not None and trade_file.trade_date > end:
                continue
            files.append(trade_file)

        files.sort(key=lambda item: (item.trade_date, item.filename))
        logger.info("Found %d Bybit trade files for %s", len(files), symbol)
        return files

    def download_trade_file(
        self,
        trade_file: TradeFile,
        output_dir: str | Path,
        *,
        extract: bool = True,
        overwrite: bool = False,
        keep_archive: bool = False,
        progress_callback: Callable[[int, int | None], None] | None = None,
    ) -> Path:
        """Download a trade file and return the final local path."""
        symbol_dir = Path(output_dir) / trade_file.symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        final_path = symbol_dir / (trade_file.csv_filename if extract else trade_file.filename)
        if final_path.exists() and not overwrite:
            logger.info("Skipping %s because %s already exists", trade_file.filename, final_path)
            return final_path

        archive_path = symbol_dir / trade_file.filename
        if archive_path.exists() and not overwrite:
            logger.debug("Using existing archive %s", archive_path)
        else:
            logger.info("Downloading %s to %s", trade_file.url, archive_path)
            with self._open_binary(trade_file.url) as response, archive_path.open("wb") as output:
                self._copy_response(response, output, progress_callback)

        if not extract or not trade_file.compressed:
            return archive_path

        logger.info("Extracting %s to %s", archive_path, final_path)
        with gzip.open(archive_path, "rb") as compressed, final_path.open("wb") as output:
            shutil.copyfileobj(compressed, output)

        if not keep_archive:
            archive_path.unlink()
            logger.debug("Removed archive %s", archive_path)

        return final_path

    def download_trades(
        self,
        symbols: list[str] | tuple[str, ...] | None,
        output_dir: str | Path,
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        extract: bool = True,
        overwrite: bool = False,
        keep_archive: bool = False,
        limit: int | None = None,
        sleep_seconds: float = 0,
    ) -> list[Path]:
        """Download trade files for one or more symbols."""
        selected_symbols = [symbol.upper() for symbol in symbols] if symbols else self.list_symbols()
        downloaded: list[Path] = []

        for symbol in selected_symbols:
            files = self.list_trade_files(symbol, start_date=start_date, end_date=end_date)
            if limit is not None:
                files = files[:limit]

            for trade_file in files:
                downloaded.append(
                    self.download_trade_file(
                        trade_file,
                        output_dir,
                        extract=extract,
                        overwrite=overwrite,
                        keep_archive=keep_archive,
                    )
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        logger.info("Downloaded %d Bybit files", len(downloaded))
        return downloaded

    def _list_links(self, url: str) -> list[str]:
        parser = LinkParser()
        with self._open_text(url) as response:
            html = response.read().decode("utf-8", errors="replace")
        parser.feed(html)
        return parser.links

    def _open_text(self, url: str) -> BinaryIO:
        return self._open_binary(url)

    def _open_binary(self, url: str) -> BinaryIO:
        request = Request(url, headers={"User-Agent": self.user_agent})
        return urlopen(request, timeout=self.timeout)

    @staticmethod
    def _copy_response(
        response: BinaryIO,
        output: BinaryIO,
        progress_callback: Callable[[int, int | None], None] | None,
    ) -> None:
        if progress_callback is None:
            shutil.copyfileobj(response, output)
            return

        headers = getattr(response, "headers", {})
        content_length = headers.get("Content-Length") if headers else None
        total_bytes = int(content_length) if content_length else None
        downloaded_bytes = 0

        while chunk := response.read(1024 * 1024):
            output.write(chunk)
            downloaded_bytes += len(chunk)
            progress_callback(downloaded_bytes, total_bytes)

    def _symbol_url(self, symbol: str) -> str:
        return urljoin(self.base_url, f"{symbol}/")

    @staticmethod
    def _directory_url(url: str) -> str:
        return url if url.endswith("/") else f"{url}/"

    @staticmethod
    def _is_directory_link(link: str) -> bool:
        return link.endswith("/") and not link.startswith("../") and not link.startswith("/")

    @staticmethod
    def _trade_file_from_link(symbol: str, symbol_url: str, link: str) -> TradeFile | None:
        filename = link.rsplit("/", 1)[-1]
        match = TRADE_FILE_PATTERN.fullmatch(filename)
        if match is None or match.group("symbol").upper() != symbol:
            return None

        trade_date = date.fromisoformat(match.group("date"))
        return TradeFile(
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
