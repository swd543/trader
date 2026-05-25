from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Protocol

type DownloadProgress = Callable[[int, int | None], None]
type InsertProgress = Callable[[int], None]
type TradeRow = tuple[datetime, str, str, float, float, str, str, float | None, float | None, float | None, str]


@dataclass(frozen=True, slots=True)
class MarketDataFile:
    provider: str
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


class HistoricalTradeProvider(Protocol):
    slug: str
    display_name: str

    def list_symbols(self) -> list[str]: ...

    def list_trade_files(
        self,
        symbol: str,
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[MarketDataFile]: ...

    def download_trade_file(
        self,
        trade_file: MarketDataFile,
        output_dir: str | Path,
        *,
        extract: bool = True,
        overwrite: bool = False,
        keep_archive: bool = False,
        progress_callback: DownloadProgress | None = None,
    ) -> Path: ...

    def iter_trade_rows(self, path: Path, source_file: str) -> Iterator[TradeRow]: ...
