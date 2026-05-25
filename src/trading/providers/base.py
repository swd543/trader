from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar, Literal

type DownloadProgress = Callable[[int, int | None], None]
type CancelCheck = Callable[[], bool]
type TradeRow = tuple[datetime, str, str, float, float, str, str, float | None, float | None, float | None, str]
"""Importer row shape: ts, symbol, side, size, price, tick_direction, trade_id, gross_value, home_notional, foreign_notional, source_file."""


class DownloadCancelled(RuntimeError):
    """Raised when a market-data download job is cancelled."""


@dataclass(frozen=True, slots=True)
class MarketDataFile:
    """A dated raw trade file advertised by a provider."""

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


@dataclass(frozen=True, slots=True)
class ProviderOption:
    """Provider constructor option exposed by both Streamlit and the CLI."""

    name: str
    label: str
    default: str | int | float | bool
    value_type: Literal["str", "int", "float", "bool"]
    help: str | None = None
    min_value: int | float | None = None
    max_value: int | float | None = None
    step: int | float | None = None

    @property
    def flag(self) -> str:
        return f"--{self.name.replace('_', '-')}"


class HistoricalTradeProvider(ABC):
    """Base class for raw historical trade providers.

    Implementations must expose discoverable symbols, dated trade files,
    cancellable downloads, and a parser that maps provider-specific CSV rows
    into the common TradeRow tuple consumed by the database importer.
    """

    slug: ClassVar[str]
    """Stable lowercase id used in table names, CLI flags, and the registry."""

    display_name: ClassVar[str]
    """Human-readable name for UI labels."""

    default_output_dir: ClassVar[str]
    """Default local download directory for this provider."""

    @classmethod
    def option_specs(cls) -> tuple[ProviderOption, ...]:
        """Constructor options exposed by generic UI and CLI wiring."""
        return ()

    def cache_options(self) -> tuple[tuple[str, object], ...]:
        """Stable provider option values used as Streamlit cache keys."""
        return tuple((option.name, getattr(self, option.name)) for option in self.option_specs())

    @abstractmethod
    async def list_symbols(self) -> list[str]:
        """Return provider symbols in normalized exchange format, for example BTCUSDT."""
        ...

    @abstractmethod
    async def list_trade_files(
        self,
        symbol: str,
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[MarketDataFile]:
        """Return available raw trade files for a symbol, optionally date-filtered."""
        ...

    @abstractmethod
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
        """Download one raw trade file and return the local parseable path."""
        ...

    @abstractmethod
    def iter_trade_rows(self, path: Path, source_file: str) -> Iterator[TradeRow]:
        """Yield normalized TradeRow values from a downloaded provider file."""
        ...
