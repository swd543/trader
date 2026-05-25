from __future__ import annotations

import re
from datetime import datetime
from functools import lru_cache

from sqlalchemy import BigInteger, DateTime, Float, Index, PrimaryKeyConstraint, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9_-]+$")
PROVIDER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
SOURCE_FILE_PATTERN = re.compile(r"(?P<symbol>.+?)(?P<date>\d{4}-\d{2}-\d{2})\.csv$")


class Base(DeclarativeBase):
    pass


class BybitTradeModel(Base):
    __abstract__ = True

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    tick_direction: Mapped[str | None] = mapped_column(String)
    trade_id: Mapped[str] = mapped_column(String, nullable=False)
    gross_value: Mapped[float | None] = mapped_column(Float)
    home_notional: Mapped[float | None] = mapped_column(Float)
    foreign_notional: Mapped[float | None] = mapped_column(Float)
    source_file: Mapped[str] = mapped_column(String, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class BybitOhlcvModel(Base):
    __abstract__ = True

    bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    quote_volume: Mapped[float | None] = mapped_column(Float)
    trade_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(normalized):
        raise ValueError(f"Unsupported symbol for table name: {symbol!r}")
    return normalized


def normalize_provider(provider: str) -> str:
    normalized = provider.strip().lower()
    if not PROVIDER_PATTERN.fullmatch(normalized):
        raise ValueError(f"Unsupported provider for table name: {provider!r}")
    return normalized


def symbol_from_source_file(source_file: str) -> str:
    match = SOURCE_FILE_PATTERN.fullmatch(source_file)
    if match is None:
        raise ValueError(f"Cannot infer symbol from Bybit source file: {source_file}")
    return normalize_symbol(match.group("symbol"))


def trade_table_prefix(provider: str) -> str:
    return f"{normalize_provider(provider)}_trades_"


def ohlcv_table_prefix(provider: str) -> str:
    return f"{normalize_provider(provider)}_ohlcv_"


def table_name_for_symbol(provider: str, symbol: str, prefix: str) -> str:
    return f"{normalize_provider(provider)}_{prefix}_{symbol_identifier_suffix(symbol)}"


def symbol_identifier_suffix(symbol: str) -> str:
    normalized = normalize_symbol(symbol)
    parts: list[str] = []
    for character in normalized:
        if character == "-":
            parts.append("_h_")
        elif character == "_":
            parts.append("_u_")
        else:
            parts.append(character.lower())
    return "".join(parts)


def symbol_from_identifier_suffix(suffix: str) -> str:
    characters: list[str] = []
    index = 0
    while index < len(suffix):
        if suffix.startswith("_h_", index):
            characters.append("-")
            index += 3
        elif suffix.startswith("_u_", index):
            characters.append("_")
            index += 3
        else:
            characters.append(suffix[index].upper())
            index += 1
    return normalize_symbol("".join(characters))


@lru_cache(maxsize=None)
def trade_model_for_symbol(provider: str, symbol: str) -> type[BybitTradeModel]:
    normalized_provider = normalize_provider(provider)
    normalized = normalize_symbol(symbol)
    table_name = table_name_for_symbol(normalized_provider, normalized, "trades")
    class_name = f"{normalized_provider.title().replace('_', '')}Trade{symbol_identifier_suffix(normalized).title().replace('_', '')}"
    return type(
        class_name,
        (BybitTradeModel,),
        {
            "__tablename__": table_name,
            "__module__": __name__,
            "__table_args__": (
                PrimaryKeyConstraint("trade_id", "ts"),
                Index(f"{table_name}_ts_idx", "ts"),
            ),
        },
    )


@lru_cache(maxsize=None)
def ohlcv_model_for_symbol(provider: str, symbol: str) -> type[BybitOhlcvModel]:
    normalized_provider = normalize_provider(provider)
    normalized = normalize_symbol(symbol)
    table_name = table_name_for_symbol(normalized_provider, normalized, "ohlcv")
    class_name = f"{normalized_provider.title().replace('_', '')}Ohlcv{symbol_identifier_suffix(normalized).title().replace('_', '')}"
    return type(
        class_name,
        (BybitOhlcvModel,),
        {
            "__tablename__": table_name,
            "__module__": __name__,
            "__table_args__": (
                PrimaryKeyConstraint("timeframe", "bucket"),
                Index(f"{table_name}_bucket_idx", "bucket"),
            ),
        },
    )
