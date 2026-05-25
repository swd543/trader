from __future__ import annotations

import os
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)


class DatabaseConfig(FrozenModel):
    host: str = "localhost"
    port: int = 5432
    database: str = "trading"
    user: str = "postgres"
    password: str = Field(default="postgres", repr=False)

    @classmethod
    def from_env(cls) -> DatabaseConfig:
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "trading"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )

    def url(self) -> URL:
        return URL.create(
            "postgresql+asyncpg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def create_engine(self) -> AsyncEngine:
        return create_async_engine(self.url(), pool_pre_ping=True)


class ImportResult(FrozenModel):
    symbol: str
    trades_table: str
    ohlcv_table: str
    source_file: str
    rows_read: int
    rows_inserted: int
    min_ts: datetime | None
    max_ts: datetime | None


class AggregateResult(FrozenModel):
    symbol: str
    ohlcv_table: str
    rows_upserted: int


class OhlcvRow(FrozenModel):
    bucket: datetime
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float | None
    trade_count: int
