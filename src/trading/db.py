from __future__ import annotations

import csv
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from sqlalchemy import Table, bindparam, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, async_sessionmaker

from trading.models import (
    Base,
    AggregateResult,
    ImportResult,
    OHLCV_TABLE_PREFIX,
    OhlcvRow,
    TRADE_TABLE_PREFIX,
    normalize_symbol,
    ohlcv_model_for_symbol,
    symbol_from_source_file,
    table_name_for_symbol,
    trade_model_for_symbol,
)


type TradeRow = tuple[datetime, str, str, float, float, str, str, float | None, float | None, float | None, str]

TRADE_COLUMNS = (
    "ts",
    "symbol",
    "side",
    "size",
    "price",
    "tick_direction",
    "trade_id",
    "gross_value",
    "home_notional",
    "foreign_notional",
    "source_file",
)


async def ensure_schema(
    engine: AsyncEngine,
    *,
    symbols: Sequence[str] | None = None,
    compression_after: str = "30 days",
) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        for symbol in symbols or ():
            await _ensure_symbol_schema(conn, symbol, compression_after=compression_after)


async def import_bybit_csv(
    engine: AsyncEngine,
    path: Path,
    *,
    progress_callback: Callable[[int], None] | None = None,
) -> ImportResult:
    source_file = path.name
    symbol = symbol_from_source_file(source_file)
    trade_model = trade_model_for_symbol(symbol)
    trades_table = table_name_for_symbol(symbol, TRADE_TABLE_PREFIX)
    ohlcv_table = table_name_for_symbol(symbol, OHLCV_TABLE_PREFIX)

    async with engine.begin() as conn:
        await _ensure_symbol_schema(conn, symbol)
        await conn.execute(
            text(
                f"""
                CREATE TEMP TABLE bybit_trades_stage (
                    LIKE {trades_table} INCLUDING DEFAULTS
                ) ON COMMIT DROP
                """
            )
        )
        rows_read = await _copy_trade_rows(conn, path, source_file, progress_callback)
        result = await conn.execute(
            text(
                f"""
                INSERT INTO {trades_table} (
                    ts,
                    symbol,
                    side,
                    size,
                    price,
                    tick_direction,
                    trade_id,
                    gross_value,
                    home_notional,
                    foreign_notional,
                    source_file
                )
                SELECT
                    ts,
                    symbol,
                    side,
                    size,
                    price,
                    tick_direction,
                    trade_id,
                    gross_value,
                    home_notional,
                    foreign_notional,
                    source_file
                FROM bybit_trades_stage
                ON CONFLICT DO NOTHING
                """
            )
        )
        rows_inserted = result.rowcount if result.rowcount is not None else 0
        bounds = await conn.execute(text("SELECT min(ts), max(ts) FROM bybit_trades_stage"))
        min_ts, max_ts = bounds.one_or_none() or (None, None)

    return ImportResult(
        symbol=symbol,
        trades_table=trade_model.__tablename__,
        ohlcv_table=ohlcv_table,
        source_file=source_file,
        rows_read=rows_read,
        rows_inserted=rows_inserted,
        min_ts=min_ts,
        max_ts=max_ts,
    )


async def upsert_ohlcv_for_source(
    engine: AsyncEngine,
    source_file: str,
    *,
    timeframe: str = "1 minute",
) -> AggregateResult:
    symbol = symbol_from_source_file(source_file)
    trade_model = trade_model_for_symbol(symbol)
    ohlcv_model = ohlcv_model_for_symbol(symbol)
    bucket = func.time_bucket(text("CAST(:timeframe AS text)::interval"), trade_model.ts)
    source_query = (
        select(
            bucket.label("bucket"),
            trade_model.symbol,
            bindparam("timeframe").label("timeframe"),
            func.first(trade_model.price, trade_model.ts).label("open"),
            func.max(trade_model.price).label("high"),
            func.min(trade_model.price).label("low"),
            func.last(trade_model.price, trade_model.ts).label("close"),
            func.sum(trade_model.size).label("volume"),
            func.sum(trade_model.foreign_notional).label("quote_volume"),
            func.count().label("trade_count"),
            func.now().label("updated_at"),
        )
        .where(trade_model.source_file == bindparam("source_file"))
        .group_by(bucket, trade_model.symbol)
    )
    insert_statement = pg_insert(cast(Table, ohlcv_model.__table__)).from_select(
        [
            "bucket",
            "symbol",
            "timeframe",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "updated_at",
        ],
        source_query,
    )
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=[ohlcv_model.timeframe, ohlcv_model.bucket],
        set_={
            "open": insert_statement.excluded.open,
            "high": insert_statement.excluded.high,
            "low": insert_statement.excluded.low,
            "close": insert_statement.excluded.close,
            "volume": insert_statement.excluded.volume,
            "quote_volume": insert_statement.excluded.quote_volume,
            "trade_count": insert_statement.excluded.trade_count,
            "updated_at": func.now(),
        },
    )

    async with engine.begin() as conn:
        await _ensure_symbol_schema(conn, symbol)
        result = await conn.execute(upsert_statement, {"timeframe": timeframe, "source_file": source_file})
    return AggregateResult(
        symbol=symbol,
        ohlcv_table=ohlcv_model.__tablename__,
        rows_upserted=result.rowcount if result.rowcount is not None else 0,
    )


async def compress_old_chunks(engine: AsyncEngine, *, older_than: str = "30 days") -> dict[str, int]:
    compressed = {"bybit_trades": 0, "bybit_ohlcv": 0}
    async with engine.begin() as conn:
        for symbol in await _existing_symbols(conn):
            for key, table_name in (
                ("bybit_trades", table_name_for_symbol(symbol, TRADE_TABLE_PREFIX)),
                ("bybit_ohlcv", table_name_for_symbol(symbol, OHLCV_TABLE_PREFIX)),
            ):
                result = await conn.execute(
                    text(
                        """
                        SELECT count(*)
                        FROM (
                            SELECT compress_chunk(chunk_name, if_not_compressed => TRUE)
                            FROM show_chunks(
                                CAST(:table_name AS text)::regclass,
                                older_than => CAST(:older_than AS text)::interval
                            ) AS chunk_name
                        ) chunks
                        """
                    ),
                    {"table_name": table_name, "older_than": older_than},
                )
                compressed[key] += int(result.scalar_one_or_none() or 0)
    return compressed


async def table_counts(engine: AsyncEngine) -> dict[str, int]:
    counts = {"bybit_trades": 0, "bybit_ohlcv": 0}
    async with engine.connect() as conn:
        for symbol in await _existing_symbols(conn):
            trade_model = trade_model_for_symbol(symbol)
            ohlcv_model = ohlcv_model_for_symbol(symbol)
            raw_count = await conn.scalar(select(func.count()).select_from(trade_model))
            ohlcv_count = await conn.scalar(select(func.count()).select_from(ohlcv_model))
            counts["bybit_trades"] += int(raw_count or 0)
            counts["bybit_ohlcv"] += int(ohlcv_count or 0)
    return counts


async def latest_ohlcv(
    engine: AsyncEngine,
    *,
    symbol: str | None = None,
    symbols: Sequence[str] | None = None,
    limit: int = 200,
) -> list[OhlcvRow]:
    existing_symbols = set(await list_ingested_symbols(engine))
    if symbol:
        selected_symbols = [normalize_symbol(symbol)]
    elif symbols:
        selected_symbols = [normalize_symbol(item) for item in symbols]
    else:
        selected_symbols = sorted(existing_symbols)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    rows: list[OhlcvRow] = []
    async with session_factory() as session:
        for selected_symbol in selected_symbols:
            if selected_symbol not in existing_symbols:
                continue
            ohlcv_model = ohlcv_model_for_symbol(selected_symbol)
            statement = (
                select(ohlcv_model)
                .order_by(ohlcv_model.bucket.desc(), ohlcv_model.symbol)
                .limit(limit)
            )
            symbol_rows = (await session.scalars(statement)).all()
            rows.extend(OhlcvRow.model_validate(row) for row in symbol_rows)

    return sorted(rows, key=lambda row: (row.bucket, row.symbol), reverse=True)[:limit]


async def list_ingested_symbols(engine: AsyncEngine) -> list[str]:
    async with engine.connect() as conn:
        return await _existing_symbols(conn)


def iter_bybit_trade_rows(path: Path, source_file: str) -> Iterator[TradeRow]:
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
                _optional_float(row.get("grossValue")),
                _optional_float(row.get("homeNotional")),
                _optional_float(row.get("foreignNotional")),
                source_file,
            )


async def _ensure_symbol_schema(
    conn: AsyncConnection,
    symbol: str,
    *,
    compression_after: str = "30 days",
) -> None:
    trade_model = trade_model_for_symbol(symbol)
    ohlcv_model = ohlcv_model_for_symbol(symbol)
    trades_table = trade_model.__tablename__
    ohlcv_table = ohlcv_model.__tablename__
    tables = [cast(Table, trade_model.__table__), cast(Table, ohlcv_model.__table__)]

    await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn, tables=tables))
    await conn.execute(
        text("SELECT create_hypertable(CAST(:table_name AS text)::regclass, 'ts', if_not_exists => TRUE, migrate_data => TRUE)"),
        {"table_name": trades_table},
    )
    await conn.execute(
        text(
            "SELECT create_hypertable(CAST(:table_name AS text)::regclass, 'bucket', if_not_exists => TRUE, migrate_data => TRUE)"
        ),
        {"table_name": ohlcv_table},
    )
    await conn.execute(
        text(
            f"""
            ALTER TABLE {trades_table} SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'symbol',
                timescaledb.compress_orderby = 'ts DESC'
            )
            """
        )
    )
    await conn.execute(
        text(
            f"""
            ALTER TABLE {ohlcv_table} SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'symbol,timeframe',
                timescaledb.compress_orderby = 'bucket DESC'
            )
            """
        )
    )
    await _add_compression_policy(conn, trades_table, compression_after)
    await _add_compression_policy(conn, ohlcv_table, compression_after)


async def _existing_symbols(conn: AsyncConnection) -> list[str]:
    result = await conn.execute(
        text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND (
                table_name LIKE :trade_prefix
                OR table_name LIKE :ohlcv_prefix
              )
            """
        ),
        {"trade_prefix": f"{TRADE_TABLE_PREFIX}%", "ohlcv_prefix": f"{OHLCV_TABLE_PREFIX}%"},
    )
    suffixes: set[str] = set()
    for table_name in result.scalars():
        if table_name.startswith(TRADE_TABLE_PREFIX):
            suffixes.add(table_name.removeprefix(TRADE_TABLE_PREFIX))
        elif table_name.startswith(OHLCV_TABLE_PREFIX):
            suffixes.add(table_name.removeprefix(OHLCV_TABLE_PREFIX))
    return sorted(suffix.upper() for suffix in suffixes)


async def _copy_trade_rows(
    conn: AsyncConnection,
    path: Path,
    source_file: str,
    progress_callback: Callable[[int], None] | None,
) -> int:
    rows_read = 0

    def rows() -> Iterator[TradeRow]:
        nonlocal rows_read
        for rows_read, row in enumerate(iter_bybit_trade_rows(path, source_file), start=1):
            if progress_callback is not None and rows_read % 50_000 == 0:
                progress_callback(rows_read)
            yield row

    raw_connection = await conn.get_raw_connection()
    driver_connection: Any = raw_connection.driver_connection
    await driver_connection.copy_records_to_table("bybit_trades_stage", records=rows(), columns=TRADE_COLUMNS)

    if progress_callback is not None:
        progress_callback(rows_read)

    return rows_read


async def _add_compression_policy(conn: AsyncConnection, table_name: str, compression_after: str) -> None:
    await conn.execute(
        text(
            """
            SELECT add_compression_policy(
                CAST(:table_name AS text)::regclass,
                CAST(:compression_after AS text)::interval,
                if_not_exists => TRUE
            )
            """
        ),
        {"table_name": table_name, "compression_after": compression_after},
    )


def _optional_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
