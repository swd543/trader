from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, cast

from sqlalchemy import Table, bindparam, func, literal, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, async_sessionmaker

from trading.models import (
    Base,
    AggregateResult,
    ImportResult,
    OhlcvRow,
    normalize_provider,
    normalize_symbol,
    ohlcv_table_prefix,
    ohlcv_model_for_symbol,
    symbol_from_identifier_suffix,
    symbol_from_source_file,
    trade_table_prefix,
    trade_model_for_symbol,
)
from trading.providers.base import TradeRow


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
BASE_OHLCV_TIMEFRAME = "1 minute"
BASE_OHLCV_INTERVAL_SQL = "INTERVAL '1 minute'"


async def ensure_schema(
    engine: AsyncEngine,
    *,
    provider: str = "bybit",
    symbols: Sequence[str] | None = None,
    compression_after: str = "30 days",
) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        for symbol in symbols or ():
            await _ensure_symbol_schema(conn, provider, symbol, compression_after=compression_after)


async def import_trade_csv(
    engine: AsyncEngine,
    path: Path,
    *,
    provider: str,
    symbol: str | None = None,
    row_iterator: Callable[[Path, str], Iterator[TradeRow]],
    progress_callback: Callable[[int], None] | None = None,
) -> ImportResult:
    normalized_provider = normalize_provider(provider)
    source_file = path.name
    normalized_symbol = normalize_symbol(symbol) if symbol else symbol_from_source_file(source_file)
    trade_model = trade_model_for_symbol(normalized_provider, normalized_symbol)
    ohlcv_model = ohlcv_model_for_symbol(normalized_provider, normalized_symbol)
    trades_table = trade_model.__tablename__
    ohlcv_table = ohlcv_model.__tablename__

    async with engine.begin() as conn:
        await _ensure_symbol_schema(conn, normalized_provider, normalized_symbol)
        await conn.execute(
            text(
                f"""
                CREATE TEMP TABLE market_data_trades_stage (
                    LIKE {trades_table} INCLUDING DEFAULTS
                ) ON COMMIT DROP
                """
            )
        )
        rows_read = await _copy_trade_rows(conn, path, source_file, row_iterator, progress_callback)
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
                FROM market_data_trades_stage
                ON CONFLICT DO NOTHING
                """
            )
        )
        rows_inserted = result.rowcount if result.rowcount is not None else 0
        bounds = await conn.execute(text("SELECT min(ts), max(ts) FROM market_data_trades_stage"))
        min_ts, max_ts = bounds.one_or_none() or (None, None)

    return ImportResult(
        provider=normalized_provider,
        symbol=normalized_symbol,
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
    provider: str = "bybit",
    symbol: str | None = None,
) -> AggregateResult:
    normalized_provider = normalize_provider(provider)
    normalized_symbol = normalize_symbol(symbol) if symbol else symbol_from_source_file(source_file)
    trade_model = trade_model_for_symbol(normalized_provider, normalized_symbol)
    ohlcv_model = ohlcv_model_for_symbol(normalized_provider, normalized_symbol)
    bucket = func.time_bucket(text(BASE_OHLCV_INTERVAL_SQL), trade_model.ts)
    source_query = (
        select(
            bucket.label("bucket"),
            trade_model.symbol,
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
        index_elements=[ohlcv_model.bucket],
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
        await _ensure_symbol_schema(conn, normalized_provider, normalized_symbol)
        result = await conn.execute(upsert_statement, {"source_file": source_file})
    return AggregateResult(
        provider=normalized_provider,
        symbol=normalized_symbol,
        ohlcv_table=ohlcv_model.__tablename__,
        rows_upserted=result.rowcount if result.rowcount is not None else 0,
    )


async def source_file_status(
    engine: AsyncEngine,
    *,
    provider: str,
    symbol: str,
    source_file: str,
    trade_date: date,
) -> dict[str, bool]:
    normalized_provider = normalize_provider(provider)
    normalized_symbol = normalize_symbol(symbol)
    trade_model = trade_model_for_symbol(normalized_provider, normalized_symbol)
    ohlcv_model = ohlcv_model_for_symbol(normalized_provider, normalized_symbol)
    day_start = datetime.combine(trade_date, time.min, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    async with engine.connect() as conn:
        has_trades = bool(
            await conn.scalar(
                select(literal(True))
                .select_from(trade_model)
                .where(trade_model.source_file == source_file)
                .limit(1)
            )
        )
        has_ohlcv = bool(
            await conn.scalar(
                select(literal(True))
                .select_from(ohlcv_model)
                .where(
                    ohlcv_model.bucket >= day_start,
                    ohlcv_model.bucket < day_end,
                )
                .limit(1)
            )
        )

    return {"trades": has_trades, "ohlcv": has_ohlcv}


async def compress_old_chunks(
    engine: AsyncEngine,
    *,
    provider: str = "bybit",
    older_than: str = "30 days",
) -> dict[str, int]:
    normalized_provider = normalize_provider(provider)
    compressed = {"trades": 0, "ohlcv": 0}
    async with engine.begin() as conn:
        for symbol in await _existing_symbols(conn, normalized_provider):
            for key, table_name in (
                ("trades", trade_model_for_symbol(normalized_provider, symbol).__tablename__),
                ("ohlcv", ohlcv_model_for_symbol(normalized_provider, symbol).__tablename__),
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


async def table_counts(engine: AsyncEngine, *, provider: str = "bybit") -> dict[str, int]:
    normalized_provider = normalize_provider(provider)
    counts = {"trades": 0, "ohlcv": 0}
    async with engine.connect() as conn:
        for symbol in await _existing_symbols(conn, normalized_provider):
            trade_model = trade_model_for_symbol(normalized_provider, symbol)
            ohlcv_model = ohlcv_model_for_symbol(normalized_provider, symbol)
            raw_count = await conn.scalar(select(func.count()).select_from(trade_model))
            ohlcv_count = await conn.scalar(select(func.count()).select_from(ohlcv_model))
            counts["trades"] += int(raw_count or 0)
            counts["ohlcv"] += int(ohlcv_count or 0)
    return counts


async def latest_ohlcv(
    engine: AsyncEngine,
    *,
    provider: str = "bybit",
    symbol: str | None = None,
    symbols: Sequence[str] | None = None,
    timeframe: str | None = None,
    limit: int = 200,
) -> list[OhlcvRow]:
    normalized_provider = normalize_provider(provider)
    existing_symbols = set(await list_ingested_symbols(engine, provider=normalized_provider))
    if symbol:
        selected_symbols = [normalize_symbol(symbol)]
    elif symbols:
        selected_symbols = [normalize_symbol(item) for item in symbols]
    else:
        selected_symbols = sorted(existing_symbols)
    requested_timeframe = timeframe or BASE_OHLCV_TIMEFRAME

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    rows: list[OhlcvRow] = []
    async with session_factory() as session:
        for selected_symbol in selected_symbols:
            if selected_symbol not in existing_symbols:
                continue
            ohlcv_model = ohlcv_model_for_symbol(normalized_provider, selected_symbol)
            if requested_timeframe == BASE_OHLCV_TIMEFRAME:
                statement = (
                    select(ohlcv_model)
                    .order_by(ohlcv_model.bucket.desc(), ohlcv_model.symbol)
                    .limit(limit)
                )
                symbol_rows = (await session.scalars(statement)).all()
                rows.extend(OhlcvRow.model_validate(row) for row in symbol_rows)
                continue

            bucket = func.time_bucket(text("CAST(:timeframe AS text)::interval"), ohlcv_model.bucket)
            statement = (
                select(
                    bucket.label("bucket"),
                    ohlcv_model.symbol.label("symbol"),
                    func.first(ohlcv_model.open, ohlcv_model.bucket).label("open"),
                    func.max(ohlcv_model.high).label("high"),
                    func.min(ohlcv_model.low).label("low"),
                    func.last(ohlcv_model.close, ohlcv_model.bucket).label("close"),
                    func.sum(ohlcv_model.volume).label("volume"),
                    func.sum(ohlcv_model.quote_volume).label("quote_volume"),
                    func.sum(ohlcv_model.trade_count).label("trade_count"),
                )
                .group_by(bucket, ohlcv_model.symbol)
                .order_by(bucket.desc(), ohlcv_model.symbol)
                .limit(limit)
            )
            result = await session.execute(statement, {"timeframe": requested_timeframe})
            rows.extend(OhlcvRow.model_validate(row._mapping) for row in result.all())

    return sorted(rows, key=lambda row: (row.bucket, row.symbol), reverse=True)


async def list_ingested_symbols(engine: AsyncEngine, *, provider: str = "bybit") -> list[str]:
    normalized_provider = normalize_provider(provider)
    async with engine.connect() as conn:
        return await _existing_symbols(conn, normalized_provider)


async def _ensure_symbol_schema(
    conn: AsyncConnection,
    provider: str,
    symbol: str,
    *,
    compression_after: str = "30 days",
) -> None:
    normalized_provider = normalize_provider(provider)
    trade_model = trade_model_for_symbol(normalized_provider, symbol)
    ohlcv_model = ohlcv_model_for_symbol(normalized_provider, symbol)
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
                timescaledb.compress_segmentby = 'symbol',
                timescaledb.compress_orderby = 'bucket DESC'
            )
            """
        )
    )
    await _add_compression_policy(conn, trades_table, compression_after)
    await _add_compression_policy(conn, ohlcv_table, compression_after)


async def _existing_symbols(conn: AsyncConnection, provider: str) -> list[str]:
    trade_prefix = trade_table_prefix(provider)
    ohlcv_prefix = ohlcv_table_prefix(provider)
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
        {"trade_prefix": f"{trade_prefix}%", "ohlcv_prefix": f"{ohlcv_prefix}%"},
    )
    suffixes: set[str] = set()
    for table_name in result.scalars():
        if table_name.startswith(trade_prefix):
            suffixes.add(table_name.removeprefix(trade_prefix))
        elif table_name.startswith(ohlcv_prefix):
            suffixes.add(table_name.removeprefix(ohlcv_prefix))
    return sorted(symbol_from_identifier_suffix(suffix) for suffix in suffixes)


async def _copy_trade_rows(
    conn: AsyncConnection,
    path: Path,
    source_file: str,
    row_iterator: Callable[[Path, str], Iterator[TradeRow]],
    progress_callback: Callable[[int], None] | None,
) -> int:
    rows_read = 0

    def rows() -> Iterator[TradeRow]:
        nonlocal rows_read
        for rows_read, row in enumerate(row_iterator(path, source_file), start=1):
            if progress_callback is not None and rows_read % 50_000 == 0:
                progress_callback(rows_read)
            yield row

    raw_connection = await conn.get_raw_connection()
    driver_connection: Any = raw_connection.driver_connection
    await driver_connection.copy_records_to_table("market_data_trades_stage", records=rows(), columns=TRADE_COLUMNS)

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
