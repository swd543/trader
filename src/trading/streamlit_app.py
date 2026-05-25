from __future__ import annotations

import os
import asyncio
from collections.abc import Coroutine
from datetime import date
from pathlib import Path
from typing import Any

import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from trading.db import (
    compress_old_chunks,
    ensure_schema,
    import_trade_csv,
    latest_ohlcv,
    table_counts,
    upsert_ohlcv_for_source,
)
from trading.models import DatabaseConfig
from trading.providers import HistoricalTradeProvider, MarketDataFile, create_provider, provider_names

TIMEFRAMES = ("1 minute", "5 minutes", "15 minutes", "1 hour", "1 day")


def main() -> None:
    st.set_page_config(page_title="Trading Data Loader", layout="wide")
    st.title("Trading Data Loader")

    db_config = sidebar_database_config()
    provider_slug, provider_options = sidebar_provider_config()
    client = create_provider(provider_slug, **provider_options)

    show_database_status(db_config, client.slug)
    st.divider()

    selected_symbols, start, end = ticker_explorer(client)
    st.divider()

    download_panel(client, db_config, selected_symbols, start, end)
    st.divider()

    ohlcv_panel(db_config, client.slug, selected_symbols)


def sidebar_database_config() -> DatabaseConfig:
    env_config = DatabaseConfig.from_env()
    st.sidebar.header("TimescaleDB")
    return DatabaseConfig(
        host=st.sidebar.text_input("Host", value=env_config.host),
        port=st.sidebar.number_input("Port", min_value=1, max_value=65535, value=env_config.port),
        database=st.sidebar.text_input("Database", value=env_config.database),
        user=st.sidebar.text_input("User", value=env_config.user),
        password=st.sidebar.text_input("Password", value=env_config.password, type="password"),
    )


def sidebar_provider_config() -> tuple[str, dict[str, object]]:
    st.sidebar.header("Market Data")
    selected_provider = st.sidebar.selectbox("Provider", options=provider_names(), format_func=str.title)
    provider_options: dict[str, object] = {}

    if selected_provider == "bybit":
        provider_options["base_url"] = st.sidebar.text_input("Bybit base URL", value="https://public.bybit.com/trading/")
        provider_options["timeout"] = float(st.sidebar.number_input("HTTP timeout", min_value=5, max_value=180, value=30, step=5))

    return selected_provider, provider_options


def show_database_status(db_config: DatabaseConfig, provider_slug: str) -> None:
    left, middle, right = st.columns([1, 1, 2])
    with left:
        if st.button("Initialize Schema", width="stretch"):
            run_schema_initialization(db_config, provider_slug)
    with middle:
        if st.button("Compress Old Chunks", width="stretch"):
            run_compression(db_config, provider_slug)
    with right:
        render_table_counts(db_config, provider_slug)


def run_schema_initialization(db_config: DatabaseConfig, provider_slug: str) -> None:
    try:
        run_async(initialize_schema(db_config, provider_slug))
        st.success("Schema ready.")
    except SQLAlchemyError as error:
        st.error(f"Schema initialization failed: {error}")


def run_compression(db_config: DatabaseConfig, provider_slug: str) -> None:
    try:
        compressed = run_async(compress_database(db_config, provider_slug))
        st.success(
            "Compressed chunks: "
            f"trades={compressed['trades']}, ohlcv={compressed['ohlcv']}"
        )
    except SQLAlchemyError as error:
        st.error(f"Compression failed: {error}")


def render_table_counts(db_config: DatabaseConfig, provider_slug: str) -> None:
    try:
        counts = run_async(fetch_table_counts(db_config, provider_slug))
        st.metric("Raw trades", f"{counts['trades']:,}")
        st.metric("OHLCV rows", f"{counts['ohlcv']:,}")
    except SQLAlchemyError:
        st.info("Database not connected.")


def ticker_explorer(client: HistoricalTradeProvider) -> tuple[list[str], date, date]:
    st.subheader("Tickers")
    controls = st.columns([3, 1, 1])
    provider_options = provider_cache_options(client)
    with controls[1]:
        start = st.date_input("Start", value=date(2020, 3, 25))
    with controls[2]:
        end = st.date_input("End", value=date(2020, 3, 26))

    if st.button("Refresh Tickers"):
        cached_symbols.clear()

    try:
        symbols = cached_symbols(client.slug, provider_options)
    except Exception as error:
        st.error(f"Could not fetch tickers: {error}")
        return [], start, end

    with controls[0]:
        selected_symbols = st.multiselect(
            "Ticker filter",
            options=symbols,
            default=default_symbols(symbols),
            placeholder="Type to search available tickers",
        )

    if selected_symbols:
        preview_files(client, selected_symbols, start, end)

    return selected_symbols, start, end


def preview_files(client: HistoricalTradeProvider, selected_symbols: list[str], start: date, end: date) -> None:
    rows: list[dict[str, Any]] = []
    provider_options = provider_cache_options(client)
    for symbol in selected_symbols:
        try:
            files = cached_trade_files(
                client.slug,
                provider_options,
                symbol,
                start.isoformat(),
                end.isoformat(),
            )
        except Exception as error:
            st.warning(f"{symbol}: {error}")
            continue
        rows.extend(
            {
                "symbol": trade_file.symbol,
                "date": trade_file.trade_date,
                "filename": trade_file.filename,
            }
            for trade_file in files
        )

    st.dataframe(rows, width="stretch", hide_index=True)


def download_panel(
    client: HistoricalTradeProvider,
    db_config: DatabaseConfig,
    selected_symbols: list[str],
    start: date,
    end: date,
) -> None:
    st.subheader("Download And Ingest")
    options = st.columns([2, 1, 1, 1, 1])
    with options[0]:
        output_dir = Path(st.text_input("Output directory", value=f"data/{client.slug}"))
    with options[1]:
        timeframe = st.selectbox("OHLCV timeframe", options=TIMEFRAMES)
    with options[2]:
        max_concurrent = st.number_input("Max concurrent files", min_value=1, max_value=8, value=8, step=1)
    with options[3]:
        overwrite = st.checkbox("Overwrite files", value=False)
    with options[4]:
        compress_after = st.checkbox("Compress old chunks", value=True)

    disabled = not selected_symbols
    if st.button("Start Download And Ingest", disabled=disabled, width="stretch"):
        run_download_and_ingest(
            client,
            db_config,
            selected_symbols,
            start,
            end,
            output_dir,
            timeframe,
            max_concurrent=max_concurrent,
            overwrite=overwrite,
            compress_after=compress_after,
        )


def run_download_and_ingest(
    client: HistoricalTradeProvider,
    db_config: DatabaseConfig,
    selected_symbols: list[str],
    start: date,
    end: date,
    output_dir: Path,
    timeframe: str,
    *,
    max_concurrent: int,
    overwrite: bool,
    compress_after: bool,
) -> None:
    job_log: list[dict[str, Any]] = []
    log_slot = st.empty()
    overall_progress = st.progress(0, text="Preparing files")
    download_progress = st.progress(0, text="Waiting to download")
    ingest_slot = st.empty()

    try:
        files = collect_trade_files(client, selected_symbols, start, end)
        if not files:
            st.warning("No files found for the selected symbols and dates.")
            return

        run_async(
            download_and_ingest_files(
                client,
                db_config,
                files,
                output_dir,
                timeframe,
                max_concurrent=max_concurrent,
                overwrite=overwrite,
                compress_after=compress_after,
                job_log=job_log,
                log_slot=log_slot,
                overall_progress=overall_progress,
                download_progress=download_progress,
                ingest_slot=ingest_slot,
            )
        )
    except Exception as error:
        st.error(f"Job failed: {error}")


def collect_trade_files(
    client: HistoricalTradeProvider,
    selected_symbols: list[str],
    start: date,
    end: date,
) -> list[MarketDataFile]:
    files: list[MarketDataFile] = []
    for symbol in selected_symbols:
        files.extend(client.list_trade_files(symbol, start_date=start, end_date=end))
    return files


async def download_and_ingest_files(
    client: HistoricalTradeProvider,
    db_config: DatabaseConfig,
    files: list[MarketDataFile],
    output_dir: Path,
    timeframe: str,
    *,
    max_concurrent: int,
    overwrite: bool,
    compress_after: bool,
    job_log: list[dict[str, Any]],
    log_slot: Any,
    overall_progress: Any,
    download_progress: Any,
    ingest_slot: Any,
) -> None:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine, provider=client.slug, symbols=sorted({file.symbol for file in files}))
        total_files = len(files)
        if max_concurrent > 1:
            await download_and_ingest_concurrently(
                client,
                engine,
                files,
                output_dir,
                timeframe,
                max_concurrent=max_concurrent,
                overwrite=overwrite,
                job_log=job_log,
                log_slot=log_slot,
                overall_progress=overall_progress,
                download_progress=download_progress,
                ingest_slot=ingest_slot,
            )
            if compress_after:
                compressed = await compress_old_chunks(engine, provider=client.slug, older_than="30 days")
                st.info(
                    "Compressed chunks: "
                    f"trades={compressed['trades']}, ohlcv={compressed['ohlcv']}"
                )
            return

        for index, trade_file in enumerate(files, start=1):
            overall_progress.progress((index - 1) / total_files, text=f"{index}/{total_files}: {trade_file.filename}")

            def on_download(downloaded_bytes: int, total_bytes: int | None) -> None:
                if total_bytes:
                    download_progress.progress(
                        min(downloaded_bytes / total_bytes, 1.0),
                        text=f"Downloading {trade_file.filename}: {downloaded_bytes / total_bytes:.0%}",
                    )
                else:
                    download_progress.progress(0, text=f"Downloading {trade_file.filename}: {downloaded_bytes:,} bytes")

            path = client.download_trade_file(
                trade_file,
                output_dir,
                overwrite=overwrite,
                progress_callback=on_download,
            )
            download_progress.progress(1.0, text=f"Downloaded {trade_file.filename}")

            def on_insert(rows_read: int) -> None:
                ingest_slot.info(f"Inserting {trade_file.filename}: {rows_read:,} rows staged")

            import_result = await import_trade_csv(
                engine,
                path,
                provider=client.slug,
                symbol=trade_file.symbol,
                row_iterator=client.iter_trade_rows,
                progress_callback=on_insert,
            )
            aggregate_result = await upsert_ohlcv_for_source(
                engine,
                import_result.source_file,
                provider=client.slug,
                symbol=import_result.symbol,
                timeframe=timeframe,
            )
            ingest_slot.success(
                f"Inserted {import_result.rows_inserted:,}/{import_result.rows_read:,} raw rows; "
                f"upserted {aggregate_result.rows_upserted:,} OHLCV rows"
            )

            job_log.append(
                {
                    "file": trade_file.filename,
                    "raw_rows": import_result.rows_read,
                    "inserted_rows": import_result.rows_inserted,
                    "ohlcv_rows": aggregate_result.rows_upserted,
                    "min_ts": import_result.min_ts,
                    "max_ts": import_result.max_ts,
                }
            )
            log_slot.dataframe(job_log, width="stretch", hide_index=True)
            overall_progress.progress(index / total_files, text=f"{index}/{total_files}: complete")

        if compress_after:
            compressed = await compress_old_chunks(engine, provider=client.slug, older_than="30 days")
            st.info(
                "Compressed chunks: "
                f"trades={compressed['trades']}, ohlcv={compressed['ohlcv']}"
            )
    finally:
        await engine.dispose()


async def download_and_ingest_concurrently(
    client: HistoricalTradeProvider,
    engine: Any,
    files: list[MarketDataFile],
    output_dir: Path,
    timeframe: str,
    *,
    max_concurrent: int,
    overwrite: bool,
    job_log: list[dict[str, Any]],
    log_slot: Any,
    overall_progress: Any,
    download_progress: Any,
    ingest_slot: Any,
) -> None:
    semaphore = asyncio.Semaphore(max_concurrent)
    completed = 0
    total_files = len(files)
    overall_progress.progress(0, text=f"Running up to {max_concurrent} files at a time")

    async def process_file(trade_file: MarketDataFile) -> dict[str, Any]:
        async with semaphore:
            path = await asyncio.to_thread(
                client.download_trade_file,
                trade_file,
                output_dir,
                overwrite=overwrite,
            )
            import_result = await import_trade_csv(
                engine,
                path,
                provider=client.slug,
                symbol=trade_file.symbol,
                row_iterator=client.iter_trade_rows,
            )
            aggregate_result = await upsert_ohlcv_for_source(
                engine,
                import_result.source_file,
                provider=client.slug,
                symbol=import_result.symbol,
                timeframe=timeframe,
            )
            return {
                "file": trade_file.filename,
                "raw_rows": import_result.rows_read,
                "inserted_rows": import_result.rows_inserted,
                "ohlcv_rows": aggregate_result.rows_upserted,
                "min_ts": import_result.min_ts,
                "max_ts": import_result.max_ts,
            }

    tasks = [asyncio.create_task(process_file(trade_file)) for trade_file in files]
    for task in asyncio.as_completed(tasks):
        row = await task
        completed += 1
        job_log.append(row)
        log_slot.dataframe(job_log, width="stretch", hide_index=True)
        overall_progress.progress(completed / total_files, text=f"{completed}/{total_files}: complete")
        download_progress.progress(completed / total_files, text=f"Completed {completed}/{total_files} files")

    ingest_slot.success(f"Completed {completed:,} files with max concurrency {max_concurrent}.")


def ohlcv_panel(db_config: DatabaseConfig, provider_slug: str, selected_symbols: list[str]) -> None:
    st.subheader("OHLCV")
    symbol = st.selectbox("Symbol", options=["", *selected_symbols], format_func=lambda value: value or "All selected")
    try:
        rows = run_async(
            fetch_latest_ohlcv(db_config, provider_slug=provider_slug, symbol=symbol or None, symbols=selected_symbols)
        )
        st.dataframe(rows, width="stretch", hide_index=True)
    except SQLAlchemyError as error:
        st.info(f"OHLCV unavailable: {error}")


async def initialize_schema(db_config: DatabaseConfig, provider_slug: str) -> None:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine, provider=provider_slug)
    finally:
        await engine.dispose()


async def compress_database(db_config: DatabaseConfig, provider_slug: str) -> dict[str, int]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine, provider=provider_slug)
        return await compress_old_chunks(engine, provider=provider_slug, older_than="30 days")
    finally:
        await engine.dispose()


async def fetch_table_counts(db_config: DatabaseConfig, provider_slug: str) -> dict[str, int]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine, provider=provider_slug)
        return await table_counts(engine, provider=provider_slug)
    finally:
        await engine.dispose()


async def fetch_latest_ohlcv(
    db_config: DatabaseConfig,
    *,
    provider_slug: str,
    symbol: str | None,
    symbols: list[str],
) -> list[dict[str, object]]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine, provider=provider_slug)
        rows = await latest_ohlcv(engine, provider=provider_slug, symbol=symbol, symbols=symbols)
        return [row.model_dump() for row in rows]
    finally:
        await engine.dispose()


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coro)


def provider_cache_options(client: HistoricalTradeProvider) -> tuple[tuple[str, object], ...]:
    if client.slug == "bybit":
        return (
            ("base_url", getattr(client, "base_url", "")),
            ("timeout", getattr(client, "timeout", 30.0)),
        )
    return ()


def provider_kwargs(options: tuple[tuple[str, object], ...]) -> dict[str, object]:
    return dict(options)


def default_symbols(symbols: list[str]) -> list[str]:
    if "BTCUSDT" in symbols:
        return ["BTCUSDT"]
    return symbols[:1]


@st.cache_data(ttl=3600)
def cached_symbols(provider_slug: str, options: tuple[tuple[str, object], ...]) -> list[str]:
    return create_provider(provider_slug, **provider_kwargs(options)).list_symbols()


@st.cache_data(ttl=300)
def cached_trade_files(
    provider_slug: str,
    options: tuple[tuple[str, object], ...],
    symbol: str,
    start: str,
    end: str,
) -> list[MarketDataFile]:
    return create_provider(provider_slug, **provider_kwargs(options)).list_trade_files(
        symbol,
        start_date=start,
        end_date=end,
    )


if __name__ == "__main__":
    os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
    main()
