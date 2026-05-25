from __future__ import annotations

import os
import asyncio
from collections.abc import Coroutine
from datetime import date
from pathlib import Path
from typing import Any

import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from trading.bybit import BybitPublicDataClient, TradeFile
from trading.db import (
    compress_old_chunks,
    ensure_schema,
    import_bybit_csv,
    latest_ohlcv,
    table_counts,
    upsert_ohlcv_for_source,
)
from trading.models import DatabaseConfig

TIMEFRAMES = ("1 minute", "5 minutes", "15 minutes", "1 hour", "1 day")


def main() -> None:
    st.set_page_config(page_title="Trading Data Loader", layout="wide")
    st.title("Trading Data Loader")

    db_config = sidebar_database_config()
    bybit_base_url = st.sidebar.text_input("Bybit base URL", value="https://public.bybit.com/trading/")
    timeout = st.sidebar.number_input("HTTP timeout", min_value=5, max_value=180, value=30, step=5)

    client = BybitPublicDataClient(base_url=bybit_base_url, timeout=float(timeout))

    show_database_status(db_config)
    st.divider()

    selected_symbols, start, end = ticker_explorer(client)
    st.divider()

    download_panel(client, db_config, selected_symbols, start, end)
    st.divider()

    ohlcv_panel(db_config, selected_symbols)


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


def show_database_status(db_config: DatabaseConfig) -> None:
    left, middle, right = st.columns([1, 1, 2])
    with left:
        if st.button("Initialize Schema", width="stretch"):
            run_schema_initialization(db_config)
    with middle:
        if st.button("Compress Old Chunks", width="stretch"):
            run_compression(db_config)
    with right:
        render_table_counts(db_config)


def run_schema_initialization(db_config: DatabaseConfig) -> None:
    try:
        run_async(initialize_schema(db_config))
        st.success("Schema ready.")
    except SQLAlchemyError as error:
        st.error(f"Schema initialization failed: {error}")


def run_compression(db_config: DatabaseConfig) -> None:
    try:
        compressed = run_async(compress_database(db_config))
        st.success(
            "Compressed chunks: "
            f"bybit_trades={compressed['bybit_trades']}, bybit_ohlcv={compressed['bybit_ohlcv']}"
        )
    except SQLAlchemyError as error:
        st.error(f"Compression failed: {error}")


def render_table_counts(db_config: DatabaseConfig) -> None:
    try:
        counts = run_async(fetch_table_counts(db_config))
        st.metric("Raw trades", f"{counts['bybit_trades']:,}")
        st.metric("OHLCV rows", f"{counts['bybit_ohlcv']:,}")
    except SQLAlchemyError:
        st.info("Database not connected.")


def ticker_explorer(client: BybitPublicDataClient) -> tuple[list[str], date, date]:
    st.subheader("Tickers")
    controls = st.columns([2, 1, 1])
    with controls[0]:
        filter_text = st.text_input("Filter", value="BTC")
    with controls[1]:
        start = st.date_input("Start", value=date(2020, 3, 25))
    with controls[2]:
        end = st.date_input("End", value=date(2020, 3, 26))

    if st.button("Refresh Tickers"):
        cached_symbols.clear()

    try:
        symbols = cached_symbols(client.base_url, client.timeout)
    except Exception as error:
        st.error(f"Could not fetch tickers: {error}")
        return [], start, end

    filtered_symbols = [symbol for symbol in symbols if filter_text.upper() in symbol.upper()]
    selected_symbols = st.multiselect("Symbols", options=filtered_symbols, default=filtered_symbols[:1])

    if selected_symbols:
        preview_files(client, selected_symbols, start, end)

    return selected_symbols, start, end


def preview_files(client: BybitPublicDataClient, selected_symbols: list[str], start: date, end: date) -> None:
    rows: list[dict[str, Any]] = []
    for symbol in selected_symbols:
        try:
            files = cached_trade_files(client.base_url, client.timeout, symbol, start.isoformat(), end.isoformat())
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
    client: BybitPublicDataClient,
    db_config: DatabaseConfig,
    selected_symbols: list[str],
    start: date,
    end: date,
) -> None:
    st.subheader("Download And Ingest")
    options = st.columns([2, 1, 1, 1])
    with options[0]:
        output_dir = Path(st.text_input("Output directory", value="data/bybit"))
    with options[1]:
        timeframe = st.selectbox("OHLCV timeframe", options=TIMEFRAMES)
    with options[2]:
        overwrite = st.checkbox("Overwrite files", value=False)
    with options[3]:
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
            overwrite=overwrite,
            compress_after=compress_after,
        )


def run_download_and_ingest(
    client: BybitPublicDataClient,
    db_config: DatabaseConfig,
    selected_symbols: list[str],
    start: date,
    end: date,
    output_dir: Path,
    timeframe: str,
    *,
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
    client: BybitPublicDataClient,
    selected_symbols: list[str],
    start: date,
    end: date,
) -> list[TradeFile]:
    files: list[TradeFile] = []
    for symbol in selected_symbols:
        files.extend(client.list_trade_files(symbol, start_date=start, end_date=end))
    return files


async def download_and_ingest_files(
    client: BybitPublicDataClient,
    db_config: DatabaseConfig,
    files: list[TradeFile],
    output_dir: Path,
    timeframe: str,
    *,
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
        await ensure_schema(engine)
        total_files = len(files)
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

            import_result = await import_bybit_csv(engine, path, progress_callback=on_insert)
            aggregate_result = await upsert_ohlcv_for_source(engine, import_result.source_file, timeframe=timeframe)
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
            compressed = await compress_old_chunks(engine, older_than="30 days")
            st.info(
                "Compressed chunks: "
                f"bybit_trades={compressed['bybit_trades']}, bybit_ohlcv={compressed['bybit_ohlcv']}"
            )
    finally:
        await engine.dispose()


def ohlcv_panel(db_config: DatabaseConfig, selected_symbols: list[str]) -> None:
    st.subheader("OHLCV")
    symbol = st.selectbox("Symbol", options=["", *selected_symbols], format_func=lambda value: value or "All selected")
    try:
        rows = run_async(fetch_latest_ohlcv(db_config, symbol=symbol or None, symbols=selected_symbols))
        st.dataframe(rows, width="stretch", hide_index=True)
    except SQLAlchemyError as error:
        st.info(f"OHLCV unavailable: {error}")


async def initialize_schema(db_config: DatabaseConfig) -> None:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine)
    finally:
        await engine.dispose()


async def compress_database(db_config: DatabaseConfig) -> dict[str, int]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine)
        return await compress_old_chunks(engine, older_than="30 days")
    finally:
        await engine.dispose()


async def fetch_table_counts(db_config: DatabaseConfig) -> dict[str, int]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine)
        return await table_counts(engine)
    finally:
        await engine.dispose()


async def fetch_latest_ohlcv(
    db_config: DatabaseConfig,
    *,
    symbol: str | None,
    symbols: list[str],
) -> list[dict[str, object]]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine)
        rows = await latest_ohlcv(engine, symbol=symbol, symbols=symbols)
        return [row.model_dump() for row in rows]
    finally:
        await engine.dispose()


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coro)


@st.cache_data(ttl=3600)
def cached_symbols(base_url: str, timeout: float) -> list[str]:
    return BybitPublicDataClient(base_url=base_url, timeout=timeout).list_symbols()


@st.cache_data(ttl=300)
def cached_trade_files(base_url: str, timeout: float, symbol: str, start: str, end: str) -> list[TradeFile]:
    return BybitPublicDataClient(base_url=base_url, timeout=timeout).list_trade_files(
        symbol,
        start_date=start,
        end_date=end,
    )


if __name__ == "__main__":
    os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
    main()
