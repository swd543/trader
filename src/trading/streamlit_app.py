from __future__ import annotations

import os
import asyncio
import logging
import threading
from collections.abc import Coroutine
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, TypedDict, cast
from uuid import uuid4

import altair as alt
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
from trading.providers.base import DownloadCancelled

TIMEFRAMES = ("1 minute", "5 minutes", "15 minutes", "1 hour", "1 day")
DOWNLOAD_JOB_KEY = "download_job"
logger = logging.getLogger(__name__)


@dataclass
class DownloadJob:
    id: str = field(default_factory=lambda: uuid4().hex)
    status: str = "running"
    message: str = "Preparing files"
    overall_fraction: float = 0.0
    overall_text: str = "Preparing files"
    download_fraction: float = 0.0
    download_text: str = "Waiting to download"
    ingest_text: str = ""
    error: str | None = None
    log_rows: list[dict[str, Any]] = field(default_factory=list)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)
    thread: threading.Thread | None = None


class OhlcvChartRow(TypedDict):
    bucket: str
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int


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
    cleanup_files = st.checkbox("Delete local files after ingest", value=True)

    active_job = current_download_job()
    job_running = active_job is not None and download_job_is_running(active_job)
    disabled = not selected_symbols or job_running
    if st.button("Start Download And Ingest", disabled=disabled, width="stretch"):
        start_download_job(
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
            cleanup_files=cleanup_files,
        )

    render_download_job_status()


def start_download_job(
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
    cleanup_files: bool,
) -> None:
    job = DownloadJob()
    thread = threading.Thread(
        target=run_download_job_worker,
        name=f"download-ingest-{job.id}",
        args=(
            job,
            client,
            db_config,
            list(selected_symbols),
            start,
            end,
            output_dir,
            timeframe,
        ),
        kwargs={
            "max_concurrent": max_concurrent,
            "overwrite": overwrite,
            "compress_after": compress_after,
            "cleanup_files": cleanup_files,
        },
        daemon=True,
    )
    job.thread = thread
    st.session_state[DOWNLOAD_JOB_KEY] = job
    thread.start()
    st.rerun()


def run_download_job_worker(
    job: DownloadJob,
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
    cleanup_files: bool,
) -> None:
    try:
        update_download_job(job, message="Collecting files", overall_text="Collecting files")
        raise_if_job_cancelled(job)
        files = collect_trade_files(client, selected_symbols, start, end)
        raise_if_job_cancelled(job)
        if not files:
            update_download_job(
                job,
                status="completed",
                message="No files found for the selected symbols and dates.",
                overall_fraction=1.0,
                overall_text="No files found",
            )
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
                cleanup_files=cleanup_files,
                job=job,
            )
        )
        update_download_job(
            job,
            status="completed",
            message="Download and ingest complete.",
            overall_fraction=1.0,
            overall_text="Complete",
        )
    except DownloadCancelled:
        update_download_job(
            job,
            status="cancelled",
            message="Download job cancelled.",
            download_text="Cancelled",
            ingest_text="Cancelled",
        )
    except Exception as error:
        logger.exception("Download and ingest job failed")
        update_download_job(job, status="failed", message=f"Job failed: {error}", error=str(error))


def current_download_job() -> DownloadJob | None:
    job = st.session_state.get(DOWNLOAD_JOB_KEY)
    return job if isinstance(job, DownloadJob) else None


def download_job_is_running(job: DownloadJob) -> bool:
    return job.status == "running" and job.thread is not None and job.thread.is_alive()


def update_download_job(job: DownloadJob, **changes: Any) -> None:
    with job.lock:
        for name, value in changes.items():
            setattr(job, name, value)


def append_download_job_log(job: DownloadJob, row: dict[str, Any]) -> None:
    with job.lock:
        job.log_rows.append(row)


def download_job_snapshot(job: DownloadJob) -> dict[str, Any]:
    with job.lock:
        return {
            "id": job.id,
            "status": job.status,
            "message": job.message,
            "overall_fraction": job.overall_fraction,
            "overall_text": job.overall_text,
            "download_fraction": job.download_fraction,
            "download_text": job.download_text,
            "ingest_text": job.ingest_text,
            "error": job.error,
            "log_rows": list(job.log_rows),
        }


@st.fragment(run_every="1s")
def render_download_job_status() -> None:
    job = current_download_job()
    if job is None:
        return

    snapshot = download_job_snapshot(job)
    status = str(snapshot["status"])
    message = str(snapshot["message"])
    running = download_job_is_running(job)

    status_columns = st.columns([3, 1])
    with status_columns[0]:
        if status == "failed":
            st.error(message)
        elif status == "cancelled":
            st.warning(message)
        elif status == "completed":
            st.success(message)
        else:
            st.info(message)
    with status_columns[1]:
        if running:
            if st.button("Cancel Downloading", key=f"cancel-download-{snapshot['id']}", width="stretch"):
                job.cancel_event.set()
                update_download_job(job, message="Cancelling after the current interruptible step...")
                st.rerun()
        elif st.button("Clear Job", key=f"clear-download-{snapshot['id']}", width="stretch"):
            del st.session_state[DOWNLOAD_JOB_KEY]
            st.rerun()

    st.progress(float(snapshot["overall_fraction"]), text=str(snapshot["overall_text"]))
    st.progress(float(snapshot["download_fraction"]), text=str(snapshot["download_text"]))
    if snapshot["ingest_text"]:
        st.info(str(snapshot["ingest_text"]))

    log_rows = cast(list[dict[str, Any]], snapshot["log_rows"])
    if log_rows:
        st.dataframe(log_rows, width="stretch", hide_index=True)


def raise_if_job_cancelled(job: DownloadJob) -> None:
    if job.cancel_event.is_set():
        raise DownloadCancelled("Download job cancelled")


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
    cleanup_files: bool,
    job: DownloadJob,
) -> None:
    engine = db_config.create_engine()
    try:
        raise_if_job_cancelled(job)
        await ensure_schema(engine, provider=client.slug, symbols=sorted({file.symbol for file in files}))
        raise_if_job_cancelled(job)
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
                cleanup_files=cleanup_files,
                job=job,
            )
            raise_if_job_cancelled(job)
            if compress_after:
                update_download_job(job, message="Compressing old chunks", ingest_text="Compressing old chunks")
                compressed = await compress_old_chunks(engine, provider=client.slug, older_than="30 days")
                update_download_job(
                    job,
                    ingest_text=(
                        "Compressed chunks: "
                        f"trades={compressed['trades']}, ohlcv={compressed['ohlcv']}"
                    ),
                )
            return

        for index, trade_file in enumerate(files, start=1):
            raise_if_job_cancelled(job)
            update_download_job(
                job,
                message=f"Processing {trade_file.filename}",
                overall_fraction=(index - 1) / total_files,
                overall_text=f"{index}/{total_files}: {trade_file.filename}",
                download_fraction=0.0,
                download_text=f"Waiting to download {trade_file.filename}",
            )

            def on_download(downloaded_bytes: int, total_bytes: int | None) -> None:
                raise_if_job_cancelled(job)
                if total_bytes:
                    update_download_job(
                        job,
                        download_fraction=min(downloaded_bytes / total_bytes, 1.0),
                        download_text=f"Downloading {trade_file.filename}: {downloaded_bytes / total_bytes:.0%}",
                    )
                else:
                    update_download_job(
                        job,
                        download_fraction=0.0,
                        download_text=f"Downloading {trade_file.filename}: {downloaded_bytes:,} bytes",
                    )

            path = await asyncio.to_thread(
                client.download_trade_file,
                trade_file,
                output_dir,
                overwrite=overwrite,
                progress_callback=on_download,
                cancel_callback=job.cancel_event.is_set,
            )
            update_download_job(job, download_fraction=1.0, download_text=f"Downloaded {trade_file.filename}")
            raise_if_job_cancelled(job)

            def on_insert(rows_read: int) -> None:
                raise_if_job_cancelled(job)
                update_download_job(job, ingest_text=f"Inserting {trade_file.filename}: {rows_read:,} rows staged")

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
            update_download_job(
                job,
                ingest_text=(
                    f"Inserted {import_result.rows_inserted:,}/{import_result.rows_read:,} raw rows; "
                    f"upserted {aggregate_result.rows_upserted:,} OHLCV rows"
                ),
            )
            local_file_deleted = cleanup_downloaded_file(path, output_dir) if cleanup_files else False

            append_download_job_log(
                job,
                {
                    "file": trade_file.filename,
                    "raw_rows": import_result.rows_read,
                    "inserted_rows": import_result.rows_inserted,
                    "ohlcv_rows": aggregate_result.rows_upserted,
                    "min_ts": import_result.min_ts,
                    "max_ts": import_result.max_ts,
                    "local_file_deleted": local_file_deleted,
                },
            )
            update_download_job(job, overall_fraction=index / total_files, overall_text=f"{index}/{total_files}: complete")

        raise_if_job_cancelled(job)
        if compress_after:
            update_download_job(job, message="Compressing old chunks", ingest_text="Compressing old chunks")
            compressed = await compress_old_chunks(engine, provider=client.slug, older_than="30 days")
            update_download_job(
                job,
                ingest_text=(
                    "Compressed chunks: "
                    f"trades={compressed['trades']}, ohlcv={compressed['ohlcv']}"
                ),
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
    cleanup_files: bool,
    job: DownloadJob,
) -> None:
    semaphore = asyncio.Semaphore(max_concurrent)
    completed = 0
    total_files = len(files)
    update_download_job(
        job,
        message=f"Running up to {max_concurrent} files at a time",
        overall_fraction=0.0,
        overall_text=f"Running up to {max_concurrent} files at a time",
        download_fraction=0.0,
        download_text="Waiting for completed files",
    )

    async def process_file(trade_file: MarketDataFile) -> dict[str, Any]:
        async with semaphore:
            raise_if_job_cancelled(job)
            path = await asyncio.to_thread(
                client.download_trade_file,
                trade_file,
                output_dir,
                overwrite=overwrite,
                cancel_callback=job.cancel_event.is_set,
            )
            raise_if_job_cancelled(job)
            import_result = await import_trade_csv(
                engine,
                path,
                provider=client.slug,
                symbol=trade_file.symbol,
                row_iterator=client.iter_trade_rows,
            )
            raise_if_job_cancelled(job)
            aggregate_result = await upsert_ohlcv_for_source(
                engine,
                import_result.source_file,
                provider=client.slug,
                symbol=import_result.symbol,
                timeframe=timeframe,
            )
            local_file_deleted = cleanup_downloaded_file(path, output_dir) if cleanup_files else False
            return {
                "file": trade_file.filename,
                "raw_rows": import_result.rows_read,
                "inserted_rows": import_result.rows_inserted,
                "ohlcv_rows": aggregate_result.rows_upserted,
                "min_ts": import_result.min_ts,
                "max_ts": import_result.max_ts,
                "local_file_deleted": local_file_deleted,
            }

    tasks = [asyncio.create_task(process_file(trade_file)) for trade_file in files]
    try:
        for task in asyncio.as_completed(tasks):
            raise_if_job_cancelled(job)
            row = await task
            completed += 1
            append_download_job_log(job, row)
            update_download_job(
                job,
                overall_fraction=completed / total_files,
                overall_text=f"{completed}/{total_files}: complete",
                download_fraction=completed / total_files,
                download_text=f"Completed {completed}/{total_files} files",
            )
    except DownloadCancelled:
        job.cancel_event.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except Exception:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    update_download_job(job, ingest_text=f"Completed {completed:,} files with max concurrency {max_concurrent}.")


def cleanup_downloaded_file(path: Path, output_dir: Path) -> bool:
    output_root = output_dir.resolve()
    target = path.resolve()
    try:
        target.relative_to(output_root)
    except ValueError:
        return False

    deleted = False
    with suppress(FileNotFoundError):
        target.unlink()
        deleted = True

    parent = target.parent
    while parent != output_root and output_root in parent.parents:
        with suppress(OSError):
            parent.rmdir()
        parent = parent.parent

    return deleted


def ohlcv_panel(db_config: DatabaseConfig, provider_slug: str, selected_symbols: list[str]) -> None:
    st.subheader("OHLCV")
    if not selected_symbols:
        st.info("Select one or more tickers to view OHLCV.")
        return

    controls = st.columns([2, 1, 1, 1])
    with controls[0]:
        symbol = st.selectbox("Symbol", options=["", *selected_symbols], format_func=lambda value: value or "All selected")
    with controls[1]:
        timeframe = st.selectbox("Chart timeframe", options=TIMEFRAMES)
    with controls[2]:
        y_axis = st.segmented_control("Y axis", options=("Linear", "Log"), default="Linear")
    with controls[3]:
        row_limit = st.number_input("Rows", min_value=100, max_value=50_000, value=5_000, step=100)

    try:
        rows = run_async(
            fetch_latest_ohlcv(
                db_config,
                provider_slug=provider_slug,
                symbol=symbol or None,
                symbols=selected_symbols,
                timeframe=timeframe,
                limit=int(row_limit),
            )
        )
        render_ohlcv_chart(rows, y_axis=str(y_axis or "Linear"))
        st.dataframe(rows, width="stretch", hide_index=True)
    except SQLAlchemyError as error:
        st.info(f"OHLCV unavailable: {error}")


def render_ohlcv_chart(rows: list[dict[str, object]], *, y_axis: str) -> None:
    if not rows:
        st.info("No OHLCV rows found for the selected tickers and timeframe.")
        return

    chart_rows = [normalize_ohlcv_chart_row(row) for row in rows]
    chart_rows.sort(key=lambda row: (row["bucket"], row["symbol"]))
    if y_axis == "Log":
        chart_rows = [row for row in chart_rows if row["close"] > 0]
        if not chart_rows:
            st.info("Log scale requires positive close prices.")
            return

    y_scale_type: Literal["linear", "log"] = "log" if y_axis == "Log" else "linear"
    chart = (
        alt.Chart(alt.Data(values=chart_rows))
        .mark_line()
        .encode(
            x=alt.X("bucket:T", title="Time"),
            y=alt.Y("close:Q", title="Close", scale=alt.Scale(type=y_scale_type, zero=False)),
            color=alt.Color("symbol:N", title="Symbol"),
            tooltip=[
                alt.Tooltip("bucket:T", title="Time"),
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("timeframe:N", title="Timeframe"),
                alt.Tooltip("open:Q", title="Open", format=",.6f"),
                alt.Tooltip("high:Q", title="High", format=",.6f"),
                alt.Tooltip("low:Q", title="Low", format=",.6f"),
                alt.Tooltip("close:Q", title="Close", format=",.6f"),
                alt.Tooltip("volume:Q", title="Volume", format=",.4f"),
                alt.Tooltip("trade_count:Q", title="Trades", format=","),
            ],
        )
        .properties(height=420)
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def normalize_ohlcv_chart_row(row: dict[str, object]) -> OhlcvChartRow:
    bucket = row["bucket"]
    return {
        "bucket": bucket.isoformat() if isinstance(bucket, datetime) else str(bucket),
        "symbol": str(row["symbol"]),
        "timeframe": str(row["timeframe"]),
        "open": float(cast(str | int | float, row["open"])),
        "high": float(cast(str | int | float, row["high"])),
        "low": float(cast(str | int | float, row["low"])),
        "close": float(cast(str | int | float, row["close"])),
        "volume": float(cast(str | int | float, row["volume"])),
        "trade_count": int(cast(str | int | float, row["trade_count"])),
    }


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
    timeframe: str,
    limit: int,
) -> list[dict[str, object]]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine, provider=provider_slug)
        rows = await latest_ohlcv(
            engine,
            provider=provider_slug,
            symbol=symbol,
            symbols=symbols,
            timeframe=timeframe,
            limit=limit,
        )
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
