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
from typing import Any, Literal, cast
from uuid import uuid4

import streamlit as st
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from trading.charts import build_price_figure, normalize_ohlcv_chart_row, normalize_trade_marker_chart_row
from trading.db import (
    BASE_OHLCV_TIMEFRAME,
    compress_old_chunks,
    ensure_schema,
    import_trade_csv_and_upsert_ohlcv,
    latest_ohlcv,
    latest_trade_markers,
    source_file_status,
    table_counts,
    upsert_ohlcv_for_source,
)
from trading.models import DatabaseConfig
from trading.providers import (
    HistoricalTradeProvider,
    MarketDataFile,
    ProviderOption,
    create_provider,
    provider_display_name,
    provider_names,
    provider_option_specs,
)
from trading.providers.base import DownloadCancelled

TIMEFRAMES = ("1 minute", "5 minutes", "15 minutes", "1 hour", "6 hours", "12 hours", "1 day")
DEFAULT_CHART_TIMEFRAME = "1 hour"
DOWNLOAD_JOB_KEY = "download_job"
DEFAULT_CHART_ROWS = 5_000
CHART_LOAD_STEP = 5_000
MAX_CHART_ROWS = 100_000
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
    file_rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)
    thread: threading.Thread | None = None


@dataclass(frozen=True, slots=True)
class IngestWorkItem:
    trade_file: MarketDataFile
    action: Literal["aggregate", "ingest_local", "download"]
    local_path: Path | None = None


def main() -> None:
    st.set_page_config(page_title="Trading Data Loader", layout="wide")
    st.title("Trading Data Loader")

    db_config = sidebar_database_config()
    provider_slug, provider_options = sidebar_provider_config()
    client = create_provider(provider_slug, **provider_options)

    tickers_tab, download_tab, charts_tab = st.tabs(["Tickers", "Download And Ingest", "Charts"])

    with tickers_tab:
        show_database_status(db_config, client.slug)
        st.divider()
        selected_symbols, start, end = ticker_explorer(client)

    with download_tab:
        download_panel(client, db_config, selected_symbols, start, end)

    with charts_tab:
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
    selected_provider = st.sidebar.selectbox(
        "Provider",
        options=provider_names(),
        format_func=provider_display_name,
    )
    provider_slug = str(selected_provider)

    provider_options = {
        option.name: render_provider_option(option)
        for option in provider_option_specs(provider_slug)
    }

    return provider_slug, provider_options


def render_provider_option(option: ProviderOption) -> object:
    if option.value_type == "bool":
        return st.sidebar.checkbox(option.label, value=bool(option.default), help=option.help)
    if option.value_type == "int":
        return int(
            st.sidebar.number_input(
                option.label,
                value=int(option.default),
                min_value=int(option.min_value) if option.min_value is not None else None,
                max_value=int(option.max_value) if option.max_value is not None else None,
                step=int(option.step) if option.step is not None else 1,
                help=option.help,
            )
        )
    if option.value_type == "float":
        return float(
            st.sidebar.number_input(
                option.label,
                value=float(option.default),
                min_value=float(option.min_value) if option.min_value is not None else None,
                max_value=float(option.max_value) if option.max_value is not None else None,
                step=float(option.step) if option.step is not None else 1.0,
                help=option.help,
            )
        )
    return st.sidebar.text_input(option.label, value=str(option.default), help=option.help)


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
    counts_key = f"table-counts:{provider_slug}"
    if st.button("Refresh Counts", width="stretch"):
        try:
            st.session_state[counts_key] = run_async(fetch_table_counts(db_config, provider_slug))
        except SQLAlchemyError:
            st.info("Database not connected.")
            return

    counts = st.session_state.get(counts_key)
    if isinstance(counts, dict):
        st.metric("Raw trades", f"{int(counts['trades']):,}")
        st.metric("OHLCV rows", f"{int(counts['ohlcv']):,}")
    else:
        st.caption("Counts are calculated on demand.")


def ticker_explorer(client: HistoricalTradeProvider) -> tuple[list[str], date, date]:
    st.subheader("Tickers")
    controls = st.columns([3, 1, 1])
    provider_options = provider_cache_options(client)
    with controls[1]:
        start = st.date_input("Start", value=date(2020, 1, 1))
    with controls[2]:
        end = st.date_input("End", value=date.today())

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
    active_job = current_download_job()
    if active_job is not None:
        render_download_job_status()
        st.divider()

    options = st.columns([2, 1, 1, 1, 1])
    with options[0]:
        output_dir = Path(st.text_input("Output directory", value=client.default_output_dir))
    with options[1]:
        max_concurrent_downloads = st.number_input("Max downloads", min_value=1, max_value=16, value=8, step=1)
    with options[2]:
        max_concurrent_db = st.number_input("Max DB files", min_value=1, max_value=8, value=2, step=1)
    with options[3]:
        overwrite = st.checkbox("Overwrite files", value=False)
    with options[4]:
        compress_after = st.checkbox("Compress old chunks", value=False)
    cleanup_files = st.checkbox("Delete local files after ingest", value=True)

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
            max_concurrent_downloads=max_concurrent_downloads,
            max_concurrent_db=max_concurrent_db,
            overwrite=overwrite,
            compress_after=compress_after,
            cleanup_files=cleanup_files,
        )


def start_download_job(
    client: HistoricalTradeProvider,
    db_config: DatabaseConfig,
    selected_symbols: list[str],
    start: date,
    end: date,
    output_dir: Path,
    *,
    max_concurrent_downloads: int,
    max_concurrent_db: int,
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
        ),
        kwargs={
            "max_concurrent_downloads": max_concurrent_downloads,
            "max_concurrent_db": max_concurrent_db,
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
    *,
    max_concurrent_downloads: int,
    max_concurrent_db: int,
    overwrite: bool,
    compress_after: bool,
    cleanup_files: bool,
) -> None:
    try:
        logger.info(
            "Download and ingest job started",
            extra={
                "event": "download_ingest.started",
                "job_id": job.id,
                "provider": client.slug,
                "symbols": selected_symbols,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "output_dir": str(output_dir),
                "max_concurrent_downloads": max_concurrent_downloads,
                "max_concurrent_db": max_concurrent_db,
                "overwrite": overwrite,
                "cleanup_files": cleanup_files,
            },
        )
        update_download_job(job, message="Collecting files", overall_text="Collecting files")
        raise_if_job_cancelled(job)
        files = run_async(collect_trade_files(client, selected_symbols, start, end))
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
        initialize_download_file_rows(job, files)
        update_download_job(job, message=f"Found {len(files):,} files", overall_text=f"Found {len(files):,} files")

        run_async(
            download_and_ingest_files(
                client,
                db_config,
                files,
                output_dir,
                max_concurrent_downloads=max_concurrent_downloads,
                max_concurrent_db=max_concurrent_db,
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
        logger.info(
            "Download and ingest job completed",
            extra={"event": "download_ingest.completed", "job_id": job.id, "provider": client.slug},
        )
    except DownloadCancelled:
        mark_unfinished_download_files(job, "cancelled")
        update_download_job(
            job,
            status="cancelled",
            message="Download job cancelled.",
            download_text="Cancelled",
            ingest_text="Cancelled",
        )
        logger.info(
            "Download and ingest job cancelled",
            extra={"event": "download_ingest.cancelled", "job_id": job.id, "provider": client.slug},
        )
    except Exception as error:
        logger.exception(
            "Download and ingest job failed",
            extra={
                "event": "download_ingest.failed",
                "job_id": job.id,
                "provider": client.slug,
                "symbols": selected_symbols,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "output_dir": str(output_dir),
            },
        )
        mark_unfinished_download_files(job, "failed")
        update_download_job(job, status="failed", message=f"Job failed: {error}", error=str(error))


def current_download_job() -> DownloadJob | None:
    job = st.session_state.get(DOWNLOAD_JOB_KEY)
    required_attributes = (
        "id",
        "status",
        "message",
        "overall_fraction",
        "overall_text",
        "download_fraction",
        "download_text",
        "ingest_text",
        "error",
        "thread",
        "lock",
        "cancel_event",
        "log_rows",
        "file_rows",
    )
    if all(hasattr(job, attribute) for attribute in required_attributes):
        return cast(DownloadJob, job)
    return None


def download_job_is_running(job: DownloadJob) -> bool:
    return job.status == "running" and job.thread is not None and job.thread.is_alive()


def update_download_job(job: DownloadJob, **changes: Any) -> None:
    with job.lock:
        for name, value in changes.items():
            setattr(job, name, value)


def append_download_job_log(job: DownloadJob, row: dict[str, Any]) -> None:
    with job.lock:
        job.log_rows.append(row)


def initialize_download_file_rows(job: DownloadJob, files: list[MarketDataFile]) -> None:
    with job.lock:
        job.file_rows = {
            trade_file.filename: download_file_row(trade_file, index=index, status="queued")
            for index, trade_file in enumerate(files, start=1)
        }


def update_download_file_row(
    job: DownloadJob,
    trade_file: MarketDataFile,
    *,
    status: str,
    action: str | None = None,
    downloaded_bytes: int | None = None,
    total_bytes: int | None = None,
    download_percent: float | None = None,
    raw_rows: int | None = None,
    inserted_rows: int | None = None,
    ohlcv_rows: int | None = None,
) -> None:
    with job.lock:
        row = job.file_rows.setdefault(trade_file.filename, download_file_row(trade_file, index=len(job.file_rows) + 1, status=status))
        row["status"] = status
        if status == "completed" and not row.get("completed_at"):
            row["completed_at"] = completion_timestamp()
        if action is not None:
            row["action"] = action
        if downloaded_bytes is not None:
            row["downloaded_bytes"] = downloaded_bytes
        if total_bytes is not None:
            row["total_bytes"] = total_bytes
        if download_percent is not None:
            row["download_percent"] = download_percent
        elif total_bytes:
            row["download_percent"] = round((downloaded_bytes or 0) / total_bytes * 100, 1)
        if raw_rows is not None:
            row["raw_rows"] = raw_rows
        if inserted_rows is not None:
            row["inserted_rows"] = inserted_rows
        if ohlcv_rows is not None:
            row["ohlcv_rows"] = ohlcv_rows


def mark_unfinished_download_files(job: DownloadJob, status: str) -> None:
    finished = {"completed", "failed", "cancelled"}
    with job.lock:
        for row in job.file_rows.values():
            if row["status"] not in finished:
                row["status"] = status


def download_file_row(trade_file: MarketDataFile, *, index: int, status: str) -> dict[str, Any]:
    return {
        "order": index,
        "symbol": trade_file.symbol,
        "date": trade_file.trade_date,
        "file": trade_file.filename,
        "action": "",
        "status": status,
        "download_percent": 0.0,
        "downloaded_bytes": 0,
        "total_bytes": None,
        "raw_rows": None,
        "inserted_rows": None,
        "ohlcv_rows": None,
        "completed_at": "",
    }


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
            "file_rows": sorted((dict(row) for row in job.file_rows.values()), key=lambda row: int(row["order"])),
        }


@st.fragment(run_every=0.5)
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

    file_rows = cast(list[dict[str, Any]], snapshot["file_rows"])
    if file_rows:
        render_download_job_row_totals(file_rows)
        completed_statuses = {"completed", "skipped"}
        active_file_rows = [row for row in file_rows if row["status"] not in completed_statuses]
        completed_file_rows = [row for row in file_rows if row["status"] in completed_statuses]
        if active_file_rows:
            st.caption("Active Files")
            render_download_file_rows(active_file_rows)
        if completed_file_rows:
            st.caption("Completed Files")
            render_download_file_rows(completed_file_rows)

    log_rows = cast(list[dict[str, Any]], snapshot["log_rows"])
    if log_rows:
        st.dataframe(log_rows, width="stretch", hide_index=True)


def render_download_job_row_totals(rows: list[dict[str, Any]]) -> None:
    raw_rows = sum_int_values(rows, "raw_rows")
    inserted_rows = sum_int_values(rows, "inserted_rows")
    ohlcv_rows = sum_int_values(rows, "ohlcv_rows")
    if raw_rows == 0 and inserted_rows == 0 and ohlcv_rows == 0:
        return

    columns = st.columns(3)
    columns[0].metric("Trades read", f"{raw_rows:,}")
    columns[1].metric("Trades inserted", f"{inserted_rows:,}")
    columns[2].metric("OHLCV rows", f"{ohlcv_rows:,}")


def sum_int_values(rows: list[dict[str, Any]], key: str) -> int:
    total = 0
    for row in rows:
        value = row.get(key)
        if isinstance(value, int):
            total += value
    return total


def render_download_file_rows(rows: list[dict[str, Any]]) -> None:
    st.dataframe(
        rows,
        width="stretch",
        hide_index=True,
        column_order=(
            "symbol",
            "date",
            "file",
            "status",
            "action",
            "download_percent",
            "downloaded_bytes",
            "total_bytes",
            "raw_rows",
            "inserted_rows",
            "ohlcv_rows",
            "completed_at",
        ),
        column_config={
            "download_percent": st.column_config.ProgressColumn(
                "Download",
                format="%.1f%%",
                min_value=0.0,
                max_value=100.0,
            ),
            "downloaded_bytes": st.column_config.NumberColumn("Downloaded bytes", format="%d"),
            "total_bytes": st.column_config.NumberColumn("Total bytes", format="%d"),
            "raw_rows": st.column_config.NumberColumn("Trades read", format="%d"),
            "inserted_rows": st.column_config.NumberColumn("Trades inserted", format="%d"),
            "ohlcv_rows": st.column_config.NumberColumn("OHLCV rows", format="%d"),
            "completed_at": st.column_config.TextColumn("Completed at"),
        },
    )


def raise_if_job_cancelled(job: DownloadJob) -> None:
    if job.cancel_event.is_set():
        raise DownloadCancelled("Download job cancelled")


def completion_timestamp() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


async def collect_trade_files(
    client: HistoricalTradeProvider,
    selected_symbols: list[str],
    start: date,
    end: date,
) -> list[MarketDataFile]:
    files: list[MarketDataFile] = []
    for symbol in selected_symbols:
        files.extend(await client.list_trade_files(symbol, start_date=start, end_date=end))
    return files


async def download_and_ingest_files(
    client: HistoricalTradeProvider,
    db_config: DatabaseConfig,
    files: list[MarketDataFile],
    output_dir: Path,
    *,
    max_concurrent_downloads: int,
    max_concurrent_db: int,
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
        plan = await plan_ingestion_work(
            engine,
            client,
            files,
            output_dir,
            max_concurrent=max_concurrent_db,
            job=job,
        )
        raise_if_job_cancelled(job)
        if not plan:
            update_download_job(
                job,
                message=f"All selected files already have raw trades and {BASE_OHLCV_TIMEFRAME} OHLCV.",
                overall_fraction=1.0,
                overall_text="Nothing to ingest",
                download_fraction=1.0,
                download_text="Nothing to download",
            )
            return
        await download_and_ingest_concurrently(
            client,
            engine,
            plan,
            output_dir,
            max_concurrent_downloads=max_concurrent_downloads,
            max_concurrent_db=max_concurrent_db,
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
    finally:
        await engine.dispose()


async def plan_ingestion_work(
    engine: AsyncEngine,
    client: HistoricalTradeProvider,
    files: list[MarketDataFile],
    output_dir: Path,
    *,
    max_concurrent: int,
    job: DownloadJob,
) -> list[IngestWorkItem]:
    plan: list[IngestWorkItem] = []
    counts = {"aggregate": 0, "ingest_local": 0, "download": 0, "skipped": 0}
    semaphore = asyncio.Semaphore(max_concurrent)
    completed = 0
    total_files = len(files)
    update_download_job(
        job,
        message="Checking existing raw trades and OHLCV",
        overall_text=f"Checking existing data 0/{total_files}",
        download_text="Planning work",
    )

    async def classify_file(trade_file: MarketDataFile) -> tuple[IngestWorkItem | None, str]:
        async with semaphore:
            raise_if_job_cancelled(job)
            source_file = trade_file.csv_filename
            status = await source_file_status(
                engine,
                provider=client.slug,
                symbol=trade_file.symbol,
                source_file=source_file,
                trade_date=trade_file.trade_date,
            )
            local_path = output_dir / trade_file.symbol / source_file

            if status["trades"] and status["ohlcv"]:
                update_download_file_row(job, trade_file, status="skipped", action="already complete", download_percent=100.0)
                return None, "skipped"
            if status["trades"]:
                update_download_file_row(job, trade_file, status="queued", action=f"aggregate {BASE_OHLCV_TIMEFRAME} candles")
                return IngestWorkItem(trade_file=trade_file, action="aggregate"), "aggregate"
            if local_path.exists():
                update_download_file_row(job, trade_file, status="queued", action="ingest local csv")
                return IngestWorkItem(trade_file=trade_file, action="ingest_local", local_path=local_path), "ingest_local"
            update_download_file_row(job, trade_file, status="queued", action="download raw trades")
            return IngestWorkItem(trade_file=trade_file, action="download"), "download"

    tasks = [asyncio.create_task(classify_file(trade_file)) for trade_file in files]
    try:
        for task in asyncio.as_completed(tasks):
            raise_if_job_cancelled(job)
            work_item, count_key = await task
            counts[count_key] += 1
            if work_item is not None:
                plan.append(work_item)
            completed += 1
            update_download_job(
                job,
                overall_fraction=completed / total_files,
                overall_text=f"Checking existing data {completed}/{total_files}",
                download_text=(
                    f"Planning: aggregate={counts['aggregate']}, local ingest={counts['ingest_local']}, "
                    f"download={counts['download']}, skipped={counts['skipped']}"
                ),
            )
    except DownloadCancelled:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except Exception:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    priority = {"aggregate": 0, "ingest_local": 1, "download": 2}
    plan.sort(
        key=lambda item: (
            priority[item.action],
            item.trade_file.trade_date,
            item.trade_file.symbol,
            item.trade_file.filename,
        )
    )
    update_download_job(
        job,
        message=(
            f"Planned work: aggregate={counts['aggregate']}, local ingest={counts['ingest_local']}, "
            f"download={counts['download']}, skipped={counts['skipped']}"
        ),
        overall_text=f"Planned {len(plan):,} work items",
    )
    return plan


async def aggregate_existing_source(
    engine: AsyncEngine,
    client: HistoricalTradeProvider,
    trade_file: MarketDataFile,
    job: DownloadJob,
) -> dict[str, Any]:
    update_download_file_row(job, trade_file, status="aggregating", action=f"aggregate {BASE_OHLCV_TIMEFRAME} candles", download_percent=100.0)
    aggregate_result = await upsert_ohlcv_for_source(
        engine,
        trade_file.csv_filename,
        provider=client.slug,
        symbol=trade_file.symbol,
        trade_date=trade_file.trade_date,
        ensure_symbol_schema=False,
    )
    update_download_file_row(
        job,
        trade_file,
        status="completed",
        download_percent=100.0,
        ohlcv_rows=aggregate_result.rows_upserted,
    )
    return {
        "file": trade_file.csv_filename,
        "action": "aggregate",
        "raw_rows": None,
        "inserted_rows": None,
        "ohlcv_rows": aggregate_result.rows_upserted,
        "min_ts": None,
        "max_ts": None,
        "local_file_deleted": False,
    }


async def ingest_local_source(
    engine: AsyncEngine,
    client: HistoricalTradeProvider,
    trade_file: MarketDataFile,
    path: Path,
    output_dir: Path,
    cleanup_files: bool,
    job: DownloadJob,
) -> dict[str, Any]:
    update_download_file_row(job, trade_file, status="ingesting", action="ingest local csv", download_percent=100.0)
    return await import_and_aggregate_source(
        engine,
        client,
        trade_file,
        path,
        output_dir,
        cleanup_files,
        job,
        action="ingest_local",
    )


async def import_and_aggregate_source(
    engine: AsyncEngine,
    client: HistoricalTradeProvider,
    trade_file: MarketDataFile,
    path: Path,
    output_dir: Path,
    cleanup_files: bool,
    job: DownloadJob,
    action: str,
) -> dict[str, Any]:
    def on_insert(rows_read: int) -> None:
        raise_if_job_cancelled(job)
        update_download_job(job, ingest_text=f"Inserting {trade_file.filename}: {rows_read:,} rows staged")

    def on_import_phase(phase: str) -> None:
        raise_if_job_cancelled(job)
        update_download_job(job, ingest_text=f"{trade_file.filename}: {phase}")

    import_result, aggregate_result = await import_trade_csv_and_upsert_ohlcv(
        engine,
        path,
        provider=client.slug,
        symbol=trade_file.symbol,
        row_iterator=client.iter_trade_rows,
        progress_callback=on_insert,
        phase_callback=on_import_phase,
        ensure_symbol_schema=False,
    )
    raise_if_job_cancelled(job)
    local_file_deleted = cleanup_downloaded_file(path, output_dir) if cleanup_files else False
    update_download_file_row(
        job,
        trade_file,
        status="completed",
        download_percent=100.0,
        raw_rows=import_result.rows_read,
        inserted_rows=import_result.rows_inserted,
        ohlcv_rows=aggregate_result.rows_upserted,
    )
    return {
        "file": trade_file.filename,
        "action": action,
        "raw_rows": import_result.rows_read,
        "inserted_rows": import_result.rows_inserted,
        "ohlcv_rows": aggregate_result.rows_upserted,
        "min_ts": import_result.min_ts,
        "max_ts": import_result.max_ts,
        "local_file_deleted": local_file_deleted,
    }


async def download_and_ingest_concurrently(
    client: HistoricalTradeProvider,
    engine: AsyncEngine,
    work_items: list[IngestWorkItem],
    output_dir: Path,
    *,
    max_concurrent_downloads: int,
    max_concurrent_db: int,
    overwrite: bool,
    cleanup_files: bool,
    job: DownloadJob,
) -> None:
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    db_semaphore = asyncio.Semaphore(max_concurrent_db)
    completed = 0
    total_files = len(work_items)
    update_download_job(
        job,
        message=f"Running up to {max_concurrent_downloads} downloads and {max_concurrent_db} DB files",
        overall_fraction=0.0,
        overall_text=f"Running up to {max_concurrent_downloads} downloads and {max_concurrent_db} DB files",
        download_fraction=0.0,
        download_text="Waiting for completed files",
    )

    async def process_file(work_item: IngestWorkItem) -> dict[str, Any]:
        trade_file = work_item.trade_file
        try:
            raise_if_job_cancelled(job)
            if work_item.action == "aggregate":
                async with db_semaphore:
                    return await aggregate_existing_source(engine, client, trade_file, job)
            if work_item.action == "ingest_local":
                if work_item.local_path is None:
                    raise ValueError(f"Missing local path for {trade_file.filename}")
                async with db_semaphore:
                    return await ingest_local_source(
                        engine,
                        client,
                        trade_file,
                        work_item.local_path,
                        output_dir,
                        cleanup_files,
                        job,
                    )

            async with download_semaphore:
                update_download_file_row(job, trade_file, status="downloading", action="download raw trades")

                def on_download(downloaded_bytes: int, total_bytes: int | None) -> None:
                    raise_if_job_cancelled(job)
                    update_download_file_row(
                        job,
                        trade_file,
                        status="downloading",
                        downloaded_bytes=downloaded_bytes,
                        total_bytes=total_bytes,
                    )

                path = await client.download_trade_file(
                    trade_file,
                    output_dir,
                    overwrite=overwrite,
                    progress_callback=on_download,
                    cancel_callback=job.cancel_event.is_set,
                )
                update_download_file_row(job, trade_file, status="downloaded", download_percent=100.0)
            raise_if_job_cancelled(job)
            update_download_file_row(job, trade_file, status="ingesting", action="import downloaded csv")
            async with db_semaphore:
                return await import_and_aggregate_source(
                    engine,
                    client,
                    trade_file,
                    path,
                    output_dir,
                    cleanup_files,
                    job,
                    action="download",
                )
        except DownloadCancelled:
            update_download_file_row(job, trade_file, status="cancelled")
            raise
        except Exception:
            update_download_file_row(job, trade_file, status="failed")
            raise

    for action, label in (
        ("aggregate", f"Aggregating missing {BASE_OHLCV_TIMEFRAME} candles"),
        ("ingest_local", "Ingesting local CSVs"),
        ("download", "Downloading missing raw files"),
    ):
        phase_items = [work_item for work_item in work_items if work_item.action == action]
        if not phase_items:
            continue
        update_download_job(job, message=label, download_text=label)
        tasks = [asyncio.create_task(process_file(work_item)) for work_item in phase_items]
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
                    download_text=f"Completed {completed}/{total_files} work items",
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

    update_download_job(
        job,
        ingest_text=(
            f"Completed {completed:,} files with "
            f"{max_concurrent_downloads} download slots and {max_concurrent_db} DB slots."
        ),
    )


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

    return deleted


def ohlcv_panel(db_config: DatabaseConfig, provider_slug: str, selected_symbols: list[str]) -> None:
    st.subheader("OHLCV")
    if not selected_symbols:
        st.info("Select one or more tickers to view OHLCV.")
        return

    controls = st.columns([2, 1, 1, 1, 1])
    with controls[0]:
        symbol = st.selectbox("Symbol", options=["", *selected_symbols], format_func=lambda value: value or "All selected")
    with controls[1]:
        timeframe = st.selectbox(
            "Chart timeframe",
            options=TIMEFRAMES,
            index=TIMEFRAMES.index(DEFAULT_CHART_TIMEFRAME),
        )
    with controls[2]:
        y_axis = st.segmented_control("Y axis", options=("Linear", "Log"), default="Linear")
    with controls[3]:
        layout_mode = st.segmented_control("Layout", options=("Overlay", "Stacked"), default="Overlay")
    with controls[4]:
        show_trades = st.checkbox("Trades", value=True)

    limit_key = ohlcv_limit_key(provider_slug, symbol or None, selected_symbols, timeframe)
    row_limit = ohlcv_row_limit(limit_key)
    hydration_controls = st.columns([1, 1, 3])
    with hydration_controls[0]:
        if st.button("Load Older", width="stretch"):
            st.session_state[limit_key] = min(row_limit + CHART_LOAD_STEP, MAX_CHART_ROWS)
            st.rerun()
    with hydration_controls[1]:
        if st.button("Refresh Latest", width="stretch"):
            st.session_state[limit_key] = DEFAULT_CHART_ROWS
            st.rerun()
    with hydration_controls[2]:
        st.caption(f"Loaded up to {row_limit:,} candles per selected ticker.")

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
        trade_rows: list[dict[str, object]] = []
        if show_trades:
            trade_rows = run_async(
                fetch_trade_markers(
                    db_config,
                    provider_slug=provider_slug,
                    symbol=symbol or None,
                    symbols=selected_symbols,
                    limit=1_000,
                )
            )
        render_ohlcv_chart(
            rows,
            trade_rows=trade_rows,
            y_axis=str(y_axis or "Linear"),
            layout_mode=str(layout_mode or "Overlay"),
        )
        st.dataframe(rows, width="stretch", hide_index=True)
    except SQLAlchemyError as error:
        st.info(f"OHLCV unavailable: {error}")


def ohlcv_limit_key(provider_slug: str, symbol: str | None, selected_symbols: list[str], timeframe: str) -> str:
    symbol_scope = symbol or f"all:{'|'.join(sorted(selected_symbols))}"
    return f"ohlcv-row-limit:{provider_slug}:{symbol_scope}:{timeframe}"


def ohlcv_row_limit(limit_key: str) -> int:
    value = st.session_state.get(limit_key, DEFAULT_CHART_ROWS)
    if not isinstance(value, int):
        return DEFAULT_CHART_ROWS
    return min(max(value, 100), MAX_CHART_ROWS)


def render_ohlcv_chart(
    rows: list[dict[str, object]],
    *,
    trade_rows: list[dict[str, object]],
    y_axis: str,
    layout_mode: str,
) -> None:
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

    trade_chart_rows = [normalize_trade_marker_chart_row(row) for row in trade_rows]
    figure = build_price_figure(
        chart_rows,
        trade_chart_rows,
        y_axis=y_axis,
        layout_mode=layout_mode,
    )
    st.plotly_chart(
        figure,
        width="stretch",
        config={
            "scrollZoom": True,
            "displaylogo": False,
            "displayModeBar": "hover",
            "doubleClick": "reset",
        },
    )


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


async def fetch_trade_markers(
    db_config: DatabaseConfig,
    *,
    provider_slug: str,
    symbol: str | None,
    symbols: list[str],
    limit: int,
) -> list[dict[str, object]]:
    engine = db_config.create_engine()
    try:
        await ensure_schema(engine, provider=provider_slug)
        rows = await latest_trade_markers(
            engine,
            provider=provider_slug,
            symbol=symbol,
            symbols=symbols,
            limit=limit,
        )
        return [row.model_dump() for row in rows]
    finally:
        await engine.dispose()


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coro)


def provider_cache_options(client: HistoricalTradeProvider) -> tuple[tuple[str, object], ...]:
    return client.cache_options()


def provider_kwargs(options: tuple[tuple[str, object], ...]) -> dict[str, object]:
    return dict(options)


def default_symbols(symbols: list[str]) -> list[str]:
    preferred_symbols = ("BTCUSD", "SOLUSD", "XRPUSD", "ETHUSD")
    defaults = [symbol for symbol in preferred_symbols if symbol in symbols]
    return defaults or symbols[:1]


@st.cache_data(ttl=3600)
def cached_symbols(provider_slug: str, options: tuple[tuple[str, object], ...]) -> list[str]:
    return run_async(create_provider(provider_slug, **provider_kwargs(options)).list_symbols())


@st.cache_data(ttl=300)
def cached_trade_files(
    provider_slug: str,
    options: tuple[tuple[str, object], ...],
    symbol: str,
    start: str,
    end: str,
) -> list[MarketDataFile]:
    return run_async(
        create_provider(provider_slug, **provider_kwargs(options)).list_trade_files(
            symbol,
            start_date=start,
            end_date=end,
        )
    )


if __name__ == "__main__":
    os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
    main()
