from __future__ import annotations

import argparse
import logging
from pathlib import Path

from trading.bybit import BybitPublicDataClient


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.verbose)
    args.func(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trading", description="Trading data utilities.")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase log verbosity.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    symbols_parser = subparsers.add_parser("symbols", help="List Bybit public-data symbols.")
    add_client_args(symbols_parser)
    symbols_parser.add_argument("--limit", type=int, help="Only print the first N symbols.")
    symbols_parser.set_defaults(func=run_symbols)

    files_parser = subparsers.add_parser("files", help="List Bybit CSV trade files for a symbol.")
    add_client_args(files_parser)
    add_date_args(files_parser)
    files_parser.add_argument("symbol", help="Symbol to inspect, for example BTCUSDT.")
    files_parser.add_argument("--limit", type=int, help="Only print the first N files.")
    files_parser.set_defaults(func=run_files)

    download_parser = subparsers.add_parser("download", help="Download Bybit CSV trade files.")
    add_client_args(download_parser)
    add_date_args(download_parser)
    download_parser.add_argument("symbols", nargs="+", help="Symbols to download, for example BTCUSDT ETHUSDT.")
    download_parser.add_argument("-o", "--output-dir", type=Path, default=Path("data/bybit"), help="Download target.")
    download_parser.add_argument("--limit", type=int, help="Download at most N files per symbol.")
    download_parser.add_argument("--no-extract", action="store_true", help="Keep files as .csv.gz archives.")
    download_parser.add_argument("--keep-archive", action="store_true", help="Keep .csv.gz files after extracting.")
    download_parser.add_argument("--overwrite", action="store_true", help="Replace existing local files.")
    download_parser.add_argument("--sleep", type=float, default=0, help="Seconds to sleep between downloads.")
    download_parser.set_defaults(func=run_download)

    return parser


def add_client_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default=None, help="Override the Bybit public-data base URL.")
    parser.add_argument("--timeout", type=float, default=30, help="HTTP timeout in seconds.")


def add_date_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", help="Start date, inclusive, as YYYY-MM-DD.")
    parser.add_argument("--end-date", help="End date, inclusive, as YYYY-MM-DD.")


def run_symbols(args: argparse.Namespace) -> None:
    client = make_client(args)
    symbols = client.list_symbols()
    for symbol in _limited(symbols, args.limit):
        print(symbol)


def run_files(args: argparse.Namespace) -> None:
    client = make_client(args)
    files = client.list_trade_files(args.symbol, start_date=args.start_date, end_date=args.end_date)
    for trade_file in _limited(files, args.limit):
        print(f"{trade_file.trade_date} {trade_file.filename} {trade_file.url}")


def run_download(args: argparse.Namespace) -> None:
    client = make_client(args)
    paths = client.download_trades(
        args.symbols,
        args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        extract=not args.no_extract,
        overwrite=args.overwrite,
        keep_archive=args.keep_archive,
        limit=args.limit,
        sleep_seconds=args.sleep,
    )
    for path in paths:
        print(path)


def make_client(args: argparse.Namespace) -> BybitPublicDataClient:
    if args.base_url:
        return BybitPublicDataClient(base_url=args.base_url, timeout=args.timeout)
    return BybitPublicDataClient(timeout=args.timeout)


def configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


def _limited[T](items: list[T], limit: int | None) -> list[T]:
    if limit is None:
        return items
    return items[:limit]


if __name__ == "__main__":
    main()
