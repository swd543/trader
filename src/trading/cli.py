from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from trading.logging_config import configure_logging, level_from_verbosity
from trading.providers import (
    HistoricalTradeProvider,
    MarketDataFile,
    ProviderOption,
    create_provider,
    provider_default_output_dir,
    provider_names,
    provider_option_specs,
)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(level=level_from_verbosity(args.verbose), log_format=args.log_format)
    asyncio.run(args.func(args))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trading", description="Trading data utilities.")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase log verbosity.")
    parser.add_argument(
        "--log-format",
        choices=("text", "json", "otel"),
        default=None,
        help="Log output format. json and otel emit OpenTelemetry-shaped log records. Defaults to TRADING_LOG_FORMAT or text.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    symbols_parser = subparsers.add_parser("symbols", help="List public-data symbols.")
    add_provider_args(symbols_parser)
    symbols_parser.add_argument("--limit", type=int, help="Only print the first N symbols.")
    symbols_parser.set_defaults(func=run_symbols)

    files_parser = subparsers.add_parser("files", help="List CSV trade files for a symbol.")
    add_provider_args(files_parser)
    add_date_args(files_parser)
    files_parser.add_argument("symbol", help="Symbol to inspect, for example BTCUSDT.")
    files_parser.add_argument("--limit", type=int, help="Only print the first N files.")
    files_parser.set_defaults(func=run_files)

    download_parser = subparsers.add_parser("download", help="Download CSV trade files.")
    add_provider_args(download_parser)
    add_date_args(download_parser)
    download_parser.add_argument("symbols", nargs="+", help="Symbols to download, for example BTCUSDT ETHUSDT.")
    download_parser.add_argument("-o", "--output-dir", type=Path, help="Download target.")
    download_parser.add_argument("--limit", type=int, help="Download at most N files per symbol.")
    download_parser.add_argument("--no-extract", action="store_true", help="Keep files as .csv.gz archives.")
    download_parser.add_argument("--keep-archive", action="store_true", help="Keep .csv.gz files after extracting.")
    download_parser.add_argument("--overwrite", action="store_true", help="Replace existing local files.")
    download_parser.add_argument("--max-concurrent", type=int, default=4, help="Maximum concurrent file downloads.")
    download_parser.set_defaults(func=run_download)

    return parser


def add_provider_args(parser: argparse.ArgumentParser) -> None:
    providers = provider_names()
    parser.add_argument("--provider", choices=providers, default=providers[0], help="Market-data provider.")
    for option in all_provider_option_specs():
        if option.value_type == "bool":
            parser.add_argument(
                option.flag,
                dest=option.name,
                default=option.default,
                action=argparse.BooleanOptionalAction,
                help=option.help,
            )
            continue
        parser.add_argument(
            option.flag,
            dest=option.name,
            type=argparse_type(option),
            default=option.default,
            help=option.help,
        )


def add_date_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", help="Start date, inclusive, as YYYY-MM-DD.")
    parser.add_argument("--end-date", help="End date, inclusive, as YYYY-MM-DD.")


async def run_symbols(args: argparse.Namespace) -> None:
    client = make_provider(args)
    symbols = await client.list_symbols()
    for symbol in _limited(symbols, args.limit):
        print(symbol)


async def run_files(args: argparse.Namespace) -> None:
    client = make_provider(args)
    files = await client.list_trade_files(args.symbol, start_date=args.start_date, end_date=args.end_date)
    for trade_file in _limited(files, args.limit):
        print(f"{trade_file.trade_date} {trade_file.filename} {trade_file.url}")


async def run_download(args: argparse.Namespace) -> None:
    client = make_provider(args)
    files: list[MarketDataFile] = []
    for symbol in args.symbols:
        symbol_files = await client.list_trade_files(symbol, start_date=args.start_date, end_date=args.end_date)
        files.extend(_limited(symbol_files, args.limit))

    semaphore = asyncio.Semaphore(args.max_concurrent)
    output_dir = args.output_dir or Path(provider_default_output_dir(args.provider))

    async def download(trade_file: MarketDataFile) -> Path:
        async with semaphore:
            return await client.download_trade_file(
                trade_file,
                output_dir,
                extract=not args.no_extract,
                overwrite=args.overwrite,
                keep_archive=args.keep_archive,
            )

    paths = await asyncio.gather(*(download(trade_file) for trade_file in files))
    for path in paths:
        print(path)


def make_provider(args: argparse.Namespace) -> HistoricalTradeProvider:
    kwargs = {
        option.name: getattr(args, option.name)
        for option in provider_option_specs(args.provider)
    }
    return create_provider(args.provider, **kwargs)


def all_provider_option_specs() -> list[ProviderOption]:
    specs: dict[str, ProviderOption] = {}
    for provider in provider_names():
        for option in provider_option_specs(provider):
            specs.setdefault(option.name, option)
    return list(specs.values())


def argparse_type(option: ProviderOption) -> type[Any]:
    if option.value_type == "int":
        return int
    if option.value_type == "float":
        return float
    return str


def _limited[T](items: list[T], limit: int | None) -> list[T]:
    if limit is None:
        return items
    return items[:limit]


if __name__ == "__main__":
    main()
