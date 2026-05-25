from __future__ import annotations

import json
import logging
import os
import time
from typing import Literal

type LogFormat = Literal["text", "json", "otel"]

_STANDARD_LOG_RECORD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {"asctime", "message"}


class OtelJsonLogFormatter(logging.Formatter):
    """Emit one OpenTelemetry log-record-shaped JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        attributes = self._attributes(record)
        payload: dict[str, object] = {
            "time_unix_nano": int(record.created * 1_000_000_000),
            "observed_time_unix_nano": time.time_ns(),
            "severity_text": record.levelname,
            "severity_number": severity_number(record.levelno),
            "body": record.getMessage(),
            "resource": resource_attributes(),
            "scope": {"name": record.name},
            "attributes": attributes,
        }
        if trace_id := attributes.pop("trace_id", None):
            payload["trace_id"] = trace_id
        if span_id := attributes.pop("span_id", None):
            payload["span_id"] = span_id
        if trace_flags := attributes.pop("trace_flags", None):
            payload["trace_flags"] = trace_flags
        return json.dumps(payload, default=str, separators=(",", ":"))

    def _attributes(self, record: logging.LogRecord) -> dict[str, object]:
        attributes: dict[str, object] = {
            "code.file.path": record.pathname,
            "code.function.name": record.funcName,
            "code.line.number": record.lineno,
            "code.namespace": record.module,
            "process.pid": record.process,
            "thread.name": record.threadName,
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_RECORD_ATTRS and not key.startswith("_"):
                attributes[key] = value
        if record.exc_info:
            exception_type = record.exc_info[0]
            exception = record.exc_info[1]
            if exception_type is not None:
                attributes["exception.type"] = f"{exception_type.__module__}.{exception_type.__name__}"
            if exception is not None:
                attributes["exception.message"] = str(exception)
            attributes["exception.stacktrace"] = self.formatException(record.exc_info)
        if record.stack_info:
            attributes["code.stacktrace"] = self.formatStack(record.stack_info)
        return attributes


JsonLogFormatter = OtelJsonLogFormatter


def configure_logging(
    *,
    level: int | str | None = None,
    log_format: LogFormat | str | None = None,
    force: bool = True,
) -> None:
    level_value: int | str = level if level is not None else os.getenv("TRADING_LOG_LEVEL", "WARNING")
    format_value = log_format if log_format is not None else os.getenv("TRADING_LOG_FORMAT", "text")
    resolved_level = _resolve_level(level_value)
    resolved_format = _resolve_format(format_value)
    handler = logging.StreamHandler()
    if resolved_format in {"json", "otel"}:
        handler.setFormatter(OtelJsonLogFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))

    logging.basicConfig(level=resolved_level, handlers=[handler], force=force)


def level_from_verbosity(verbosity: int) -> int:
    if verbosity == 1:
        return logging.INFO
    if verbosity >= 2:
        return logging.DEBUG
    return logging.WARNING


def _resolve_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    resolved = logging.getLevelNamesMapping().get(level.upper())
    if isinstance(resolved, int):
        return resolved
    raise ValueError(f"Unsupported log level: {level!r}")


def _resolve_format(log_format: str) -> LogFormat:
    normalized = log_format.strip().lower()
    if normalized in {"text", "json", "otel"}:
        return normalized  # type: ignore[return-value]
    raise ValueError(f"Unsupported log format: {log_format!r}")


def severity_number(level: int) -> int:
    if level >= logging.CRITICAL:
        return 21
    if level >= logging.ERROR:
        return 17
    if level >= logging.WARNING:
        return 13
    if level >= logging.INFO:
        return 9
    if level >= logging.DEBUG:
        return 5
    if level > logging.NOTSET:
        return 1
    return 0


def resource_attributes() -> dict[str, object]:
    attributes: dict[str, object] = {
        "service.name": os.getenv("OTEL_SERVICE_NAME", os.getenv("TRADING_SERVICE_NAME", "trading")),
    }
    if namespace := os.getenv("TRADING_SERVICE_NAMESPACE"):
        attributes["service.namespace"] = namespace
    if deployment_environment := os.getenv("TRADING_DEPLOYMENT_ENVIRONMENT"):
        attributes["deployment.environment.name"] = deployment_environment
    return attributes
