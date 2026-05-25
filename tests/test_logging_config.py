from __future__ import annotations

import json
import logging

from trading.logging_config import OtelJsonLogFormatter, severity_number


def test_otel_json_log_formatter_includes_extra_fields() -> None:
    record = logging.LogRecord(
        name="trading.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="loaded %s",
        args=("rows",),
        exc_info=None,
    )
    record.event = "test.event"
    record.rows = 3

    payload = json.loads(OtelJsonLogFormatter().format(record))

    assert payload["severity_text"] == "INFO"
    assert payload["severity_number"] == 9
    assert payload["body"] == "loaded rows"
    assert payload["scope"] == {"name": "trading.test"}
    assert payload["resource"]["service.name"] == "trading"
    assert payload["attributes"]["event"] == "test.event"
    assert payload["attributes"]["rows"] == 3
    assert payload["attributes"]["code.function.name"] is None


def test_severity_number_maps_python_levels_to_otel_ranges() -> None:
    assert severity_number(logging.DEBUG) == 5
    assert severity_number(logging.INFO) == 9
    assert severity_number(logging.WARNING) == 13
    assert severity_number(logging.ERROR) == 17
    assert severity_number(logging.CRITICAL) == 21
