"""Unit tests for the shared structured-logging module."""

from __future__ import annotations

import json
import logging
import sys

from common.logging import JsonFormatter, configure_logging, with_ctx


def _record(msg: str, **extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_format_is_single_line_json() -> None:
    formatter = JsonFormatter(service="unit-test")
    line = formatter.format(_record("hello"))
    payload = json.loads(line)
    assert payload["msg"] == "hello"
    assert payload["service"] == "unit-test"
    assert payload["level"] == "info"
    assert "\n" not in line


def test_with_ctx_merges_correlation_keys_top_level() -> None:
    formatter = JsonFormatter(service="unit-test")
    line = formatter.format(_record("session closed", **with_ctx(ride_id="r-42", city_id=3)))
    payload = json.loads(line)
    assert payload["ride_id"] == "r-42"
    assert payload["city_id"] == 3


def test_exception_is_serialised() -> None:
    formatter = JsonFormatter(service="unit-test")
    try:
        raise ValueError("boom")
    except ValueError:
        record = _record("failed")
        record.exc_info = sys.exc_info()
    payload = json.loads(formatter.format(record))
    assert "ValueError: boom" in payload["exc"]


def test_configure_logging_is_idempotent() -> None:
    configure_logging("svc")
    configure_logging("svc")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)
