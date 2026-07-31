"""Structured JSON logging shared by every service in the platform.

Each log line is one JSON object so the compose stack's stdout is machine
parseable end to end. Cross-service correlation happens on ``ride_id`` (and
any other key passed through :func:`with_ctx`), which is how a single ride can
be traced from the generator through the sessionizer to the Postgres sink.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    """Render every record as a single-line JSON object."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "service": self._service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        ctx = getattr(record, "ctx", None)
        if isinstance(ctx, dict):
            payload.update(ctx)
        if record.exc_info and record.exc_info[0] is not None:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(service: str, level: int = logging.INFO) -> logging.Logger:
    """Install the JSON formatter on the root logger and return a service logger.

    Idempotent: repeated calls replace the handler rather than stacking
    duplicates, so services can call it defensively at startup.
    """
    root = logging.getLogger()
    root.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root.handlers[:] = [handler]
    return logging.getLogger(service)


def with_ctx(**ctx: Any) -> dict[str, dict[str, Any]]:
    """Build the ``extra`` mapping carrying correlated fields.

    Usage: ``log.info("session closed", extra=with_ctx(ride_id=ride_id))``.
    The formatter merges these keys into the top level of the JSON line.
    """
    return {"ctx": ctx}
