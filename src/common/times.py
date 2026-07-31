"""Timestamp normalisation at serde boundaries.

Avro's timestamp-millis logical type decodes to timezone-aware datetimes;
internally every service works in UTC epoch milliseconds. These helpers are
the only place that conversion happens.
"""

from __future__ import annotations

from datetime import UTC, datetime


def to_epoch_ms(value: datetime | int) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return value


def to_epoch_ms_opt(value: datetime | int | None) -> int | None:
    return None if value is None else to_epoch_ms(value)


def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="milliseconds")
