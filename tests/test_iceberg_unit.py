"""Pure-logic tests for the Iceberg sink: arrow shaping and evolution policy."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sinks.iceberg_sink import (
    ICEBERG_SCHEMA_V1,
    PARTITION_SPEC,
    arrow_schema,
    should_evolve,
    to_arrow,
)

T0 = 1_767_225_600_000


def _value(version: int = 1, **extra: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "event_id": "r-1.1",
        "ride_id": "r-1",
        "event_type": "requested",
        "event_ts": T0,
        "rider_id": "u-1",
        "driver_id": None,
        "city_id": 3,
        "pickup_lat": 28.6,
        "pickup_lon": 77.2,
        "dropoff_lat": None,
        "dropoff_lon": None,
        "fare_cents": None,
        "surge_multiplier": 1.2,
        "payload_version": version,
    }
    if version >= 2:
        base["promo_code"] = extra.pop("promo_code", None)
    base.update(extra)
    return base


def test_arrow_schema_toggles_promo_column() -> None:
    assert "promo_code" not in arrow_schema(False).names
    assert "promo_code" in arrow_schema(True).names
    assert len(arrow_schema(False).names) == len(ICEBERG_SCHEMA_V1.fields)


def test_to_arrow_converts_timestamps_and_nulls() -> None:
    batch = to_arrow([_value()], with_promo=False)
    assert batch.num_rows == 1
    ts = batch.column("event_ts")[0].as_py()
    assert ts == datetime.fromtimestamp(T0 / 1000, tz=UTC)
    assert batch.column("driver_id")[0].as_py() is None
    assert batch.column("fare_cents")[0].as_py() is None
    assert batch.column("city_id")[0].as_py() == 3


def test_to_arrow_with_promo_reads_missing_as_null() -> None:
    batch = to_arrow([_value(), _value(2, promo_code="SAVE10")], with_promo=True)
    assert batch.column("promo_code").to_pylist() == [None, "SAVE10"]


def test_should_evolve_only_once_and_only_on_v2() -> None:
    assert not should_evolve([_value(), _value()], already_evolved=False)
    assert should_evolve([_value(), _value(2)], already_evolved=False)
    assert not should_evolve([_value(2)], already_evolved=True)


def test_partition_spec_covers_day_and_city() -> None:
    names = [field.name for field in PARTITION_SPEC.fields]
    assert names == ["event_day", "city_id_part"]
