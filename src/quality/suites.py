"""Great Expectations suite definitions and the rolling row-count baseline.

Two suites, run as real pipeline steps (not decoration):

- ``raw_rides_events``: structural contract on the landing table: exact column
  set, non-null keys, domain ranges on coordinates and money, categorical
  membership on event_type and city_id.
- ``fct_rides``: mart-level contract: primary-key uniqueness, categorical
  membership, row-count anomaly detection against a rolling baseline from
  prior runs (stored in serving.dq_results), and freshness.

Freshness note: the spec words freshness as "max event_ts within N minutes of
now", but event time here is an accelerated SIMULATION clock that legitimately
outruns the wall clock at up to 60x, so wall-clock freshness is asserted on
``ingested_at`` (when the sink actually landed rows), which is the honest
liveness signal. Recorded in DEVIATIONS.md.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from statistics import median
from typing import Any

import great_expectations as gx
from great_expectations import expectations as gxe

RIDE_EVENT_COLUMNS = [
    "ride_id",
    "event_seq",
    "event_id",
    "event_type",
    "event_ts",
    "rider_id",
    "driver_id",
    "city_id",
    "pickup_lat",
    "pickup_lon",
    "dropoff_lat",
    "dropoff_lon",
    "fare_cents",
    "surge_multiplier",
    "payload_version",
    "promo_code",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
]

EVENT_TYPES = ["requested", "matched", "driver_arrived", "started", "completed", "cancelled"]
CITY_IDS = [1, 2, 3, 4, 5]

# Domain bounding box for the five simulated cities, generous margins included.
LAT_BOUNDS = (8.0, 32.0)
LON_BOUNDS = (68.0, 92.0)


def raw_rides_events_expectations() -> list[Any]:
    return [
        gxe.ExpectTableColumnsToMatchSet(column_set=RIDE_EVENT_COLUMNS, exact_match=True),
        gxe.ExpectColumnValuesToNotBeNull(column="ride_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_type"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_ts"),
        gxe.ExpectColumnValuesToNotBeNull(column="rider_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="city_id"),
        gxe.ExpectColumnValuesToBeInSet(column="event_type", value_set=EVENT_TYPES),
        gxe.ExpectColumnValuesToBeInSet(column="city_id", value_set=CITY_IDS),
        gxe.ExpectColumnValuesToBeBetween(
            column="pickup_lat", min_value=LAT_BOUNDS[0], max_value=LAT_BOUNDS[1]
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="pickup_lon", min_value=LON_BOUNDS[0], max_value=LON_BOUNDS[1]
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="dropoff_lat", min_value=LAT_BOUNDS[0], max_value=LAT_BOUNDS[1]
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="dropoff_lon", min_value=LON_BOUNDS[0], max_value=LON_BOUNDS[1]
        ),
        gxe.ExpectColumnValuesToBeBetween(column="fare_cents", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="surge_multiplier", min_value=1.0, max_value=3.0),
        gxe.ExpectColumnValuesToBeInSet(column="payload_version", value_set=[1, 2]),
    ]


def rowcount_bounds(history: list[int]) -> tuple[int, int | None]:
    """Rolling baseline for an append-only fact table.

    The floor is the highest count ever observed: this table only grows, so a
    shrink means data loss. The ceiling guards against runaway duplication:
    four times the rolling median of recent observations plus slack for a
    young table. With no history the bounds are wide open (first run learns).
    """
    if not history:
        return 0, None
    floor = max(history)
    recent = history[-10:]
    ceiling = int(median(recent)) * 4 + 1000
    return floor, max(ceiling, floor + 1000)


def fct_rides_expectations(
    history: list[int], freshness_minutes: int, now: datetime | None = None
) -> list[Any]:
    floor, ceiling = rowcount_bounds(history)
    now = now or datetime.now(tz=UTC)
    return [
        gxe.ExpectColumnValuesToBeUnique(column="ride_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="ride_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="terminal_state"),
        gxe.ExpectColumnValuesToNotBeNull(column="requested_at"),
        gxe.ExpectColumnValuesToBeInSet(
            column="terminal_state", value_set=["completed", "cancelled", "abandoned"]
        ),
        gxe.ExpectTableRowCountToBeBetween(min_value=floor, max_value=ceiling),
        gxe.ExpectColumnMaxToBeBetween(
            column="ingested_at",
            min_value=now - timedelta(minutes=freshness_minutes),
        ),
    ]


def build_suite(name: str, expectations: list[Any]) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=name)
    for expectation in expectations:
        suite.add_expectation(expectation)
    return suite
