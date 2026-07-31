"""Row builders and SQL consistency for the Postgres sink (no database)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from common import topics
from processors.pg_sink import (
    ROW_BUILDERS,
    UPSERTS,
    location_row,
    payment_row,
    rides_event_row,
    session_row,
)

T0 = 1_767_225_600_000


def _ride_value(**extra: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "event_id": "r-1.1",
        "ride_id": "r-1",
        "event_type": "requested",
        "event_ts": T0,
        "rider_id": "u-1",
        "driver_id": None,
        "city_id": 1,
        "pickup_lat": 12.97,
        "pickup_lon": 77.59,
        "dropoff_lat": None,
        "dropoff_lon": None,
        "fare_cents": None,
        "surge_multiplier": 1.1,
        "payload_version": 1,
    }
    base.update(extra)
    return base


def test_rides_event_row_maps_seq_and_timestamps() -> None:
    row = rides_event_row(_ride_value(), partition=3, offset=42)
    assert row[0] == "r-1"
    assert row[1] == 1  # requested -> seq 1
    assert row[4] == datetime.fromtimestamp(T0 / 1000, tz=UTC)
    assert row[-2:] == (3, 42)


def test_rides_event_row_accepts_datetime_input() -> None:
    dt = datetime.fromtimestamp(T0 / 1000, tz=UTC)
    row = rides_event_row(_ride_value(event_ts=dt), partition=0, offset=0)
    assert row[4] == dt


def test_unknown_event_type_raises_for_poison_path() -> None:
    with pytest.raises(ValueError):
        rides_event_row(_ride_value(event_type="teleported"), partition=0, offset=0)


def test_cancelled_maps_to_seq_six() -> None:
    row = rides_event_row(_ride_value(event_type="cancelled"), partition=0, offset=0)
    assert row[1] == 6


def test_every_row_builder_matches_its_sql_arity() -> None:
    """Placeholder count in each upsert must equal the built row's length."""
    samples: dict[str, tuple[Any, ...]] = {
        topics.RIDES_EVENTS: rides_event_row(_ride_value(), 0, 0),
        topics.RIDES_SESSIONS: session_row(
            {
                "ride_id": "r-1",
                "rider_id": "u-1",
                "driver_id": "d-1",
                "city_id": 1,
                "terminal_state": "completed",
                "event_seq": 5,
                "requested_ts": T0,
                "matched_ts": T0 + 1000,
                "driver_arrived_ts": None,
                "started_ts": None,
                "ended_ts": None,
                "time_to_match_sec": 1.0,
                "time_to_pickup_sec": None,
                "ride_duration_sec": None,
                "haversine_distance_km": 2.0,
                "avg_speed_kmh": None,
                "is_late_arrival": False,
                "fare_cents": 100,
                "surge_multiplier": 1.0,
                "promo_code": None,
                "pickup_lat": 1.0,
                "pickup_lon": 2.0,
                "dropoff_lat": None,
                "dropoff_lon": None,
            }
        ),
        topics.PAYMENTS_TRANSACTIONS: payment_row(
            {
                "txn_id": "t-1",
                "ride_id": "r-1",
                "amount_cents": 100,
                "status": "completed",
                "method": "card",
                "ts": T0,
            }
        ),
        topics.DRIVERS_LOCATIONS: location_row(
            {
                "driver_id": "d-1",
                "ts": T0,
                "lat": 1.0,
                "lon": 2.0,
                "speed_kmh": 10.0,
                "heading": 90.0,
                "status": "idle",
                "city_id": 1,
            }
        ),
    }
    for topic, row in samples.items():
        placeholders = UPSERTS[topic].split(" ON CONFLICT")[0].count("%s")
        assert placeholders == len(row), f"{topic}: {placeholders} placeholders vs {len(row)} cols"


def test_row_builders_cover_all_sink_topics() -> None:
    assert set(ROW_BUILDERS) == set(UPSERTS)
