"""Session derivation under every arrival pathology the generator produces."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from common.geo import haversine_km
from processors.session_builder import (
    RideAccumulator,
    build_session,
    timed_out,
)

T0 = 1_767_225_600_000  # requested
T_MATCH = T0 + 30_000
T_ARRIVE = T0 + 210_000
T_START = T0 + 250_000
T_END = T0 + 1_150_000

PICKUP = (12.9716, 77.5946)
DROPOFF = (12.9352, 77.6245)


def _event(event_type: str, ts: int, **extra: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "event_id": f"r-1.{event_type}",
        "ride_id": "r-1",
        "event_type": event_type,
        "event_ts": ts,
        "rider_id": "u-7",
        "driver_id": None,
        "city_id": 1,
        "pickup_lat": PICKUP[0],
        "pickup_lon": PICKUP[1],
        "dropoff_lat": None,
        "dropoff_lon": None,
        "fare_cents": None,
        "surge_multiplier": 1.2,
        "payload_version": 1,
    }
    base.update(extra)
    return base


def _full_ride_events() -> list[dict[str, Any]]:
    return [
        _event("requested", T0, dropoff_lat=DROPOFF[0], dropoff_lon=DROPOFF[1]),
        _event("matched", T_MATCH, driver_id="d-1-0001"),
        _event("driver_arrived", T_ARRIVE, driver_id="d-1-0001"),
        _event("started", T_START, driver_id="d-1-0001"),
        _event(
            "completed",
            T_END,
            driver_id="d-1-0001",
            fare_cents=31_400,
            dropoff_lat=DROPOFF[0],
            dropoff_lon=DROPOFF[1],
        ),
    ]


def _fold(events: list[dict[str, Any]]) -> RideAccumulator:
    acc = RideAccumulator("r-1")
    for event in events:
        acc.add(event)
    return acc


def test_happy_path_derivations_are_correct() -> None:
    session = build_session(_fold(_full_ride_events()))
    assert session["terminal_state"] == "completed"
    assert session["event_seq"] == 5
    assert session["time_to_match_sec"] == 30.0
    assert session["time_to_pickup_sec"] == 180.0
    assert session["ride_duration_sec"] == 900.0
    expected_km = haversine_km(*PICKUP, *DROPOFF)
    assert session["haversine_distance_km"] == pytest.approx(expected_km)
    assert session["avg_speed_kmh"] == pytest.approx(expected_km / 0.25)
    assert session["is_late_arrival"] is False
    assert session["fare_cents"] == 31_400
    assert session["driver_id"] == "d-1-0001"
    assert session["ended_ts"] == T_END


def test_out_of_order_arrival_yields_identical_session() -> None:
    events = _full_ride_events()
    shuffled = [events[3], events[0], events[4], events[1], events[2]]
    assert build_session(_fold(shuffled)) == build_session(_fold(events))


def test_duplicates_fold_once() -> None:
    events = _full_ride_events()
    with_dupes = [*events, events[1], events[4]]
    session = build_session(_fold(with_dupes))
    assert session["event_seq"] == 5


def test_cancelled_ride_has_null_trip_fields() -> None:
    events = [
        _event("requested", T0),
        _event("matched", T_MATCH, driver_id="d-1-0002"),
        _event("cancelled", T0 + 90_000, driver_id="d-1-0002"),
    ]
    session = build_session(_fold(events))
    assert session["terminal_state"] == "cancelled"
    assert session["ride_duration_sec"] is None
    assert session["fare_cents"] is None
    assert session["ended_ts"] == T0 + 90_000
    assert session["time_to_match_sec"] == 30.0


def test_late_arrival_flag_trips_over_threshold() -> None:
    events = [
        _event("requested", T0),
        _event("matched", T_MATCH),
        _event("driver_arrived", T_MATCH + 601_000),
        _event("started", T_MATCH + 700_000),
        _event("completed", T_MATCH + 1_500_000, fare_cents=100),
    ]
    assert build_session(_fold(events))["is_late_arrival"] is True


def test_missing_intermediate_event_is_tolerated() -> None:
    # 'started' was malformed and went to the DLQ; the session still closes.
    events = [
        _event("requested", T0),
        _event("matched", T_MATCH, driver_id="d-1-0003"),
        _event(
            "completed",
            T_END,
            driver_id="d-1-0003",
            fare_cents=20_000,
            dropoff_lat=DROPOFF[0],
            dropoff_lon=DROPOFF[1],
        ),
    ]
    session = build_session(_fold(events))
    assert session["terminal_state"] == "completed"
    assert session["ride_duration_sec"] is None
    assert session["haversine_distance_km"] is not None


def test_abandoned_close_via_timeout() -> None:
    acc = _fold([_event("requested", T0), _event("matched", T_MATCH)])
    timeout_ms = 2 * 3600 * 1000
    assert not timed_out(acc, T_MATCH + timeout_ms, timeout_ms)
    assert timed_out(acc, T_MATCH + timeout_ms + 1, timeout_ms)
    session = build_session(acc, closed_by_timeout=True)
    assert session["terminal_state"] == "abandoned"
    assert session["ended_ts"] is None
    assert session["event_seq"] == 2


def test_open_ride_refuses_to_close_without_timeout() -> None:
    acc = _fold([_event("requested", T0)])
    with pytest.raises(ValueError, match="not closable"):
        build_session(acc)


def test_promo_code_carried_from_v2_events() -> None:
    events = _full_ride_events()
    events[3] = {**events[3], "payload_version": 2, "promo_code": "SAVE10"}
    session = build_session(_fold(events))
    assert session["promo_code"] == "SAVE10"


def test_datetime_inputs_are_normalised() -> None:
    events = _full_ride_events()
    events[0] = {**events[0], "event_ts": datetime.fromtimestamp(T0 / 1000, tz=UTC)}
    session = build_session(_fold(events))
    assert session["requested_ts"] == T0
