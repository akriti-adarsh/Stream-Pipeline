"""Pure session derivation: fold ride events into one RideSession record.

No I/O here. The fold is order-insensitive (events are indexed by their
lifecycle ordinal, so out-of-order arrival lands in the right slot) and
duplicate-insensitive (same ordinal overwrites with identical content). The
sessionizer feeds it live events; recovery feeds it journal rows; the tests
feed it every pathological ordering the generator can produce.
"""

from __future__ import annotations

from typing import Any

from common.geo import haversine_km
from common.times import to_epoch_ms
from generator.state_machine import EVENT_SEQ, TERMINAL_STATES, RideEventType

LATE_ARRIVAL_THRESHOLD_SEC = 600.0
SESSION_TIMEOUT_MS_DEFAULT = 2 * 3600 * 1000  # 2h event time


class RideAccumulator:
    """Per-ride event slots, keyed by lifecycle ordinal."""

    def __init__(self, ride_id: str) -> None:
        self.ride_id = ride_id
        self.slots: dict[int, dict[str, Any]] = {}

    def add(self, value: dict[str, Any]) -> None:
        event_type = RideEventType(value["event_type"])
        self.slots[EVENT_SEQ[event_type]] = {**value, "event_ts": to_epoch_ms(value["event_ts"])}

    @property
    def terminal(self) -> RideEventType | None:
        for value in self.slots.values():
            kind = RideEventType(value["event_type"])
            if kind in TERMINAL_STATES:
                return kind
        return None

    @property
    def last_event_ts(self) -> int:
        return max(v["event_ts"] for v in self.slots.values()) if self.slots else 0

    def events(self) -> list[dict[str, Any]]:
        return [self.slots[seq] for seq in sorted(self.slots)]


def _get(slots: dict[int, dict[str, Any]], event: RideEventType, field: str) -> Any:
    value = slots.get(EVENT_SEQ[event])
    return None if value is None else value.get(field)


def _first(slots: dict[int, dict[str, Any]], field: str) -> Any:
    for seq in sorted(slots):
        got = slots[seq].get(field)
        if got is not None:
            return got
    return None


def build_session(acc: RideAccumulator, *, closed_by_timeout: bool = False) -> dict[str, Any]:
    """Derive the RideSession record from whatever events actually arrived.

    Tolerant by construction: any subset of lifecycle events yields a valid
    record with nulls where knowledge is missing, because malformed events are
    DLQ-diverted and late ones may never come.
    """
    slots = acc.slots
    terminal = acc.terminal
    if terminal is None and not closed_by_timeout:
        raise ValueError(f"ride {acc.ride_id} is not closable yet")

    requested_ts = _get(slots, RideEventType.REQUESTED, "event_ts")
    matched_ts = _get(slots, RideEventType.MATCHED, "event_ts")
    arrived_ts = _get(slots, RideEventType.DRIVER_ARRIVED, "event_ts")
    started_ts = _get(slots, RideEventType.STARTED, "event_ts")
    if terminal is RideEventType.COMPLETED:
        ended_ts = _get(slots, RideEventType.COMPLETED, "event_ts")
    elif terminal is RideEventType.CANCELLED:
        ended_ts = _get(slots, RideEventType.CANCELLED, "event_ts")
    else:
        ended_ts = None

    def diff_sec(a: Any, b: Any) -> float | None:
        return None if a is None or b is None else max((a - b) / 1000.0, 0.0)

    time_to_match = diff_sec(matched_ts, requested_ts)
    time_to_pickup = diff_sec(arrived_ts, matched_ts)
    ride_duration = diff_sec(ended_ts, started_ts) if terminal is RideEventType.COMPLETED else None

    pickup_lat = _first(slots, "pickup_lat")
    pickup_lon = _first(slots, "pickup_lon")
    dropoff_lat = _first(slots, "dropoff_lat")
    dropoff_lon = _first(slots, "dropoff_lon")

    distance_km: float | None = None
    if None not in (pickup_lat, pickup_lon, dropoff_lat, dropoff_lon):
        distance_km = haversine_km(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)

    avg_speed: float | None = None
    if distance_km is not None and ride_duration is not None and ride_duration > 0:
        avg_speed = distance_km / (ride_duration / 3600.0)

    return {
        "ride_id": acc.ride_id,
        "rider_id": _first(slots, "rider_id"),
        "driver_id": _first(slots, "driver_id"),
        "city_id": _first(slots, "city_id"),
        "terminal_state": terminal.value if terminal is not None else "abandoned",
        "event_seq": len(slots),
        "requested_ts": requested_ts if requested_ts is not None else acc.last_event_ts,
        "matched_ts": matched_ts,
        "driver_arrived_ts": arrived_ts,
        "started_ts": started_ts,
        "ended_ts": ended_ts,
        "time_to_match_sec": time_to_match,
        "time_to_pickup_sec": time_to_pickup,
        "ride_duration_sec": ride_duration,
        "haversine_distance_km": distance_km,
        "avg_speed_kmh": avg_speed,
        "is_late_arrival": (
            time_to_pickup is not None and time_to_pickup > LATE_ARRIVAL_THRESHOLD_SEC
        ),
        "fare_cents": _get(slots, RideEventType.COMPLETED, "fare_cents"),
        "surge_multiplier": _first(slots, "surge_multiplier") or 1.0,
        "promo_code": _first(slots, "promo_code"),
        "pickup_lat": pickup_lat if pickup_lat is not None else 0.0,
        "pickup_lon": pickup_lon if pickup_lon is not None else 0.0,
        "dropoff_lat": dropoff_lat,
        "dropoff_lon": dropoff_lon,
    }


def timed_out(acc: RideAccumulator, watermark_ms: int, timeout_ms: int) -> bool:
    """Event-time timeout: the ride saw no activity for timeout_ms of watermark."""
    return bool(acc.slots) and watermark_ms - acc.last_event_ts > timeout_ms
