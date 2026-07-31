"""Event envelope yielded by the simulator and the builders that shape payloads.

The simulator yields plain schema-shaped dicts; serialisation against the
registry happens only in the transport layer. That split is what lets the unit
suite exercise the whole simulation without a broker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from common import topics
from generator.state_machine import EVENT_SEQ, RideEventType


@dataclass(frozen=True)
class SourceEvent:
    topic: str
    key: str
    value: dict[str, Any]
    ts_ms: int
    # Set by the imperfection layer: the transport serialises the value
    # normally, then corrupts the wire bytes so the message is unparseable
    # downstream and must land in the DLQ.
    corrupt: bool = False


def ride_event(
    *,
    ride_id: str,
    event_type: RideEventType,
    event_ts_ms: int,
    rider_id: str,
    city_id: int,
    pickup: tuple[float, float],
    surge_multiplier: float,
    driver_id: str | None = None,
    dropoff: tuple[float, float] | None = None,
    fare_cents: int | None = None,
    payload_version: int = 1,
    promo_code: str | None = None,
) -> SourceEvent:
    value: dict[str, Any] = {
        "event_id": f"{ride_id}.{EVENT_SEQ[event_type]}",
        "ride_id": ride_id,
        "event_type": event_type.value,
        "event_ts": event_ts_ms,
        "rider_id": rider_id,
        "driver_id": driver_id,
        "city_id": city_id,
        "pickup_lat": pickup[0],
        "pickup_lon": pickup[1],
        "dropoff_lat": dropoff[0] if dropoff else None,
        "dropoff_lon": dropoff[1] if dropoff else None,
        "fare_cents": fare_cents,
        "surge_multiplier": surge_multiplier,
        "payload_version": payload_version,
    }
    if payload_version >= 2:
        value["promo_code"] = promo_code
    return SourceEvent(topics.RIDES_EVENTS, ride_id, value, event_ts_ms)


def payment_event(
    *,
    txn_id: str,
    ride_id: str,
    amount_cents: int,
    status: str,
    method: str,
    ts_ms: int,
) -> SourceEvent:
    value: dict[str, Any] = {
        "txn_id": txn_id,
        "ride_id": ride_id,
        "amount_cents": amount_cents,
        "status": status,
        "method": method,
        "ts": ts_ms,
    }
    return SourceEvent(topics.PAYMENTS_TRANSACTIONS, ride_id, value, ts_ms)


def driver_location_event(
    *,
    driver_id: str,
    ts_ms: int,
    lat: float,
    lon: float,
    speed_kmh: float,
    heading: float,
    status: str,
    city_id: int,
) -> SourceEvent:
    value: dict[str, Any] = {
        "driver_id": driver_id,
        "ts": ts_ms,
        "lat": lat,
        "lon": lon,
        "speed_kmh": speed_kmh,
        "heading": heading,
        "status": status,
        "city_id": city_id,
    }
    return SourceEvent(topics.DRIVERS_LOCATIONS, driver_id, value, ts_ms)
