"""Serde tests for every schema file: legality, round trips, and v1 to v2 evolution.

These run without any registry: fastavro performs real Avro serialisation with
writer/reader schema resolution, which is exactly the mechanism the registry
serializers use underneath. Note the platform-wide serde convention proven
here: timestamp-millis fields accept epoch-millis integers on write and always
decode to timezone-aware datetimes on read.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from typing import Any

import fastavro
import jsonschema
import pytest

from common.schemas import (
    AVRO_SUBJECTS,
    JSON_SUBJECTS,
    load_avro_schema,
    load_json_schema,
    parsed,
)


def _dt(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


def _roundtrip(
    writer_schema: dict[str, Any], reader_schema: dict[str, Any], record: dict[str, Any]
) -> dict[str, Any]:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(writer_schema), record)
    buf.seek(0)
    return dict(
        fastavro.schemaless_reader(  # type: ignore[arg-type]
            buf, fastavro.parse_schema(writer_schema), fastavro.parse_schema(reader_schema)
        )
    )


V1_EVENT: dict[str, Any] = {
    "event_id": "e-0001",
    "ride_id": "r-0001",
    "event_type": "requested",
    "event_ts": 1_750_000_000_000,
    "rider_id": "u-9",
    "driver_id": None,
    "city_id": 1,
    "pickup_lat": 12.9716,
    "pickup_lon": 77.5946,
    "dropoff_lat": None,
    "dropoff_lon": None,
    "fare_cents": None,
    "surge_multiplier": 1.0,
    "payload_version": 1,
}

V1_DECODED: dict[str, Any] = {**V1_EVENT, "event_ts": _dt(1_750_000_000_000)}


def test_every_avro_schema_file_is_legal() -> None:
    for files in AVRO_SUBJECTS.values():
        for filename in files:
            fastavro.parse_schema(parsed(load_avro_schema(filename)))


def test_v1_roundtrip_is_lossless() -> None:
    schema = parsed(load_avro_schema("rides_events_v1.avsc"))
    assert _roundtrip(schema, schema, V1_EVENT) == V1_DECODED


def test_v1_data_read_with_v2_reader_gains_null_promo_code() -> None:
    v1 = parsed(load_avro_schema("rides_events_v1.avsc"))
    v2 = parsed(load_avro_schema("rides_events_v2.avsc"))
    decoded = _roundtrip(v1, v2, V1_EVENT)
    assert decoded == {**V1_DECODED, "promo_code": None}


def test_v2_data_read_with_v1_reader_drops_promo_code() -> None:
    v1 = parsed(load_avro_schema("rides_events_v1.avsc"))
    v2 = parsed(load_avro_schema("rides_events_v2.avsc"))
    v2_event = {**V1_EVENT, "payload_version": 2, "promo_code": "SAVE10"}
    decoded = _roundtrip(v2, v1, v2_event)
    assert "promo_code" not in decoded
    assert decoded == {**V1_DECODED, "payload_version": 2}


def test_every_nullable_field_has_null_default() -> None:
    """Evolution hygiene: any union with null must default to null, in every Avro schema.

    This is the property that makes adding optional fields backward compatible,
    so it is enforced across the whole schema set rather than assumed.
    """
    for files in AVRO_SUBJECTS.values():
        for filename in files:
            for field in parsed(load_avro_schema(filename))["fields"]:
                if isinstance(field["type"], list) and "null" in field["type"]:
                    assert field["type"][0] == "null", f"{filename}:{field['name']} null not first"
                    assert field.get("default", "MISSING") is None, (
                        f"{filename}:{field['name']} lacks null default"
                    )


def test_session_schema_roundtrip() -> None:
    schema = parsed(load_avro_schema("rides_sessions.avsc"))
    record: dict[str, Any] = {
        "ride_id": "r-1",
        "rider_id": "u-1",
        "driver_id": "d-1",
        "city_id": 2,
        "terminal_state": "completed",
        "event_seq": 5,
        "requested_ts": 1_750_000_000_000,
        "matched_ts": 1_750_000_030_000,
        "driver_arrived_ts": 1_750_000_200_000,
        "started_ts": 1_750_000_260_000,
        "ended_ts": 1_750_001_000_000,
        "time_to_match_sec": 30.0,
        "time_to_pickup_sec": 170.0,
        "ride_duration_sec": 740.0,
        "haversine_distance_km": 6.2,
        "avg_speed_kmh": 30.2,
        "is_late_arrival": False,
        "fare_cents": 45_500,
        "surge_multiplier": 1.2,
        "promo_code": None,
        "pickup_lat": 12.97,
        "pickup_lon": 77.59,
        "dropoff_lat": 12.93,
        "dropoff_lon": 77.62,
    }
    expected = {
        **record,
        **{
            key: _dt(record[key])
            for key in ("requested_ts", "matched_ts", "driver_arrived_ts", "started_ts", "ended_ts")
        },
    }
    assert _roundtrip(schema, schema, record) == expected


class TestPaymentJsonSchema:
    schema: dict[str, Any] = json.loads(load_json_schema("payments_transactions.schema.json"))

    def _valid(self) -> dict[str, Any]:
        return {
            "txn_id": "t-1",
            "ride_id": "r-1",
            "amount_cents": 45_500,
            "status": "completed",
            "method": "card",
            "ts": 1_750_001_060_000,
        }

    def test_valid_payment_passes(self) -> None:
        jsonschema.validate(self._valid(), self.schema)

    def test_unknown_status_fails(self) -> None:
        bad = {**self._valid(), "status": "definitely-not-a-status"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, self.schema)

    def test_negative_amount_fails(self) -> None:
        bad = {**self._valid(), "amount_cents": -5}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, self.schema)

    def test_extra_field_fails(self) -> None:
        bad = {**self._valid(), "smuggled": True}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, self.schema)


def test_subject_maps_cover_all_topics() -> None:
    subjects = set(AVRO_SUBJECTS) | set(JSON_SUBJECTS)
    assert subjects == {
        "rides.events-value",
        "drivers.locations-value",
        "rides.sessions-value",
        "payments.transactions-value",
    }
