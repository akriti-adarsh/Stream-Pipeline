"""Schema loading, registration, and serde construction for every topic.

Schemas live as files under ``schemas/`` at the repo root (Avro ``.avsc`` and
JSON Schema ``.schema.json``) so they are reviewable artifacts, not strings
buried in code. This module is the single place that turns them into
registry-backed serializers and deserializers.

The registry is Redpanda's built-in Schema Registry (Confluent-compatible REST
API on port 8081), so ``confluent_kafka.schema_registry`` clients work
unchanged.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import jsonschema
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

from common import topics


def schema_dir() -> Path:
    """Resolve the schemas directory: env override first, repo-relative default second."""
    override = os.environ.get("SCHEMA_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "schemas"


def load_avro_schema(filename: str) -> str:
    """Return the raw Avro schema JSON string for a file under schemas/avro/."""
    return (schema_dir() / "avro" / filename).read_text(encoding="utf-8")


def load_json_schema(filename: str) -> str:
    """Return the raw JSON Schema string for a file under schemas/json/."""
    return (schema_dir() / "json" / filename).read_text(encoding="utf-8")


AVRO_SUBJECTS: dict[str, tuple[str, ...]] = {
    # subject -> schema files registered in order (older versions first)
    f"{topics.RIDES_EVENTS}-value": ("rides_events_v1.avsc", "rides_events_v2.avsc"),
    f"{topics.DRIVERS_LOCATIONS}-value": ("drivers_locations.avsc",),
    f"{topics.RIDES_SESSIONS}-value": ("rides_sessions.avsc",),
}

JSON_SUBJECTS: dict[str, str] = {
    f"{topics.PAYMENTS_TRANSACTIONS}-value": "payments_transactions.schema.json",
}


def make_registry_client(url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


def register_all(url: str) -> dict[str, int]:
    """Register every subject idempotently and pin BACKWARD compatibility.

    Registering rides.events v1 then v2 under the same subject is a live proof
    that the evolution is registry-legal: a BACKWARD-incompatible v2 would be
    rejected here with an HTTP 409.
    Returns subject -> latest schema id.
    """
    client = make_registry_client(url)
    registered: dict[str, int] = {}
    for subject, files in AVRO_SUBJECTS.items():
        for filename in files:
            schema = Schema(load_avro_schema(filename), "AVRO")
            registered[subject] = client.register_schema(subject, schema)
        client.set_compatibility(subject, "BACKWARD")
    for subject, filename in JSON_SUBJECTS.items():
        schema = Schema(load_json_schema(filename), "JSON")
        registered[subject] = client.register_schema(subject, schema)
        client.set_compatibility(subject, "BACKWARD")
    return registered


def ride_event_serializer(client: SchemaRegistryClient, version: int = 1) -> AvroSerializer:
    filename = "rides_events_v1.avsc" if version == 1 else "rides_events_v2.avsc"
    return AvroSerializer(client, load_avro_schema(filename))


def ride_event_deserializer(client: SchemaRegistryClient) -> AvroDeserializer:
    """Reader pinned to the v2 schema: tolerates v1 data (promo_code defaults to null)
    and v2 data alike, which is the consumer side of the schema-evolution story."""
    return AvroDeserializer(client, load_avro_schema("rides_events_v2.avsc"))


def driver_location_serializer(client: SchemaRegistryClient) -> AvroSerializer:
    return AvroSerializer(client, load_avro_schema("drivers_locations.avsc"))


def driver_location_deserializer(client: SchemaRegistryClient) -> AvroDeserializer:
    return AvroDeserializer(client, load_avro_schema("drivers_locations.avsc"))


def ride_session_serializer(client: SchemaRegistryClient) -> AvroSerializer:
    return AvroSerializer(client, load_avro_schema("rides_sessions.avsc"))


def ride_session_deserializer(client: SchemaRegistryClient) -> AvroDeserializer:
    return AvroDeserializer(client, load_avro_schema("rides_sessions.avsc"))


def payment_json_bytes(value: dict[str, Any]) -> bytes:
    """Serialise a payment as PLAIN JSON, validated against the registered schema.

    Deliberately not Confluent-framed: open-source Flink has no format for the
    registry's 5-byte JSON Schema framing, and the payments topic feeds a
    Flink interval join. The subject still lives in the registry (see
    register_all), so the schema is governed; only the wire framing differs.
    Recorded in DEVIATIONS.md.
    """
    jsonschema.validate(value, _payment_schema())
    return json.dumps(value, separators=(",", ":")).encode()


def parse_payment(data: bytes) -> dict[str, Any]:
    """Parse and validate a plain-JSON payment; raises on any malformed input."""
    value = json.loads(data)
    if not isinstance(value, dict):
        raise jsonschema.ValidationError(f"payment must be an object, got {type(value)}")
    jsonschema.validate(value, _payment_schema())
    return value


@lru_cache(maxsize=1)
def _payment_schema() -> dict[str, Any]:
    return parsed(load_json_schema("payments_transactions.schema.json"))


def parsed(schema_text: str) -> dict[str, Any]:
    """Parse a schema JSON string into a dict (test and tooling helper)."""
    result: dict[str, Any] = json.loads(schema_text)
    return result
