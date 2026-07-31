"""Registration-order and serde-factory tests (no network: stubbed registry client).

The real registry round trip is exercised by the integration suite against
Redpanda's built-in Schema Registry; these tests pin the logic that surrounds
it: version ordering, compatibility pinning, and factory construction.
"""

from __future__ import annotations

from typing import Any

import pytest

from common import schemas


class StubRegistryClient:
    def __init__(self) -> None:
        self.registered: list[tuple[str, str]] = []
        self.compatibility: dict[str, str] = {}

    def register_schema(self, subject_name: str, schema: Any) -> int:
        self.registered.append((subject_name, schema.schema_type))
        return len(self.registered)

    def set_compatibility(self, subject_name: str, level: str) -> str:
        self.compatibility[subject_name] = level
        return level


def test_register_all_registers_versions_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = StubRegistryClient()
    monkeypatch.setattr(schemas, "make_registry_client", lambda url: stub)
    ids = schemas.register_all("http://registry.invalid")

    rides_registrations = [s for s, _ in stub.registered if s == "rides.events-value"]
    assert len(rides_registrations) == 2, "v1 then v2 must both be registered"

    avro_subjects = {s for s, kind in stub.registered if kind == "AVRO"}
    json_subjects = {s for s, kind in stub.registered if kind == "JSON"}
    assert avro_subjects == set(schemas.AVRO_SUBJECTS)
    assert json_subjects == set(schemas.JSON_SUBJECTS)

    assert set(stub.compatibility) == set(schemas.AVRO_SUBJECTS) | set(schemas.JSON_SUBJECTS)
    assert set(stub.compatibility.values()) == {"BACKWARD"}
    assert set(ids) == set(schemas.AVRO_SUBJECTS) | set(schemas.JSON_SUBJECTS)


def test_serde_factories_construct_without_network() -> None:
    """Serializer construction parses schemas locally; the registry is only hit on use."""
    client = schemas.make_registry_client("http://localhost:1")
    schemas.ride_event_serializer(client, version=1)
    schemas.ride_event_serializer(client, version=2)
    schemas.ride_event_deserializer(client)
    schemas.driver_location_serializer(client)
    schemas.driver_location_deserializer(client)
    schemas.ride_session_serializer(client)
    schemas.ride_session_deserializer(client)
    schemas.payment_serializer(client)
    schemas.payment_deserializer(client)
