"""Transport orchestration tests with a fake producer.

Real serialisation against the live registry is integration-tested with the
compose stack; here the contract under test is everything around it: manifest
bookkeeping, corruption mechanics, and delivery accounting.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from common import topics
from generator.events import SourceEvent, driver_location_event, payment_event, ride_event
from generator.producer import KafkaTransport, Serializer, corrupt_payload
from generator.state_machine import RideEventType


class FakeProducer:
    def __init__(self, fail_topics: set[str] | None = None) -> None:
        self.messages: list[dict[str, Any]] = []
        self._fail_topics = fail_topics or set()

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Any,
    ) -> None:
        self.messages.append({"topic": topic, "key": key, "value": value})
        if topic in self._fail_topics:
            on_delivery("simulated broker error", None)
        else:
            on_delivery(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


def _fake_serializers() -> dict[str, Serializer]:
    def serialize(topic: str, value: dict[str, Any]) -> bytes:
        return b"\x00" + json.dumps(value, sort_keys=True).encode()

    return dict.fromkeys(
        (topics.RIDES_EVENTS, topics.DRIVERS_LOCATIONS, topics.PAYMENTS_TRANSACTIONS), serialize
    )


def _transport(tmp_path: Path, producer: FakeProducer) -> KafkaTransport:
    return KafkaTransport(
        "unused:9092",
        "http://unused",
        acked_manifest=tmp_path / "acked.jsonl",
        producer=producer,
        serializers=_fake_serializers(),
        initialise=False,
    )


def _ride(i: int, corrupt: bool = False) -> SourceEvent:
    event = ride_event(
        ride_id=f"r-{i}",
        event_type=RideEventType.REQUESTED,
        event_ts_ms=1000 + i,
        rider_id="u-1",
        city_id=1,
        pickup=(12.9, 77.5),
        surge_multiplier=1.0,
    )
    if corrupt:
        return replace(event, corrupt=True)
    return event


def test_acked_events_land_in_manifest(tmp_path: Path) -> None:
    producer = FakeProducer()
    transport = _transport(tmp_path, producer)
    transport.send(_ride(1))
    transport.send(
        payment_event(
            txn_id="t-r-9",
            ride_id="r-9",
            amount_cents=100,
            status="completed",
            method="card",
            ts_ms=5,
        )
    )
    transport.close()
    lines = [json.loads(line) for line in (tmp_path / "acked.jsonl").read_text().splitlines()]
    assert {"topic": topics.RIDES_EVENTS, "id": "r-1.1"} in lines
    assert {"topic": topics.PAYMENTS_TRANSACTIONS, "id": "t-r-9"} in lines
    assert transport.acked[topics.RIDES_EVENTS] == 1
    assert transport.acked[topics.PAYMENTS_TRANSACTIONS] == 1


def test_corrupt_events_get_broken_magic_byte_and_stay_out_of_manifest(tmp_path: Path) -> None:
    producer = FakeProducer()
    transport = _transport(tmp_path, producer)
    transport.send(_ride(2, corrupt=True))
    transport.close()
    assert transport.corrupted == 1
    wire = producer.messages[0]["value"]
    assert wire[0] == 0xFF, "magic byte must be overwritten"
    assert (tmp_path / "acked.jsonl").read_text() == ""
    # The corruption is exactly repairable: restore the magic byte.
    assert wire[1:] == json.dumps(_ride(2).value, sort_keys=True).encode()


def test_corrupt_payload_is_repairable_roundtrip() -> None:
    original = b"\x00\x00\x00\x00\x07payload-bytes"
    corrupted = corrupt_payload(original)
    assert corrupted != original
    assert b"\x00" + corrupted[1:] == original


def test_failed_deliveries_are_counted_not_acked(tmp_path: Path) -> None:
    producer = FakeProducer(fail_topics={topics.RIDES_EVENTS})
    transport = _transport(tmp_path, producer)
    transport.send(_ride(3))
    transport.close()
    assert transport.failed[topics.RIDES_EVENTS] == 1
    assert transport.acked[topics.RIDES_EVENTS] == 0
    assert (tmp_path / "acked.jsonl").read_text() == ""


def test_locations_are_acked_but_not_manifested(tmp_path: Path) -> None:
    producer = FakeProducer()
    transport = _transport(tmp_path, producer)
    transport.send(
        driver_location_event(
            driver_id="d-1-0001",
            ts_ms=1,
            lat=12.9,
            lon=77.5,
            speed_kmh=10.0,
            heading=90.0,
            status="idle",
            city_id=1,
        )
    )
    transport.close()
    assert transport.acked[topics.DRIVERS_LOCATIONS] == 1
    assert (tmp_path / "acked.jsonl").read_text() == ""
