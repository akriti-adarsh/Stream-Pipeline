"""Kafka transport for the generator.

Serialises against the Schema Registry (Avro for rides and locations, JSON
Schema for payments), produces with an idempotent producer, and records every
broker-acknowledged event id to a manifest file. That manifest is the ground
truth the exactly-once kill test compares Postgres against: "what the broker
acked" is the only honest definition of "what was produced".

Corrupt-flagged events are serialised normally and then have their first wire
byte (the registry framing's magic byte) overwritten, which makes them
unparseable to every registry-aware consumer: the canonical poison message.
The DLQ replay tooling can repair exactly this corruption, closing the
poison -> DLQ -> fix -> replay loop with real bytes.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import IO, Any

from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField, SerializationContext

from common import topics
from common.kafka import ensure_topics, producer_config
from common.logging import with_ctx
from common.schemas import (
    driver_location_serializer,
    make_registry_client,
    payment_serializer,
    register_all,
    ride_event_serializer,
)
from generator.events import SourceEvent

Serializer = Callable[[str, dict[str, Any]], bytes]

CORRUPT_FIRST_BYTE = 0xFF


def corrupt_payload(payload: bytes) -> bytes:
    """Overwrite the magic byte so registry-aware deserialisation must fail."""
    return bytes([CORRUPT_FIRST_BYTE]) + payload[1:]


def _wrap(confluent_serializer: Any) -> Serializer:
    def serialize(topic: str, value: dict[str, Any]) -> bytes:
        data = confluent_serializer(value, SerializationContext(topic, MessageField.VALUE))
        assert isinstance(data, bytes)
        return data

    return serialize


def build_serializers(registry_url: str, *, ride_schema_version: int = 2) -> dict[str, Serializer]:
    client = make_registry_client(registry_url)
    return {
        topics.RIDES_EVENTS: _wrap(ride_event_serializer(client, version=ride_schema_version)),
        topics.DRIVERS_LOCATIONS: _wrap(driver_location_serializer(client)),
        topics.PAYMENTS_TRANSACTIONS: _wrap(payment_serializer(client)),
    }


class KafkaTransport:
    """Produce SourceEvents; track acks; keep the manifest honest."""

    def __init__(
        self,
        bootstrap: str,
        registry_url: str,
        *,
        acked_manifest: Path | None = None,
        producer: Any | None = None,
        serializers: dict[str, Serializer] | None = None,
        initialise: bool = True,
    ) -> None:
        self._log = logging.getLogger("generator-transport")
        if initialise:
            ensure_topics(bootstrap)
            register_all(registry_url)
        self._producer = producer if producer is not None else Producer(producer_config(bootstrap))
        self._serializers = (
            serializers if serializers is not None else build_serializers(registry_url)
        )
        self.acked: Counter[str] = Counter()
        self.failed: Counter[str] = Counter()
        self.corrupted = 0
        self._manifest: IO[str] | None = None
        if acked_manifest is not None:
            acked_manifest.parent.mkdir(parents=True, exist_ok=True)
            self._manifest = acked_manifest.open("w", encoding="utf-8", newline="\n")

    # ------------------------------------------------------------------ sends

    def send(self, event: SourceEvent) -> None:
        payload = self._serializers[event.topic](event.topic, event.value)
        if event.corrupt:
            payload = corrupt_payload(payload)
            self.corrupted += 1
        stable_id = self._stable_id(event)
        self._producer.produce(
            topic=event.topic,
            key=event.key.encode("utf-8"),
            value=payload,
            timestamp=event.ts_ms,
            on_delivery=self._on_delivery(event.topic, stable_id, event.corrupt),
        )
        self._producer.poll(0)

    @staticmethod
    def _stable_id(event: SourceEvent) -> str | None:
        if event.topic == topics.RIDES_EVENTS:
            return str(event.value["event_id"])
        if event.topic == topics.PAYMENTS_TRANSACTIONS:
            return str(event.value["txn_id"])
        return None

    def _on_delivery(
        self, topic: str, stable_id: str | None, corrupt: bool
    ) -> Callable[[Any, Any], None]:
        def callback(err: Any, msg: Any) -> None:
            if err is not None:
                self.failed[topic] += 1
                self._log.warning(
                    "delivery failed", extra=with_ctx(topic=topic, error=str(err), id=stable_id)
                )
                return
            self.acked[topic] += 1
            if self._manifest is not None and stable_id is not None and not corrupt:
                self._manifest.write(json.dumps({"topic": topic, "id": stable_id}) + "\n")

        return callback

    # ---------------------------------------------------------------- closing

    def flush(self, timeout_sec: float = 30.0) -> int:
        remaining = self._producer.flush(timeout_sec)
        assert isinstance(remaining, int)
        return remaining

    def close(self) -> None:
        remaining = self.flush()
        if self._manifest is not None:
            self._manifest.flush()
            self._manifest.close()
        self._log.info(
            "transport closed",
            extra=with_ctx(
                acked=dict(self.acked),
                failed=dict(self.failed),
                corrupted=self.corrupted,
                unflushed=remaining,
            ),
        )
