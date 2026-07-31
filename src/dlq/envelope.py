"""Dead-letter envelope: everything needed to diagnose AND replay a poison message.

The envelope preserves the raw bytes exactly (base64), because the whole point
of a DLQ is that the payload could not be parsed; any lossy representation
would make replay impossible. Alongside the bytes travel the error, the
consumer group that gave up, and the original coordinates
(topic, partition, offset) so an operator can correlate with broker logs.
"""

from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer

from common.kafka import producer_config
from common.topics import dlq_topic


@dataclass(frozen=True)
class Envelope:
    source_topic: str
    partition: int
    offset: int
    key_b64: str | None
    payload_b64: str
    error: str
    consumer_group: str
    failed_at: str

    @property
    def payload(self) -> bytes:
        return base64.b64decode(self.payload_b64)

    @property
    def key(self) -> bytes | None:
        return None if self.key_b64 is None else base64.b64decode(self.key_b64)


def build_envelope(msg: Any, error: Exception, consumer_group: str) -> Envelope:
    key = msg.key()
    payload = msg.value() or b""
    return Envelope(
        source_topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        key_b64=None if key is None else base64.b64encode(key).decode(),
        payload_b64=base64.b64encode(payload).decode(),
        error=f"{type(error).__name__}: {error}",
        consumer_group=consumer_group,
        failed_at=datetime.now(tz=UTC).isoformat(timespec="milliseconds"),
    )


def envelope_bytes(envelope: Envelope) -> bytes:
    return json.dumps(asdict(envelope), separators=(",", ":")).encode()


def parse_envelope(data: bytes) -> Envelope:
    return Envelope(**json.loads(data))


class DlqProducer:
    """Non-transactional idempotent producer for consumers that do not run in
    Kafka transactions (the Postgres sink). The sessionizer instead produces
    envelopes with its own transactional producer, inside the batch
    transaction, so even its dead letters are exactly-once."""

    def __init__(self, bootstrap: str, consumer_group: str, producer: Any | None = None) -> None:
        self._producer = producer if producer is not None else Producer(producer_config(bootstrap))
        self._group = consumer_group
        self.sent = 0

    def handle(self, msg: Any, error: Exception) -> None:
        envelope = build_envelope(msg, error, self._group)
        self._producer.produce(
            topic=dlq_topic(envelope.source_topic),
            key=envelope.key,
            value=envelope_bytes(envelope),
        )
        self._producer.poll(0)
        self.sent += 1

    def flush(self, timeout_sec: float = 10.0) -> None:
        self._producer.flush(timeout_sec)
