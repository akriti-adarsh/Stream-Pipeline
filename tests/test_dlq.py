"""DLQ envelope round trips, transactional wiring, and the replay core."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from confluent_kafka import TopicPartition

from common import topics
from common.topics import dlq_topic
from dlq.envelope import DlqProducer, build_envelope, envelope_bytes, parse_envelope
from processors.ride_sessionizer import Sessionizer, SessionizerConfig
from processors.state_store import StateStore
from tests.kafka_fakes import (
    FakeConsumer,
    FakeMessage,
    FakeTransactionalProducer,
    json_deserialize,
    json_serialize,
)

sys.path.insert(0, "scripts")
from replay_dlq import ReplayStats, repair_magic_byte, replay


def _poison_msg(offset: int = 7) -> FakeMessage:
    return FakeMessage(topics.RIDES_EVENTS, 2, offset, b"\xffgarbage-bytes", key=b"r-13")


def test_envelope_roundtrip_preserves_exact_bytes() -> None:
    envelope = build_envelope(_poison_msg(), ValueError("magic byte"), "sessionizer")
    parsed = parse_envelope(envelope_bytes(envelope))
    assert parsed.payload == b"\xffgarbage-bytes"
    assert parsed.key == b"r-13"
    assert parsed.source_topic == topics.RIDES_EVENTS
    assert parsed.partition == 2
    assert parsed.offset == 7
    assert parsed.consumer_group == "sessionizer"
    assert parsed.error == "ValueError: magic byte"
    assert parsed.failed_at.endswith("+00:00")


def test_envelope_handles_null_key() -> None:
    msg = FakeMessage(topics.PAYMENTS_TRANSACTIONS, 0, 1, b"junk")
    parsed = parse_envelope(envelope_bytes(build_envelope(msg, ValueError("x"), "pg-sink")))
    assert parsed.key is None


def test_dlq_producer_routes_to_dlq_topic() -> None:
    class FakeProducer:
        def __init__(self) -> None:
            self.produced: list[tuple[str, bytes | None, bytes]] = []

        def produce(self, topic: str, key: bytes | None, value: bytes) -> None:
            self.produced.append((topic, key, value))

        def poll(self, timeout: float) -> int:
            return 0

        def flush(self, timeout: float) -> int:
            return 0

    fake = FakeProducer()
    dlq = DlqProducer("unused:9092", "pg-sink", producer=fake)
    dlq.handle(_poison_msg(), ValueError("bad"))
    dlq.flush()
    assert dlq.sent == 1
    topic, _, value = fake.produced[0]
    assert topic == dlq_topic(topics.RIDES_EVENTS)
    assert parse_envelope(value).error == "ValueError: bad"


def test_sessionizer_produces_envelope_inside_transaction(tmp_path: Path) -> None:
    consumer = FakeConsumer()
    producer = FakeTransactionalProducer()
    cfg = SessionizerConfig(state_path=str(tmp_path / "s.db"))
    sess = Sessionizer(
        cfg,
        consumer=consumer,
        producer=producer,
        deserialize=json_deserialize,
        serialize_session=json_serialize,
        serialize_event=json_serialize,
        store=StateStore(cfg.state_path),
    )
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])
    sess.process_batch([FakeMessage(topics.RIDES_EVENTS, 0, 0, b"POISON", key=b"r-1")])

    ops = [op for op, _ in producer.log]
    assert ops == ["begin", "produce", "send_offsets", "commit"]
    _, (produced_topic, _, payload) = producer.log[1]
    assert produced_topic == dlq_topic(topics.RIDES_EVENTS)
    envelope = parse_envelope(payload)
    assert envelope.payload == b"POISON"
    assert envelope.consumer_group == "sessionizer"
    # And the poison offset was committed: it is consumed, not stuck.
    _, offsets = producer.log[2]
    assert offsets == [(topics.RIDES_EVENTS, 0, 1)]


def test_repair_magic_byte_restores_framing() -> None:
    assert repair_magic_byte(b"\xffrest-of-message") == b"\x00rest-of-message"
    assert repair_magic_byte(b"") == b""


def test_replay_core_filters_transforms_and_produces() -> None:
    envelopes = [
        build_envelope(_poison_msg(1), ValueError("magic byte"), "sessionizer"),
        build_envelope(
            FakeMessage(topics.RIDES_EVENTS, 0, 2, b"\xffother", key=b"k"),
            RuntimeError("different failure"),
            "sessionizer",
        ),
    ]
    produced: list[tuple[str, bytes | None, bytes]] = []
    stats = replay(
        envelopes,
        lambda topic, key, payload: produced.append((topic, key, payload)),
        predicate=lambda e: e.error.startswith("ValueError"),
        transform=repair_magic_byte,
    )
    assert (stats.seen, stats.filtered_out, stats.replayed) == (2, 1, 1)
    assert produced == [(topics.RIDES_EVENTS, b"r-13", b"\x00garbage-bytes")]


def test_replay_dry_run_produces_nothing() -> None:
    envelopes = [build_envelope(_poison_msg(), ValueError("x"), "g")]
    produced: list[Any] = []
    stats = replay(envelopes, lambda *a: produced.append(a), dry_run=True)
    assert stats.replayed == 1
    assert produced == []
    assert json.loads(stats.as_json())["dry_run"] is True


def test_replay_stats_json_shape() -> None:
    stats = ReplayStats(seen=5, filtered_out=2, replayed=3)
    assert json.loads(stats.as_json()) == {
        "seen": 5,
        "filtered_out": 2,
        "replayed": 3,
        "dry_run": False,
    }
