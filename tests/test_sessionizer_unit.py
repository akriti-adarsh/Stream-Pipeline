"""Sessionizer transactional mechanics against in-memory Kafka doubles.

The real-broker SIGKILL chaos test lives in the integration suite; these tests
pin the ordering and recovery CONTRACT: state commit, then begin, produce,
send_offsets, commit, and rollback-on-abort leaving no trace.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import TopicPartition

from common import topics
from processors.ride_sessionizer import Sessionizer, SessionizerConfig
from processors.state_store import StateStore
from tests.kafka_fakes import (
    FakeConsumer,
    FakeMessage,
    FakeTransactionalProducer,
    json_deserialize,
    json_serialize,
)

T0 = 1_767_225_600_000


def _msg(partition: int, offset: int, value: dict[str, Any]) -> FakeMessage:
    return FakeMessage(topics.RIDES_EVENTS, partition, offset, json.dumps(value).encode())


def _event(ride_id: str, event_type: str, ts: int, **extra: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "event_id": f"{ride_id}.{event_type}",
        "ride_id": ride_id,
        "event_type": event_type,
        "event_ts": ts,
        "rider_id": "u-1",
        "driver_id": "d-1-0001" if event_type != "requested" else None,
        "city_id": 1,
        "pickup_lat": 12.97,
        "pickup_lon": 77.59,
        "dropoff_lat": 12.93,
        "dropoff_lon": 77.62,
        "fare_cents": 20_000 if event_type == "completed" else None,
        "surge_multiplier": 1.0,
        "payload_version": 1,
    }
    base.update(extra)
    return base


def _full_ride(ride_id: str, partition: int, start_offset: int, t0: int) -> list[FakeMessage]:
    kinds = ["requested", "matched", "driver_arrived", "started", "completed"]
    return [
        _msg(partition, start_offset + i, _event(ride_id, kind, t0 + i * 60_000))
        for i, kind in enumerate(kinds)
    ]


def _sessionizer(
    tmp_path: Path,
    consumer: FakeConsumer,
    producer: FakeTransactionalProducer,
    **cfg_over: Any,
) -> Sessionizer:
    cfg = SessionizerConfig(state_path=str(tmp_path / "state.db"), **cfg_over)
    return Sessionizer(
        cfg,
        consumer=consumer,
        producer=producer,
        deserialize=json_deserialize,
        serialize_session=json_serialize,
        serialize_event=json_serialize,
        store=StateStore(cfg.state_path),
    )


def test_full_ride_in_one_batch_emits_one_session_with_correct_txn_ordering(
    tmp_path: Path,
) -> None:
    consumer = FakeConsumer()
    producer = FakeTransactionalProducer()
    sess = _sessionizer(tmp_path, consumer, producer)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])

    sessions = sess.process_batch(_full_ride("r-1", 0, 100, T0))
    assert len(sessions) == 1
    assert sessions[0]["terminal_state"] == "completed"

    ops = [op for op, _ in producer.log]
    assert ops[0] == "begin"
    assert ops[-2:] == ["send_offsets", "commit"]
    assert ops[1:-2] == ["produce"] * 6  # 5 clean-mirror forwards + 1 session
    produced_topics = [payload[0] for op, payload in producer.log if op == "produce"]
    assert produced_topics.count(topics.RIDES_EVENTS_CLEAN) == 5
    assert produced_topics.count(topics.RIDES_SESSIONS) == 1
    _, offsets = next(entry for entry in producer.log if entry[0] == "send_offsets")
    assert offsets == [(topics.RIDES_EVENTS, 0, 105)]  # last offset 104 + 1


def test_ride_spanning_batches_closes_in_second_batch(tmp_path: Path) -> None:
    consumer = FakeConsumer()
    producer = FakeTransactionalProducer()
    sess = _sessionizer(tmp_path, consumer, producer)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])

    ride = _full_ride("r-2", 0, 10, T0)
    assert sess.process_batch(ride[:3]) == []
    sessions = sess.process_batch(ride[3:])
    assert len(sessions) == 1
    assert sessions[0]["event_seq"] == 5
    assert len(producer.committed_sessions()) == 1


def test_commit_failure_aborts_and_rolls_back_state_then_replay_emits_once(
    tmp_path: Path,
) -> None:
    """The batch's Kafka txn aborts; local state must forget the batch, and a
    fresh instance replaying it must emit the identical session exactly once."""
    consumer = FakeConsumer(committed_offsets={(topics.RIDES_EVENTS, 0): 100})
    producer = FakeTransactionalProducer(fail_commits=1)
    sess = _sessionizer(tmp_path, consumer, producer)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])

    batch = _full_ride("r-3", 0, 100, T0)
    with pytest.raises(RuntimeError, match="simulated commit failure"):
        sess.process_batch(batch)
    assert ("abort", None) in producer.log
    assert producer.committed_sessions() == []

    # Same store, fresh instance, same committed offset: replay.
    consumer2 = FakeConsumer(committed_offsets={(topics.RIDES_EVENTS, 0): 100})
    producer2 = FakeTransactionalProducer()
    cfg = SessionizerConfig(state_path=str(tmp_path / "state.db"))
    sess2 = Sessionizer(
        cfg,
        consumer=consumer2,
        producer=producer2,
        deserialize=json_deserialize,
        serialize_session=json_serialize,
        serialize_event=json_serialize,
        store=StateStore(cfg.state_path),
    )
    sess2.on_assign(consumer2, [TopicPartition(topics.RIDES_EVENTS, 0)])
    sessions = sess2.process_batch(batch)
    assert len(sessions) == 1
    committed = producer2.committed_sessions()
    assert len(committed) == 1
    assert committed[0]["ride_id"] == "r-3"
    assert committed[0]["terminal_state"] == "completed"


def test_crash_hook_fires_after_state_commit_before_kafka_txn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fired = {}

    def fake_exit(code: int) -> None:
        fired["code"] = code
        raise SystemExit(code)

    monkeypatch.setattr("processors.ride_sessionizer.os._exit", fake_exit)
    consumer = FakeConsumer()
    producer = FakeTransactionalProducer()
    sess = _sessionizer(tmp_path, consumer, producer, crash_after_state_commit=True)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])

    with pytest.raises(SystemExit):
        sess.process_batch(_full_ride("r-4", 0, 0, T0))
    assert fired["code"] == 137
    # State was committed (the close exists), but no Kafka transaction began.
    store = StateStore(tmp_path / "state.db")
    assert store.is_closed("r-4")
    assert producer.log == []
    # Recovery with committed offset 0 erases the phantom close.
    consumer2 = FakeConsumer(committed_offsets={(topics.RIDES_EVENTS, 0): 0})
    producer2 = FakeTransactionalProducer()
    cfg = SessionizerConfig(state_path=str(tmp_path / "state.db"))
    sess2 = Sessionizer(
        cfg,
        consumer=consumer2,
        producer=producer2,
        deserialize=json_deserialize,
        serialize_session=json_serialize,
        serialize_event=json_serialize,
        store=store,
    )
    sess2.on_assign(consumer2, [TopicPartition(topics.RIDES_EVENTS, 0)])
    assert not store.is_closed("r-4")
    sessions = sess2.process_batch(_full_ride("r-4", 0, 0, T0))
    assert len(sessions) == 1
    assert len(producer2.committed_sessions()) == 1


def test_timeout_closes_abandoned_ride(tmp_path: Path) -> None:
    consumer = FakeConsumer()
    producer = FakeTransactionalProducer()
    sess = _sessionizer(tmp_path, consumer, producer, session_timeout_ms=3_600_000)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])

    sess.process_batch(
        [
            _msg(0, 0, _event("r-5", "requested", T0)),
            _msg(0, 1, _event("r-5", "matched", T0 + 30_000)),
        ]
    )
    # Unrelated traffic two hours later pushes the watermark past the timeout.
    sessions = sess.process_batch(
        [_msg(0, 2, _event("r-6", "requested", T0 + 2 * 3_600_000 + 31_000))]
    )
    abandoned = [s for s in sessions if s["ride_id"] == "r-5"]
    assert len(abandoned) == 1
    assert abandoned[0]["terminal_state"] == "abandoned"
    assert abandoned[0]["ended_ts"] is None


def test_late_event_for_closed_ride_is_skipped(tmp_path: Path) -> None:
    consumer = FakeConsumer()
    producer = FakeTransactionalProducer()
    sess = _sessionizer(tmp_path, consumer, producer)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])

    sess.process_batch(_full_ride("r-7", 0, 0, T0))
    sessions = sess.process_batch([_msg(0, 5, _event("r-7", "started", T0 + 1))])
    assert sessions == []
    assert len(producer.committed_sessions()) == 1  # still exactly one


def test_poison_message_is_counted_and_skipped_without_killing_the_batch(
    tmp_path: Path,
) -> None:
    consumer = FakeConsumer()
    producer = FakeTransactionalProducer()
    sess = _sessionizer(tmp_path, consumer, producer)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])

    ride = _full_ride("r-8", 0, 0, T0)
    poisoned = [
        *ride[:2],
        FakeMessage(topics.RIDES_EVENTS, 0, 2, b"POISON"),
        *ride[2:],
    ]
    sessions = sess.process_batch(poisoned)
    assert sess.poison_seen == 1
    assert len(sessions) == 1  # the ride still closed
    _, offsets = next(entry for entry in producer.log if entry[0] == "send_offsets")
    assert offsets == [(topics.RIDES_EVENTS, 0, 5)]  # poison offset is consumed, not stuck


def test_run_loop_respects_max_batches_and_closes_consumer(tmp_path: Path) -> None:
    consumer = FakeConsumer()
    consumer.batches = [_full_ride("r-9", 0, 0, T0)]
    producer = FakeTransactionalProducer()
    sess = _sessionizer(tmp_path, consumer, producer)
    sess.on_assign(consumer, [TopicPartition(topics.RIDES_EVENTS, 0)])
    sess.run(max_batches=1)
    assert consumer.subscribed == [topics.RIDES_EVENTS]
    assert consumer.closed
    assert ("init", None) in producer.log
    assert sess.sessions_emitted == 1
