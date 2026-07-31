"""PgSink batch mechanics against in-memory doubles: one Postgres transaction
per batch (data rows + offset upsert), poison isolation, rollback on failure,
and the pg-stored-offset seek on assignment."""

from __future__ import annotations

import json
from typing import Any

import pytest
from confluent_kafka import OFFSET_BEGINNING, TopicPartition
from prometheus_client import REGISTRY

from common import topics
from processors.pg_sink import OFFSET_UPSERT, SINK_TOPICS, PgSink, PgSinkConfig
from tests.kafka_fakes import FakeConsumer, FakeMessage, json_deserialize

T0 = 1_767_225_600_000


class _FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.executed.append((sql, params))

    def executemany(self, sql: str, rows: Any) -> None:
        if self._conn.fail_contains and self._conn.fail_contains in sql:
            raise RuntimeError("simulated database failure")
        self._conn.executed_many.append((sql, list(rows)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._conn.rows_to_return)


class FakeConnection:
    def __init__(
        self,
        rows_to_return: list[tuple[Any, ...]] | None = None,
        fail_contains: str | None = None,
    ) -> None:
        self.rows_to_return = rows_to_return or []
        self.fail_contains = fail_contains
        self.executed: list[tuple[str, Any]] = []
        self.executed_many: list[tuple[str, list[Any]]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class AssignableFakeConsumer(FakeConsumer):
    def __init__(self) -> None:
        super().__init__()
        self.assigned: list[Any] = []

    def assign(self, partitions: list[Any]) -> None:
        self.assigned = partitions


def _sink(
    connection: FakeConnection, consumer: FakeConsumer | None = None
) -> tuple[PgSink, list[Any]]:
    poisons: list[Any] = []
    sink = PgSink(
        PgSinkConfig(),
        consumer=consumer if consumer is not None else AssignableFakeConsumer(),
        deserializers=dict.fromkeys(SINK_TOPICS, json_deserialize),
        connection=connection,
        on_poison=lambda msg, error: poisons.append((msg, error)),
    )
    return sink, poisons


def _msg(topic: str, partition: int, offset: int, value: dict[str, Any]) -> FakeMessage:
    return FakeMessage(topic, partition, offset, json.dumps(value).encode())


def _event_value() -> dict[str, Any]:
    return {
        "event_id": "r-1.1",
        "ride_id": "r-1",
        "event_type": "requested",
        "event_ts": T0,
        "rider_id": "u-1",
        "driver_id": None,
        "city_id": 1,
        "pickup_lat": 12.97,
        "pickup_lon": 77.59,
        "surge_multiplier": 1.0,
        "payload_version": 1,
    }


def _session_value() -> dict[str, Any]:
    return {
        "ride_id": "r-1",
        "rider_id": "u-1",
        "driver_id": "d-1",
        "city_id": 1,
        "terminal_state": "completed",
        "event_seq": 5,
        "requested_ts": T0,
        "ended_ts": T0 + 300_000,
        "is_late_arrival": False,
        "fare_cents": 20_000,
        "surge_multiplier": 1.0,
        "pickup_lat": 12.97,
        "pickup_lon": 77.59,
    }


def _payment_value() -> dict[str, Any]:
    return {
        "txn_id": "t-1",
        "ride_id": "r-1",
        "amount_cents": 20_000,
        "status": "captured",
        "method": "card",
        "ts": T0 + 310_000,
    }


def _location_value() -> dict[str, Any]:
    return {
        "driver_id": "d-1",
        "ts": T0,
        "lat": 12.9,
        "lon": 77.6,
        "speed_kmh": 30.0,
        "heading": 90.0,
        "status": "on_trip",
        "city_id": 1,
    }


def _sample(name: str, labels: dict[str, str]) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_batch_writes_all_topics_and_offsets_in_one_commit() -> None:
    conn = FakeConnection()
    sink, poisons = _sink(conn)
    before = _sample("sp_rows_written_total", {"service": "pg-sink", "table": "raw.rides_events"})
    written = sink.process_batch(
        [
            _msg(topics.RIDES_EVENTS, 0, 10, _event_value()),
            _msg(topics.RIDES_SESSIONS, 1, 20, _session_value()),
            _msg(topics.PAYMENTS_TRANSACTIONS, 0, 30, _payment_value()),
            _msg(topics.DRIVERS_LOCATIONS, 2, 40, _location_value()),
        ]
    )
    assert written == {
        topics.RIDES_EVENTS: 1,
        topics.RIDES_SESSIONS: 1,
        topics.PAYMENTS_TRANSACTIONS: 1,
        topics.DRIVERS_LOCATIONS: 1,
    }
    assert sink.rows_written == 4
    assert poisons == []
    assert conn.commits == 2  # DDL at construction + the batch
    offsets = next(rows for sql, rows in conn.executed_many if sql == OFFSET_UPSERT)
    assert ("pg-sink", topics.RIDES_EVENTS, 0, 10) in offsets
    assert ("pg-sink", topics.DRIVERS_LOCATIONS, 2, 40) in offsets
    after = _sample("sp_rows_written_total", {"service": "pg-sink", "table": "raw.rides_events"})
    assert after == before + 1


def test_poison_message_is_isolated_and_batch_still_commits() -> None:
    conn = FakeConnection()
    sink, poisons = _sink(conn)
    before = _sample("sp_dlq_messages_total", {"service": "pg-sink", "topic": "rides.events.dlq"})
    written = sink.process_batch(
        [
            _msg(topics.RIDES_EVENTS, 0, 10, _event_value()),
            FakeMessage(topics.RIDES_EVENTS, 0, 11, b"POISON"),
        ]
    )
    assert written == {topics.RIDES_EVENTS: 1}
    assert sink.poison_seen == 1
    assert len(poisons) == 1
    # The poison offset is still committed: it was consumed, not stuck.
    offsets = next(rows for sql, rows in conn.executed_many if sql == OFFSET_UPSERT)
    assert ("pg-sink", topics.RIDES_EVENTS, 0, 11) in offsets
    after = _sample("sp_dlq_messages_total", {"service": "pg-sink", "topic": "rides.events.dlq"})
    assert after == before + 1


def test_database_failure_rolls_back_and_raises() -> None:
    conn = FakeConnection(fail_contains="consumer_offsets")
    sink, _ = _sink(conn)
    with pytest.raises(RuntimeError, match="simulated database failure"):
        sink.process_batch([_msg(topics.RIDES_EVENTS, 0, 10, _event_value())])
    assert conn.rollbacks == 1
    assert sink.rows_written == 0


def test_consumer_error_message_raises() -> None:
    sink, _ = _sink(FakeConnection())
    bad = FakeMessage(topics.RIDES_EVENTS, 0, 0, b"", error="broker exploded")
    with pytest.raises(RuntimeError, match="consumer error"):
        sink.process_batch([bad])


def test_on_assign_seeks_to_pg_stored_offset_plus_one() -> None:
    conn = FakeConnection(rows_to_return=[(topics.RIDES_EVENTS, 0, 792)])
    consumer = AssignableFakeConsumer()
    sink, _ = _sink(conn, consumer)
    parts = [
        TopicPartition(topics.RIDES_EVENTS, 0),
        TopicPartition(topics.RIDES_SESSIONS, 1),
    ]
    sink.on_assign(consumer, parts)
    assert consumer.assigned is parts
    assert parts[0].offset == 793  # stored last-processed + 1
    assert parts[1].offset == OFFSET_BEGINNING  # nothing stored: read from the start


def test_run_loop_consumes_batches_and_closes() -> None:
    conn = FakeConnection()
    consumer = AssignableFakeConsumer()
    consumer.batches = [[_msg(topics.RIDES_EVENTS, 0, 10, _event_value())]]
    sink, _ = _sink(conn, consumer)
    sink.run(max_batches=1)
    assert consumer.subscribed == list(SINK_TOPICS)
    assert consumer.closed
    assert sink.rows_written == 1
