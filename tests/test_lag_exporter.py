"""Lag exporter shaping logic and the collect pipeline against fakes."""

from __future__ import annotations

from typing import Any

import pytest
from confluent_kafka import TopicPartition
from prometheus_client import REGISTRY

from observability.lag_exporter import (
    LagExporter,
    LagSample,
    committed_next_from_broker,
    committed_next_from_pg,
    compute_lags,
    publish,
)

OFFSET_INVALID = -1001


def test_broker_committed_offsets_keep_next_semantics_and_drop_sentinels() -> None:
    offsets = [
        TopicPartition("rides.events", 0, 120),
        TopicPartition("rides.events", 1, 0),
        TopicPartition("rides.events", 2, OFFSET_INVALID),
    ]
    shaped = committed_next_from_broker(offsets)
    assert shaped == {
        ("rides.events", 0): 120,
        ("rides.events", 1): 0,
        ("rides.events", 2): None,
    }


def test_pg_stored_offsets_are_last_processed_so_next_is_plus_one() -> None:
    rows = [("rides.events", 0, 792), ("rides.sessions", 2, 0)]
    assert committed_next_from_pg(rows) == {
        ("rides.events", 0): 793,
        ("rides.sessions", 2): 1,
    }


def test_compute_lags_subtracts_clamps_and_skips_uncommitted() -> None:
    ends = {
        ("rides.events", 0): 1000,
        ("rides.events", 1): 500,
        ("rides.events", 2): 50,
        ("rides.events", 3): 10,
    }
    committed = {
        ("rides.events", 0): 900,
        ("rides.events", 1): 500,
        ("rides.events", 2): 60,  # ahead of end (racy watermark read): clamp to 0
        ("rides.events", 3): None,  # never committed: skipped, not "fully lagging"
    }
    samples = compute_lags("sessionizer", ends, committed)
    assert samples == [
        LagSample("sessionizer", "rides.events", 0, 100),
        LagSample("sessionizer", "rides.events", 1, 0),
        LagSample("sessionizer", "rides.events", 2, 0),
    ]


def test_publish_sets_the_gauge_per_partition() -> None:
    publish([LagSample("g1", "rides.events", 4, 42)])
    value = REGISTRY.get_sample_value(
        "sp_consumer_lag", {"group": "g1", "topic": "rides.events", "partition": "4"}
    )
    assert value == 42


class _FakeMetadataTopic:
    def __init__(self, partitions: list[int]) -> None:
        self.partitions = dict.fromkeys(partitions)


class _FakeMetadata:
    def __init__(self, topics: dict[str, list[int]]) -> None:
        self.topics = {name: _FakeMetadataTopic(parts) for name, parts in topics.items()}


class _FakeProbe:
    def __init__(self, topics: dict[str, list[int]], highs: dict[tuple[str, int], int]) -> None:
        self._metadata = _FakeMetadata(topics)
        self._highs = highs

    def list_topics(self, timeout: float) -> _FakeMetadata:
        return self._metadata

    def get_watermark_offsets(self, tp: Any, timeout: float, cached: bool) -> tuple[int, int]:
        return 0, self._highs[(tp.topic, tp.partition)]


class _FakeGroupConsumer:
    def __init__(self, committed: dict[tuple[str, int], int]) -> None:
        self._committed = committed

    def committed(self, parts: list[Any], timeout: float) -> list[TopicPartition]:
        return [
            TopicPartition(
                tp.topic,
                tp.partition,
                self._committed.get((tp.topic, tp.partition), OFFSET_INVALID),
            )
            for tp in parts
        ]


def test_collect_merges_broker_groups_and_pg_group() -> None:
    probe = _FakeProbe(
        topics={"rides.events": [0, 1], "rides.sessions": [0]},
        highs={("rides.events", 0): 100, ("rides.events", 1): 80, ("rides.sessions", 0): 30},
    )
    groups = {
        "sessionizer": _FakeGroupConsumer({("rides.events", 0): 90, ("rides.events", 1): 80}),
        "iceberg-sink": _FakeGroupConsumer({}),  # nothing committed yet: no samples
    }
    exporter = LagExporter(
        "unused:0",
        "postgresql://unused",
        probe=probe,
        group_consumers=groups,
        pg_rows=lambda: [("rides.sessions", 0, 24)],
    )
    samples = exporter.collect()
    assert LagSample("sessionizer", "rides.events", 0, 10) in samples
    assert LagSample("sessionizer", "rides.events", 1, 0) in samples
    # pg-sink: stored last-processed 24 -> next 25 -> lag 30 - 25 = 5
    assert LagSample("pg-sink", "rides.sessions", 0, 5) in samples
    assert all(sample.group != "iceberg-sink" for sample in samples)


def test_run_forever_publishes_and_survives_a_failed_cycle(monkeypatch: Any) -> None:
    sleeps = {"count": 0}

    def fake_sleep(_seconds: float) -> None:
        sleeps["count"] += 1
        if sleeps["count"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("observability.lag_exporter.time.sleep", fake_sleep)

    pg_calls = {"count": 0}

    def flaky_pg_rows() -> list[tuple[str, int, int]]:
        pg_calls["count"] += 1
        if pg_calls["count"] > 1:
            raise ConnectionError("postgres is having a moment")
        return [("rides.sessions", 0, 24)]

    probe = _FakeProbe(
        topics={"rides.sessions": [0]},
        highs={("rides.sessions", 0): 30},
    )
    exporter = LagExporter(
        "unused:0",
        "postgresql://unused",
        probe=probe,
        group_consumers={"sessionizer": _FakeGroupConsumer({("rides.sessions", 0): 28})},
        pg_rows=flaky_pg_rows,
    )
    with pytest.raises(KeyboardInterrupt):
        exporter.run_forever()
    # Cycle one published; cycle two failed inside collect and was survived.
    assert sleeps["count"] == 2
    assert pg_calls["count"] == 2
    value = REGISTRY.get_sample_value(
        "sp_consumer_lag", {"group": "pg-sink", "topic": "rides.sessions", "partition": "0"}
    )
    assert value == 5
