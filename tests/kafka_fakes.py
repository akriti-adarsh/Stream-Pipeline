"""In-memory doubles for confluent-kafka objects, shared by processor tests.

They mimic exactly the slice of the API the processors touch, and they record
every call so tests can assert transactional ordering, not just outcomes.
"""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import TopicPartition


class FakeMessage:
    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        value: bytes,
        *,
        key: bytes | None = None,
        error: Any | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._error = error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def error(self) -> Any | None:
        return self._error


class FakeConsumer:
    def __init__(self, committed_offsets: dict[tuple[str, int], int] | None = None) -> None:
        self.committed_offsets = committed_offsets or {}
        self.subscribed: list[str] = []
        self.batches: list[list[FakeMessage]] = []
        self.seeks: list[TopicPartition] = []
        self.closed = False
        self.assign_callback: Any = None
        self.revoke_callback: Any = None

    def subscribe(
        self, subscription: list[str], on_assign: Any = None, on_revoke: Any = None
    ) -> None:
        self.subscribed = subscription
        self.assign_callback = on_assign
        self.revoke_callback = on_revoke

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        return self.batches.pop(0) if self.batches else []

    def committed(
        self, partitions: list[TopicPartition], timeout: float = 10
    ) -> list[TopicPartition]:
        return [
            TopicPartition(
                tp.topic,
                tp.partition,
                self.committed_offsets.get((tp.topic, tp.partition), -1001),
            )
            for tp in partitions
        ]

    def consumer_group_metadata(self) -> str:
        return "fake-group-metadata"

    def seek(self, tp: TopicPartition) -> None:
        self.seeks.append(tp)

    def close(self) -> None:
        self.closed = True


class FakeTransactionalProducer:
    def __init__(self, fail_commits: int = 0) -> None:
        self.log: list[tuple[str, Any]] = []
        self._fail_commits = fail_commits

    def init_transactions(self) -> None:
        self.log.append(("init", None))

    def begin_transaction(self) -> None:
        self.log.append(("begin", None))

    def produce(self, topic: str, key: bytes, value: bytes, **kwargs: Any) -> None:
        self.log.append(("produce", (topic, key, value)))

    def send_offsets_to_transaction(self, offsets: list[TopicPartition], metadata: Any) -> None:
        self.log.append(("send_offsets", [(o.topic, o.partition, o.offset) for o in offsets]))

    def commit_transaction(self) -> None:
        if self._fail_commits > 0:
            self._fail_commits -= 1
            self.log.append(("commit_failed", None))
            raise RuntimeError("simulated commit failure")
        self.log.append(("commit", None))

    def abort_transaction(self) -> None:
        self.log.append(("abort", None))

    def committed_sessions(self) -> list[dict[str, Any]]:
        """Session values produced in transactions that COMMITTED."""
        sessions: list[dict[str, Any]] = []
        pending: list[dict[str, Any]] = []
        for op, payload in self.log:
            if op == "begin":
                pending = []
            elif op == "produce":
                pending.append(json.loads(payload[2]))
            elif op == "commit":
                sessions.extend(pending)
                pending = []
            elif op in ("abort", "commit_failed"):
                pending = []
        return sessions


def json_deserialize(topic: str, payload: bytes) -> dict[str, Any]:
    if payload == b"POISON":
        raise ValueError("unparseable payload")
    result = json.loads(payload)
    assert isinstance(result, dict)
    return result


def json_serialize(topic: str, value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True).encode()
