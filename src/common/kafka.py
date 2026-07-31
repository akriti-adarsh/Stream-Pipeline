"""Shared Kafka plumbing: topic bootstrap and producer/consumer config builders.

Topic creation is idempotent and runs at service startup, so a bare
``docker compose up`` self-initialises without any manual admin step.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from common import topics

TOPIC_PARTITIONS: dict[str, int] = {
    topics.RIDES_EVENTS: 6,
    topics.DRIVERS_LOCATIONS: 6,
    topics.PAYMENTS_TRANSACTIONS: 3,
    topics.RIDES_SESSIONS: 3,
    topics.LATE_EVENTS: 1,
    topics.dlq_topic(topics.RIDES_EVENTS): 1,
    topics.dlq_topic(topics.PAYMENTS_TRANSACTIONS): 1,
    topics.dlq_topic(topics.RIDES_SESSIONS): 1,
}


def ensure_topics(bootstrap: str, partitions: dict[str, int] | None = None) -> None:
    """Create every platform topic that does not exist yet (replication 1: single broker)."""
    wanted = partitions or TOPIC_PARTITIONS
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [
        NewTopic(name, num_partitions=count, replication_factor=1)
        for name, count in wanted.items()
        if name not in existing
    ]
    if not missing:
        return
    for name, future in admin.create_topics(missing, request_timeout=15).items():
        try:
            future.result(timeout=15)
        except Exception as exc:  # topic may have been created concurrently
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"failed to create topic {name}: {exc}") from exc


def producer_config(bootstrap: str, *, transactional_id: str | None = None) -> dict[str, Any]:
    """Baseline safe producer config: idempotent, full acks, modest batching."""
    conf: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
    }
    if transactional_id is not None:
        conf["transactional.id"] = transactional_id
    return conf


def consumer_config(
    bootstrap: str,
    group_id: str,
    *,
    read_committed: bool = True,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
) -> dict[str, Any]:
    """Baseline consumer config.

    read_committed matters: Kafka's default is read_uncommitted, which would
    happily surface records from aborted transactions and quietly void every
    exactly-once claim downstream. Nothing in this repo reads uncommitted.
    """
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
        "isolation.level": "read_committed" if read_committed else "read_uncommitted",
    }
