"""Canonical topic names and helpers. Every service imports these; nothing hand-types a topic."""

from __future__ import annotations

RIDES_EVENTS = "rides.events"
DRIVERS_LOCATIONS = "drivers.locations"
PAYMENTS_TRANSACTIONS = "payments.transactions"
RIDES_SESSIONS = "rides.sessions"
LATE_EVENTS = "late.events"
# Poison-free mirror of rides.events, forwarded transactionally by the
# sessionizer (which owns the DLQ split). Exists because open-source Flink's
# avro-confluent format cannot skip undeserialisable records: one poison
# message would fail the whole declarative pipeline. See DEVIATIONS.md.
RIDES_EVENTS_CLEAN = "rides.events.clean"

SOURCE_TOPICS = (RIDES_EVENTS, DRIVERS_LOCATIONS, PAYMENTS_TRANSACTIONS)


def dlq_topic(topic: str) -> str:
    """Dead-letter topic for a source topic, per the <topic>.dlq convention."""
    return f"{topic}.dlq"
