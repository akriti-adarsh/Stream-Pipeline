"""Postgres sink against the LIVE compose stack (marked integration).

Requires: docker compose --profile core up, topics already carrying data
(any prior generator run). Verifies the sink drains all four topics into
Postgres, records offsets, and that a second fresh sink instance is a no-op:
the definition of idempotent.
"""

from __future__ import annotations

import psycopg
import pytest

from processors.pg_sink import PgSink, PgSinkConfig

pytestmark = pytest.mark.integration

DSN = "postgresql://stream:stream@localhost:5433/stream"

TABLES = (
    "raw.rides_events",
    "raw.ride_sessions",
    "raw.payments_transactions",
    "raw.driver_locations",
)


def _counts() -> dict[str, int]:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        result = {}
        for table in TABLES:
            cur.execute(f"SELECT count(*) FROM {table}")  # fixed identifiers, not user input
            result[table] = int(cur.fetchone()[0])
        return result


def test_sink_drains_topics_then_second_run_is_noop() -> None:
    sink = PgSink(PgSinkConfig())
    sink.run(idle_timeout_sec=10.0)
    first = _counts()
    assert first["raw.rides_events"] > 0
    assert first["raw.ride_sessions"] > 0
    assert first["raw.driver_locations"] > 0
    assert sink.rows_written > 0

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM serving.consumer_offsets WHERE consumer_group = 'pg-sink'"
        )
        assert int(cur.fetchone()[0]) > 0
        # No event ever landed twice: the unique index would have blown up the
        # sink, but count distinct proves it from the data side too.
        cur.execute("SELECT count(*), count(DISTINCT event_id) FROM raw.rides_events")
        total, distinct = cur.fetchone()
        assert total == distinct

    resumed = PgSink(PgSinkConfig())
    resumed.run(idle_timeout_sec=8.0)
    assert resumed.rows_written == 0, "second run must find nothing new"
    assert _counts() == first
