"""Late data end to end: a ride injected 10 minutes behind the watermark must
surface in late.events (the Flink side output) AND still reach fct_rides
through the incremental model's lookback window.

Requires the full profile: the Flink jobs must be RUNNING (they are brought up
by `docker compose --profile full up`).
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

import psycopg
import pytest
from confluent_kafka import Consumer, TopicPartition

from common import topics
from common.kafka import consumer_config
from generator.events import ride_event
from generator.producer import KafkaTransport
from generator.state_machine import RideEventType
from processors.pg_sink import PgSink, PgSinkConfig
from processors.ride_sessionizer import Sessionizer, SessionizerConfig

pytestmark = [pytest.mark.integration, pytest.mark.timeout(420)]

DSN = "postgresql://stream:stream@localhost:5433/stream"
BOOTSTRAP = "localhost:19092"
REPO = Path(__file__).resolve().parents[2]


def _flink_jobs_running() -> bool:
    try:
        with urllib.request.urlopen("http://localhost:18083/jobs/overview", timeout=5) as resp:
            jobs = json.load(resp)["jobs"]
        return any(
            j["name"] == "city-metrics-and-late-output" and j["state"] == "RUNNING" for j in jobs
        )
    except Exception:
        return False


def _watermark_anchor_ms() -> int:
    """Approximate the Flink watermark: the newest event time already ingested."""
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT extract(epoch from max(event_ts)) * 1000 FROM raw.rides_events")
        value = cur.fetchone()[0]
    assert value is not None, "no prior events; run the platform before this test"
    return int(value)


def _late_ride_events(ride_id: str, t0: int) -> list:
    stages = [
        (RideEventType.REQUESTED, t0),
        (RideEventType.MATCHED, t0 + 20_000),
        (RideEventType.DRIVER_ARRIVED, t0 + 120_000),
        (RideEventType.STARTED, t0 + 150_000),
        (RideEventType.COMPLETED, t0 + 400_000),
    ]
    return [
        ride_event(
            ride_id=ride_id,
            event_type=kind,
            event_ts_ms=ts,
            rider_id="u-late",
            city_id=2,
            pickup=(19.07, 72.87),
            dropoff=(19.01, 72.85),
            surge_multiplier=1.0,
            driver_id="d-2-9999" if kind is not RideEventType.REQUESTED else None,
            fare_cents=12_345 if kind is RideEventType.COMPLETED else None,
        )
        for kind, ts in stages
    ]


def test_ten_minute_late_ride_hits_side_output_and_dbt_lookback(tmp_path: Path) -> None:
    if not _flink_jobs_running():
        pytest.skip("Flink city-metrics job not running; bring up the full profile first")

    ride_id = f"late{uuid.uuid4().hex[:10]}"
    t0 = _watermark_anchor_ms() - 600_000  # ten minutes behind the newest event time

    # Tail late.events from its CURRENT end so only new arrivals count.
    tail = Consumer(consumer_config(BOOTSTRAP, f"late-tail-{ride_id}"))
    tp = TopicPartition(topics.LATE_EVENTS, 0)
    _low, high = tail.get_watermark_offsets(tp, timeout=10)
    tp.offset = high
    tail.assign([tp])

    transport = KafkaTransport(BOOTSTRAP, "http://localhost:18081")
    for event in _late_ride_events(ride_id, t0):
        transport.send(event)
    transport.close()

    # The sessionizer forwards them to the clean mirror (Flink's source) and
    # closes the ride; both happen in its transaction.
    sess = Sessionizer(SessionizerConfig(state_path=str(tmp_path / "s.db")))
    sess.run(idle_timeout_sec=10.0)

    found = None
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline and found is None:
            msg = tail.poll(2.0)
            if msg is None or msg.error() is not None:
                continue
            value = json.loads(msg.value())
            if value.get("ride_id") == ride_id:
                found = value
    finally:
        tail.close()
    assert found is not None, "late ride never appeared in late.events"
    assert int(found["late_by_sec"]) >= 500, found

    # And the dbt lookback picks the late session up into the incremental fact.
    sink = PgSink(PgSinkConfig())
    sink.run(idle_timeout_sec=10.0)
    build = subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--select",
            "fct_rides",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert build.returncode == 0, build.stdout[-2000:]
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT terminal_state, fare_cents FROM analytics_marts.fct_rides WHERE ride_id = %s",
            (ride_id,),
        )
        row = cur.fetchone()
    assert row is not None, "late ride missing from fct_rides: the lookback failed"
    assert row[0] == "completed"
    assert row[1] == 12_345
