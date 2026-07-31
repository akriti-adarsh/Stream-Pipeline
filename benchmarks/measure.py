"""Measure sustained throughput and end-to-end latency on the LIVE platform.

Run with the full compose profile up and the containerised services flowing.
Two measurements, both against real systems, no synthetic shortcuts:

1. Sustained throughput over a window (default 60 s): broker-side produced
   messages per second (high-watermark deltas across all source topics, the
   broker's own truth) and Postgres-side ingested rows per second (raw table
   count deltas: what actually landed, after dedup).

2. End-to-end produce-to-queryable latency: marker ride events are produced
   one at a time from this host with a wall-clock send stamp; each is polled
   for in raw.rides_events until visible. The reported quantiles are
   wall-clock seconds from broker ack to SQL visibility through the real
   consumer (pg-sink batch loop included). Markers ride the normal topic
   with valid payloads; they are indistinguishable from traffic downstream.

Writes benchmarks/results/measure-<utcstamp>.json (committed artifact: every
number in the README traces here).
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, TopicPartition

from common import topics
from common.kafka import consumer_config
from generator.events import ride_event
from generator.producer import KafkaTransport
from generator.state_machine import RideEventType

SOURCE_TOPICS = (topics.RIDES_EVENTS, topics.DRIVERS_LOCATIONS, topics.PAYMENTS_TRANSACTIONS)
RAW_COUNT_SQL = (
    "SELECT (SELECT count(*) FROM raw.rides_events)"
    " + (SELECT count(*) FROM raw.driver_locations)"
    " + (SELECT count(*) FROM raw.payments_transactions)"
    " + (SELECT count(*) FROM raw.ride_sessions)"
)


def topic_high_watermarks(bootstrap: str) -> int:
    consumer = Consumer(consumer_config(bootstrap, f"measure-{uuid.uuid4().hex[:6]}"))
    total = 0
    try:
        metadata = consumer.list_topics(timeout=10)
        for topic in SOURCE_TOPICS:
            for partition_id in metadata.topics[topic].partitions:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id), timeout=10
                )
                total += high
    finally:
        consumer.close()
    return total


def measure_throughput(bootstrap: str, dsn: str, window_sec: float) -> dict[str, float]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(RAW_COUNT_SQL)
        pg_start = int(cur.fetchone()[0])
    broker_start = topic_high_watermarks(bootstrap)
    t_start = time.monotonic()
    time.sleep(window_sec)
    elapsed = time.monotonic() - t_start
    broker_end = topic_high_watermarks(bootstrap)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(RAW_COUNT_SQL)
        pg_end = int(cur.fetchone()[0])
    return {
        "window_sec": round(elapsed, 2),
        "broker_msgs_per_sec": round((broker_end - broker_start) / elapsed, 1),
        "pg_rows_per_sec": round((pg_end - pg_start) / elapsed, 1),
        "broker_msgs_in_window": broker_end - broker_start,
        "pg_rows_in_window": pg_end - pg_start,
    }


def measure_latency(bootstrap: str, registry: str, dsn: str, markers: int) -> dict[str, object]:
    transport = KafkaTransport(bootstrap, registry, initialise=False)
    samples: list[float] = []
    conn = psycopg.connect(dsn)
    try:
        for _ in range(markers):
            ride_id = f"bench-{uuid.uuid4().hex[:10]}"
            event = ride_event(
                ride_id=ride_id,
                event_type=RideEventType.REQUESTED,
                event_ts_ms=int(time.time() * 1000),
                rider_id="u-bench",
                city_id=1,
                pickup=(12.9716, 77.5946),
                dropoff=(12.9352, 77.6245),
                surge_multiplier=1.0,
            )
            transport.send(event)
            transport.flush(10.0)
            sent_at = time.monotonic()
            event_id = f"{ride_id}.1"
            deadline = sent_at + 60.0
            seen: float | None = None
            with conn.cursor() as cur:
                while time.monotonic() < deadline:
                    cur.execute("SELECT 1 FROM raw.rides_events WHERE event_id = %s", (event_id,))
                    if cur.fetchone() is not None:
                        seen = time.monotonic() - sent_at
                        break
                    time.sleep(0.05)
            conn.commit()
            if seen is None:
                raise RuntimeError(f"marker {event_id} never became visible within 60s")
            samples.append(seen)
            time.sleep(1.0)
    finally:
        conn.close()
        transport.close()
    samples_sorted = sorted(samples)
    return {
        "markers": markers,
        "p50_sec": round(statistics.median(samples_sorted), 3),
        "p95_sec": round(samples_sorted[max(int(len(samples_sorted) * 0.95) - 1, 0)], 3),
        "max_sec": round(samples_sorted[-1], 3),
        "samples_sec": [round(sample, 3) for sample in samples],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--registry", default="http://localhost:18081")
    parser.add_argument("--dsn", default="postgresql://stream:stream@localhost:5433/stream")
    parser.add_argument("--window", type=float, default=60.0)
    parser.add_argument("--markers", type=int, default=30)
    args = parser.parse_args()

    result = {
        "measured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "profile": "full (containerised services)",
        "throughput": measure_throughput(args.bootstrap, args.dsn, args.window),
        "latency_produce_to_queryable": measure_latency(
            args.bootstrap, args.registry, args.dsn, args.markers
        ),
    }
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"measure-{stamp}.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"\nwritten: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
