"""The full poison-message cycle: corrupt -> DLQ -> repair -> replay -> processed.

Uses the platform's canonical poison (flipped Confluent magic byte), the real
sessionizer as the detecting consumer, and the real replay tool with its
--repair-magic-byte transform. The proof of "processed" is the repaired
events landing in Postgres through the ordinary pipeline.
"""

from __future__ import annotations

import sys
import uuid
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest

from common import topics
from generator.events import ride_event
from generator.producer import KafkaTransport
from generator.state_machine import RideEventType
from processors.pg_sink import PgSink, PgSinkConfig
from processors.ride_sessionizer import Sessionizer, SessionizerConfig

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from confluent_kafka import Consumer

from common.kafka import consumer_config
from replay_dlq import drain_dlq
from replay_dlq import main as replay_main

pytestmark = [pytest.mark.integration, pytest.mark.timeout(420)]

DSN = "postgresql://stream:stream@localhost:5433/stream"
BOOTSTRAP = "localhost:19092"


def _pg_has_events(event_ids: set[str]) -> set[str]:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT event_id FROM raw.rides_events WHERE event_id = ANY(%s)",
            (sorted(event_ids),),
        )
        return {row[0] for row in cur.fetchall()}


def test_poison_dlq_repair_replay_cycle(tmp_path: Path) -> None:
    marker = f"replaytest{uuid.uuid4().hex[:8]}"
    ride_ids = [f"{marker}-{n}" for n in range(5)]
    base_ts = 1_767_225_600_000

    transport = KafkaTransport(BOOTSTRAP, "http://localhost:18081")
    events = [
        ride_event(
            ride_id=ride_id,
            event_type=RideEventType.REQUESTED,
            event_ts_ms=base_ts + n,
            rider_id="u-replay",
            city_id=1,
            pickup=(12.97, 77.59),
            surge_multiplier=1.0,
        )
        for n, ride_id in enumerate(ride_ids)
    ]
    for event in events:
        transport.send(replace(event, corrupt=True))
    transport.close()
    assert transport.corrupted == 5

    # 1. The sessionizer must divert all five to the DLQ.
    sess = Sessionizer(SessionizerConfig(state_path=str(tmp_path / "s.db")))
    sess.run(idle_timeout_sec=10.0)
    assert sess.poison_seen >= 5

    consumer = Consumer(consumer_config(BOOTSTRAP, f"dlq-check-{marker}"))
    try:
        envelopes = drain_dlq(consumer, topics.RIDES_EVENTS, idle_polls=3)
    finally:
        consumer.close()
    ours = [e for e in envelopes if e.key is not None and e.key.decode().startswith(marker)]
    assert len(ours) >= 5, f"expected our 5 poison envelopes in the DLQ, saw {len(ours)}"
    assert all(e.payload[0] == 0xFF for e in ours), "envelopes must carry the corrupt bytes"

    # 2. Replay with the magic-byte repair, filtered to exactly our messages.
    exit_code = replay_main(
        [
            "--topic",
            topics.RIDES_EVENTS,
            "--bootstrap",
            BOOTSTRAP,
            "--repair-magic-byte",
            "--filter",
            f"e.key is not None and e.key.decode().startswith('{marker}')",
        ]
    )
    assert exit_code == 0

    # 3. The repaired originals flow through the NORMAL pipeline into Postgres.
    sink = PgSink(PgSinkConfig())
    sink.run(idle_timeout_sec=10.0)
    expected_ids = {f"{ride_id}.1" for ride_id in ride_ids}
    landed = _pg_has_events(expected_ids)
    assert landed == expected_ids, f"repaired events missing from Postgres: {expected_ids - landed}"
