"""Idempotent Postgres sink: the exactly-once argument for a non-Kafka store.

THE CRUX: the offsets table row is ``(consumer_group, topic, partition,
kafka_offset)``, upserted in the same Postgres transaction as the data rows;
on startup the consumer seeks to stored offset + 1 and deliberately ignores
broker-committed offsets for this path. Kafka transactions cannot span an
external database, so the sink makes Postgres itself the system of record for
its own progress: data and position commit atomically or not at all. A crash
between Kafka delivery and the Postgres commit simply replays the batch, and
the idempotent upserts make the replay invisible:

- raw.rides_events upserts on ``(ride_id, event_seq)``, the deterministic
  lifecycle ordinal, so broker-level duplicates AND generator-injected
  duplicates (same event id re-sent) both collapse into one row;
- raw.ride_sessions upserts on ``ride_id``, keeping the richer close (higher
  event_seq) if a session is ever re-derived;
- payments and driver locations upsert on their natural keys.

Every topic here is read with isolation.level=read_committed because
rides.sessions is transactionally produced; reading uncommitted would surface
aborted sessionizer transactions and silently void the whole claim.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer
from confluent_kafka.serialization import MessageField, SerializationContext

from common import topics
from common.kafka import consumer_config
from common.logging import configure_logging, with_ctx
from common.schemas import (
    driver_location_deserializer,
    make_registry_client,
    payment_deserializer,
    ride_event_deserializer,
    ride_session_deserializer,
)
from common.times import to_epoch_ms
from generator.state_machine import EVENT_SEQ, RideEventType

PoisonHandler = Callable[[Any, Exception], None]

SINK_TOPICS = (
    topics.RIDES_EVENTS,
    topics.RIDES_SESSIONS,
    topics.PAYMENTS_TRANSACTIONS,
    topics.DRIVERS_LOCATIONS,
)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS serving;
CREATE TABLE IF NOT EXISTS raw.rides_events (
    ride_id text NOT NULL,
    event_seq int NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    event_ts timestamptz NOT NULL,
    rider_id text NOT NULL,
    driver_id text,
    city_id int NOT NULL,
    pickup_lat double precision NOT NULL,
    pickup_lon double precision NOT NULL,
    dropoff_lat double precision,
    dropoff_lon double precision,
    fare_cents bigint,
    surge_multiplier double precision NOT NULL,
    payload_version int NOT NULL,
    promo_code text,
    kafka_partition int NOT NULL,
    kafka_offset bigint NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ride_id, event_seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS rides_events_event_id ON raw.rides_events (event_id);
CREATE INDEX IF NOT EXISTS rides_events_ts ON raw.rides_events (event_ts);
CREATE TABLE IF NOT EXISTS raw.ride_sessions (
    ride_id text PRIMARY KEY,
    rider_id text NOT NULL,
    driver_id text,
    city_id int NOT NULL,
    terminal_state text NOT NULL,
    event_seq int NOT NULL,
    requested_ts timestamptz NOT NULL,
    matched_ts timestamptz,
    driver_arrived_ts timestamptz,
    started_ts timestamptz,
    ended_ts timestamptz,
    time_to_match_sec double precision,
    time_to_pickup_sec double precision,
    ride_duration_sec double precision,
    haversine_distance_km double precision,
    avg_speed_kmh double precision,
    is_late_arrival boolean NOT NULL,
    fare_cents bigint,
    surge_multiplier double precision NOT NULL,
    promo_code text,
    pickup_lat double precision NOT NULL,
    pickup_lon double precision NOT NULL,
    dropoff_lat double precision,
    dropoff_lon double precision,
    ingested_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ride_sessions_requested ON raw.ride_sessions (requested_ts);
CREATE TABLE IF NOT EXISTS raw.payments_transactions (
    txn_id text PRIMARY KEY,
    ride_id text NOT NULL,
    amount_cents bigint NOT NULL,
    status text NOT NULL,
    method text NOT NULL,
    ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS raw.driver_locations (
    driver_id text NOT NULL,
    ts timestamptz NOT NULL,
    lat double precision NOT NULL,
    lon double precision NOT NULL,
    speed_kmh double precision NOT NULL,
    heading double precision NOT NULL,
    status text NOT NULL,
    city_id int NOT NULL,
    PRIMARY KEY (driver_id, ts)
);
CREATE TABLE IF NOT EXISTS serving.consumer_offsets (
    consumer_group text NOT NULL,
    topic text NOT NULL,
    partition int NOT NULL,
    kafka_offset bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);
"""


def _dt(value: datetime | int | None) -> datetime | None:
    if value is None:
        return None
    ms = to_epoch_ms(value)
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def rides_event_row(value: dict[str, Any], partition: int, offset: int) -> tuple[Any, ...]:
    event_seq = EVENT_SEQ[RideEventType(value["event_type"])]
    return (
        value["ride_id"],
        event_seq,
        value["event_id"],
        value["event_type"],
        _dt(value["event_ts"]),
        value["rider_id"],
        value.get("driver_id"),
        value["city_id"],
        value["pickup_lat"],
        value["pickup_lon"],
        value.get("dropoff_lat"),
        value.get("dropoff_lon"),
        value.get("fare_cents"),
        value["surge_multiplier"],
        value.get("payload_version", 1),
        value.get("promo_code"),
        partition,
        offset,
    )


def session_row(value: dict[str, Any]) -> tuple[Any, ...]:
    return (
        value["ride_id"],
        value["rider_id"],
        value.get("driver_id"),
        value["city_id"],
        value["terminal_state"],
        value["event_seq"],
        _dt(value["requested_ts"]),
        _dt(value.get("matched_ts")),
        _dt(value.get("driver_arrived_ts")),
        _dt(value.get("started_ts")),
        _dt(value.get("ended_ts")),
        value.get("time_to_match_sec"),
        value.get("time_to_pickup_sec"),
        value.get("ride_duration_sec"),
        value.get("haversine_distance_km"),
        value.get("avg_speed_kmh"),
        value["is_late_arrival"],
        value.get("fare_cents"),
        value["surge_multiplier"],
        value.get("promo_code"),
        value["pickup_lat"],
        value["pickup_lon"],
        value.get("dropoff_lat"),
        value.get("dropoff_lon"),
    )


def payment_row(value: dict[str, Any]) -> tuple[Any, ...]:
    return (
        value["txn_id"],
        value["ride_id"],
        value["amount_cents"],
        value["status"],
        value["method"],
        _dt(value["ts"]),
    )


def location_row(value: dict[str, Any]) -> tuple[Any, ...]:
    return (
        value["driver_id"],
        _dt(value["ts"]),
        value["lat"],
        value["lon"],
        value["speed_kmh"],
        value["heading"],
        value["status"],
        value["city_id"],
    )


UPSERTS: dict[str, str] = {
    topics.RIDES_EVENTS: (
        "INSERT INTO raw.rides_events (ride_id, event_seq, event_id, event_type, event_ts,"
        " rider_id, driver_id, city_id, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon,"
        " fare_cents, surge_multiplier, payload_version, promo_code, kafka_partition,"
        " kafka_offset) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
        " %s, %s, %s) ON CONFLICT (ride_id, event_seq) DO NOTHING"
    ),
    topics.RIDES_SESSIONS: (
        "INSERT INTO raw.ride_sessions (ride_id, rider_id, driver_id, city_id, terminal_state,"
        " event_seq, requested_ts, matched_ts, driver_arrived_ts, started_ts, ended_ts,"
        " time_to_match_sec, time_to_pickup_sec, ride_duration_sec, haversine_distance_km,"
        " avg_speed_kmh, is_late_arrival, fare_cents, surge_multiplier, promo_code, pickup_lat,"
        " pickup_lon, dropoff_lat, dropoff_lon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
        " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        " ON CONFLICT (ride_id) DO UPDATE SET"
        " terminal_state = EXCLUDED.terminal_state, event_seq = EXCLUDED.event_seq,"
        " ended_ts = EXCLUDED.ended_ts, time_to_match_sec = EXCLUDED.time_to_match_sec,"
        " time_to_pickup_sec = EXCLUDED.time_to_pickup_sec,"
        " ride_duration_sec = EXCLUDED.ride_duration_sec,"
        " haversine_distance_km = EXCLUDED.haversine_distance_km,"
        " avg_speed_kmh = EXCLUDED.avg_speed_kmh, is_late_arrival = EXCLUDED.is_late_arrival,"
        " fare_cents = EXCLUDED.fare_cents, promo_code = EXCLUDED.promo_code"
        " WHERE EXCLUDED.event_seq > raw.ride_sessions.event_seq"
    ),
    topics.PAYMENTS_TRANSACTIONS: (
        "INSERT INTO raw.payments_transactions (txn_id, ride_id, amount_cents, status, method,"
        " ts) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (txn_id) DO NOTHING"
    ),
    topics.DRIVERS_LOCATIONS: (
        "INSERT INTO raw.driver_locations (driver_id, ts, lat, lon, speed_kmh, heading, status,"
        " city_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (driver_id, ts) DO NOTHING"
    ),
}

OFFSET_UPSERT = (
    "INSERT INTO serving.consumer_offsets (consumer_group, topic, partition, kafka_offset,"
    " updated_at) VALUES (%s, %s, %s, %s, now())"
    " ON CONFLICT (consumer_group, topic, partition) DO UPDATE SET"
    " kafka_offset = GREATEST(EXCLUDED.kafka_offset, serving.consumer_offsets.kafka_offset),"
    " updated_at = now()"
)

ROW_BUILDERS: dict[str, Callable[[dict[str, Any], int, int], tuple[Any, ...]]] = {
    topics.RIDES_EVENTS: rides_event_row,
    topics.RIDES_SESSIONS: lambda value, partition, offset: session_row(value),
    topics.PAYMENTS_TRANSACTIONS: lambda value, partition, offset: payment_row(value),
    topics.DRIVERS_LOCATIONS: lambda value, partition, offset: location_row(value),
}


@dataclass
class PgSinkConfig:
    bootstrap: str = "localhost:19092"
    registry_url: str = "http://localhost:18081"
    dsn: str = "postgresql://stream:stream@localhost:5433/stream"
    group_id: str = "pg-sink"
    batch_max: int = 2000
    poll_timeout_sec: float = 1.0

    @classmethod
    def from_env(cls) -> PgSinkConfig:
        return cls(
            bootstrap=os.environ.get("KAFKA_BOOTSTRAP", cls.bootstrap),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", cls.registry_url),
            dsn=os.environ.get("POSTGRES_DSN", cls.dsn),
        )


def build_deserializers(registry_url: str) -> dict[str, Callable[[str, bytes], dict[str, Any]]]:
    client = make_registry_client(registry_url)
    per_topic = {
        topics.RIDES_EVENTS: ride_event_deserializer(client),
        topics.RIDES_SESSIONS: ride_session_deserializer(client),
        topics.PAYMENTS_TRANSACTIONS: payment_deserializer(client),
        topics.DRIVERS_LOCATIONS: driver_location_deserializer(client),
    }

    def wrap(deser: Any) -> Callable[[str, bytes], dict[str, Any]]:
        def call(topic: str, payload: bytes) -> dict[str, Any]:
            value = deser(payload, SerializationContext(topic, MessageField.VALUE))
            if not isinstance(value, dict):
                raise TypeError(f"expected record dict, got {type(value)}")
            return value

        return call

    return {topic: wrap(deser) for topic, deser in per_topic.items()}


class PgSink:
    def __init__(
        self,
        cfg: PgSinkConfig,
        *,
        consumer: Any | None = None,
        deserializers: dict[str, Callable[[str, bytes], dict[str, Any]]] | None = None,
        connection: Any | None = None,
        on_poison: PoisonHandler | None = None,
    ) -> None:
        self._cfg = cfg
        self._log = configure_logging("pg-sink")
        self._conn = connection if connection is not None else psycopg.connect(cfg.dsn)
        with self._conn.cursor() as cur:
            cur.execute(DDL)
        self._conn.commit()
        self._consumer = (
            consumer
            if consumer is not None
            else Consumer(consumer_config(cfg.bootstrap, cfg.group_id))
        )
        self._deserializers = (
            deserializers if deserializers is not None else build_deserializers(cfg.registry_url)
        )
        self._on_poison: PoisonHandler = on_poison if on_poison is not None else self._log_poison
        self.rows_written = 0
        self.poison_seen = 0

    def _log_poison(self, msg: Any, error: Exception) -> None:
        self._log.warning(
            "poison message skipped (DLQ producer attaches at the DLQ milestone)",
            extra=with_ctx(
                topic=msg.topic(), partition=msg.partition(), offset=msg.offset(), error=str(error)
            ),
        )

    # ---------------------------------------------------------------- offsets

    def stored_offsets(self, partitions: list[Any]) -> dict[tuple[str, int], int]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT topic, partition, kafka_offset FROM serving.consumer_offsets"
                " WHERE consumer_group = %s",
                (self._cfg.group_id,),
            )
            stored = {(topic, part): offset for topic, part, offset in cur.fetchall()}
        self._conn.commit()
        return {
            (tp.topic, tp.partition): stored.get((tp.topic, tp.partition), -1) for tp in partitions
        }

    def on_assign(self, consumer: Any, partitions: list[Any]) -> None:
        """Seek every partition to its Postgres-stored offset + 1. Broker group
        offsets are deliberately ignored: Postgres is the system of record."""
        stored = self.stored_offsets(partitions)
        for tp in partitions:
            last = stored[(tp.topic, tp.partition)]
            tp.offset = last + 1 if last >= 0 else OFFSET_BEGINNING
        consumer.assign(partitions)
        self._log.info(
            "assigned with pg-stored offsets",
            extra=with_ctx(
                positions={f"{t}[{p}]": o for (t, p), o in stored.items()},
            ),
        )

    # ------------------------------------------------------------------- loop

    def run(self, max_batches: int | None = None, idle_timeout_sec: float | None = None) -> None:
        """Consume forever, or until max_batches non-empty batches, or until the
        topics have been quiet for idle_timeout_sec (bounded runs in tests)."""
        self._consumer.subscribe(list(SINK_TOPICS), on_assign=self.on_assign)
        batches = 0
        last_data = time.monotonic()
        self._log.info("pg-sink running", extra=with_ctx(group=self._cfg.group_id))
        try:
            while max_batches is None or batches < max_batches:
                msgs = self._consumer.consume(self._cfg.batch_max, self._cfg.poll_timeout_sec)
                if not msgs:
                    if idle_timeout_sec is not None and (
                        time.monotonic() - last_data > idle_timeout_sec
                    ):
                        break
                    continue
                last_data = time.monotonic()
                self.process_batch(msgs)
                batches += 1
        finally:
            self._consumer.close()

    def process_batch(self, msgs: list[Any]) -> dict[str, int]:
        rows: dict[str, list[tuple[Any, ...]]] = {topic: [] for topic in SINK_TOPICS}
        max_offsets: dict[tuple[str, int], int] = {}
        for msg in msgs:
            if msg.error() is not None:
                raise RuntimeError(f"consumer error: {msg.error()}")
            topic = msg.topic()
            key = (topic, msg.partition())
            max_offsets[key] = max(max_offsets.get(key, -1), msg.offset())
            try:
                value = self._deserializers[topic](topic, msg.value())
                rows[topic].append(ROW_BUILDERS[topic](value, msg.partition(), msg.offset()))
            except Exception as error:  # poison messages take the DLQ path
                self.poison_seen += 1
                self._on_poison(msg, error)
        written = {topic: len(batch) for topic, batch in rows.items() if batch}
        try:
            with self._conn.cursor() as cur:
                for topic, batch in rows.items():
                    if batch:
                        cur.executemany(UPSERTS[topic], batch)
                cur.executemany(
                    OFFSET_UPSERT,
                    [
                        (self._cfg.group_id, topic, partition, offset)
                        for (topic, partition), offset in max_offsets.items()
                    ],
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        self.rows_written += sum(written.values())
        return written


def main() -> int:
    sink = PgSink(PgSinkConfig.from_env())
    sink.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
