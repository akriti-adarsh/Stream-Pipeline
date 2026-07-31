"""Iceberg lakehouse sink: rides.events batched into a partitioned table on MinIO.

Consumes the RAW rides.events topic (read_committed), diverts poison to the
DLQ exactly like the other Python consumers, batches clean records, and
appends them to ``lakehouse.rides_events`` via the PyIceberg REST catalog.
The table is partitioned by ``days(event_ts)`` and ``city_id``.

Delivery contract: AT-LEAST-ONCE. Broker offsets are committed only after a
successful append, so a crash between append and commit replays the batch and
can duplicate rows in the lakehouse. That is a deliberate trade-off, spelled
out here because the spec demands honesty about it: the exactly-once budget
was spent on the Postgres serving path (offsets-in-transaction plus idempotent
upserts), where duplicates would corrupt marts and dashboards. The lakehouse
is the raw historical archive; consumers of it deduplicate on event_id when
exactness matters, and the append-only snapshot log makes any replay visible
and reversible (time travel to the pre-replay snapshot).

Schema evolution, live: the table is created WITHOUT promo_code (the v1
world). When the first payload_version >= 2 record arrives, the sink evolves
the table schema in place (add_column) and starts writing the new column, no
redeployment. Files written before the evolution keep reading cleanly, which
the integration suite proves via time travel.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from confluent_kafka import Consumer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from common import topics
from common.kafka import consumer_config
from common.logging import configure_logging, with_ctx
from common.schemas import make_registry_client, ride_event_deserializer
from common.times import to_epoch_ms
from dlq.envelope import DlqProducer

PoisonHandler = Callable[[Any, Exception], None]

NAMESPACE = "lakehouse"
TABLE_NAME = "lakehouse.rides_events"

ICEBERG_SCHEMA_V1 = Schema(
    NestedField(1, "event_id", StringType(), required=False),
    NestedField(2, "ride_id", StringType(), required=False),
    NestedField(3, "event_type", StringType(), required=False),
    NestedField(4, "event_ts", TimestamptzType(), required=False),
    NestedField(5, "rider_id", StringType(), required=False),
    NestedField(6, "driver_id", StringType(), required=False),
    NestedField(7, "city_id", IntegerType(), required=False),
    NestedField(8, "pickup_lat", DoubleType(), required=False),
    NestedField(9, "pickup_lon", DoubleType(), required=False),
    NestedField(10, "dropoff_lat", DoubleType(), required=False),
    NestedField(11, "dropoff_lon", DoubleType(), required=False),
    NestedField(12, "fare_cents", LongType(), required=False),
    NestedField(13, "surge_multiplier", DoubleType(), required=False),
    NestedField(14, "payload_version", IntegerType(), required=False),
    NestedField(15, "is_synthetic", BooleanType(), required=False),
)

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="event_day"),
    PartitionField(source_id=7, field_id=1001, transform=IdentityTransform(), name="city_id_part"),
)

_BASE_COLUMNS: list[tuple[str, pa.DataType]] = [
    ("event_id", pa.string()),
    ("ride_id", pa.string()),
    ("event_type", pa.string()),
    ("event_ts", pa.timestamp("us", tz="UTC")),
    ("rider_id", pa.string()),
    ("driver_id", pa.string()),
    ("city_id", pa.int32()),
    ("pickup_lat", pa.float64()),
    ("pickup_lon", pa.float64()),
    ("dropoff_lat", pa.float64()),
    ("dropoff_lon", pa.float64()),
    ("fare_cents", pa.int64()),
    ("surge_multiplier", pa.float64()),
    ("payload_version", pa.int32()),
    ("is_synthetic", pa.bool_()),
]


def arrow_schema(with_promo: bool) -> pa.Schema:
    fields = [pa.field(name, dtype) for name, dtype in _BASE_COLUMNS]
    if with_promo:
        fields.append(pa.field("promo_code", pa.string()))
    return pa.schema(fields)


def should_evolve(values: list[dict[str, Any]], already_evolved: bool) -> bool:
    """Evolve exactly once, on the first sight of a v2 payload."""
    if already_evolved:
        return False
    return any(int(value.get("payload_version", 1)) >= 2 for value in values)


def to_arrow(values: list[dict[str, Any]], with_promo: bool) -> pa.Table:
    """Shape deserialized events into an Arrow batch matching the table schema."""
    rows: dict[str, list[Any]] = {name: [] for name, _ in _BASE_COLUMNS}
    if with_promo:
        rows["promo_code"] = []
    for value in values:
        rows["event_id"].append(str(value["event_id"]))
        rows["ride_id"].append(str(value["ride_id"]))
        rows["event_type"].append(str(value["event_type"]))
        rows["event_ts"].append(
            datetime.fromtimestamp(to_epoch_ms(value["event_ts"]) / 1000, tz=UTC)
        )
        rows["rider_id"].append(str(value["rider_id"]))
        rows["driver_id"].append(value.get("driver_id"))
        rows["city_id"].append(int(value["city_id"]))
        rows["pickup_lat"].append(float(value["pickup_lat"]))
        rows["pickup_lon"].append(float(value["pickup_lon"]))
        rows["dropoff_lat"].append(value.get("dropoff_lat"))
        rows["dropoff_lon"].append(value.get("dropoff_lon"))
        rows["fare_cents"].append(value.get("fare_cents"))
        rows["surge_multiplier"].append(float(value["surge_multiplier"]))
        rows["payload_version"].append(int(value.get("payload_version", 1)))
        rows["is_synthetic"].append(True)
        if with_promo:
            rows["promo_code"].append(value.get("promo_code"))
    return pa.table(rows, schema=arrow_schema(with_promo))


@dataclass
class IcebergSinkConfig:
    bootstrap: str = "localhost:19092"
    registry_url: str = "http://localhost:18081"
    rest_uri: str = "http://localhost:18181"
    s3_endpoint: str = "http://localhost:19000"
    s3_key: str = "minioadmin"
    s3_secret: str = "minioadmin"
    group_id: str = "iceberg-sink"
    batch_max_records: int = 2000
    flush_interval_sec: float = 10.0
    poll_timeout_sec: float = 1.0

    @classmethod
    def from_env(cls) -> IcebergSinkConfig:
        return cls(
            bootstrap=os.environ.get("KAFKA_BOOTSTRAP", cls.bootstrap),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", cls.registry_url),
            rest_uri=os.environ.get("ICEBERG_REST_URI", cls.rest_uri),
            s3_endpoint=os.environ.get("ICEBERG_S3_ENDPOINT", cls.s3_endpoint),
        )


def open_catalog(cfg: IcebergSinkConfig) -> Catalog:
    return load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": cfg.rest_uri,
            "s3.endpoint": cfg.s3_endpoint,
            "s3.access-key-id": cfg.s3_key,
            "s3.secret-access-key": cfg.s3_secret,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


class IcebergSink:
    def __init__(
        self,
        cfg: IcebergSinkConfig,
        *,
        consumer: Any | None = None,
        deserialize: Callable[[str, bytes], dict[str, Any]] | None = None,
        catalog: Catalog | None = None,
        on_poison: PoisonHandler | None = None,
    ) -> None:
        self._cfg = cfg
        self._log = configure_logging("iceberg-sink")
        self._catalog = catalog if catalog is not None else open_catalog(cfg)
        self._table = self._ensure_table()
        self._has_promo = any(f.name == "promo_code" for f in self._table.schema().fields)
        self._consumer = (
            consumer
            if consumer is not None
            else Consumer(consumer_config(cfg.bootstrap, cfg.group_id))
        )
        if deserialize is None:
            client = make_registry_client(cfg.registry_url)
            avro_deser = ride_event_deserializer(client)

            def _deserialize(topic: str, payload: bytes) -> dict[str, Any]:
                value = avro_deser(payload, SerializationContext(topic, MessageField.VALUE))
                assert isinstance(value, dict)
                return value

            deserialize = _deserialize
        self._deserialize = deserialize
        self._dlq: DlqProducer | None = None
        if on_poison is not None:
            self._on_poison: PoisonHandler = on_poison
        else:
            self._dlq = DlqProducer(cfg.bootstrap, cfg.group_id)
            self._on_poison = self._dlq.handle
        self._buffer: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()
        self.rows_appended = 0
        self.poison_seen = 0
        self.evolutions = 0

    def _ensure_table(self) -> Table:
        self._catalog.create_namespace_if_not_exists(NAMESPACE)
        try:
            return self._catalog.load_table(TABLE_NAME)
        except NoSuchTableError:
            self._log.info("creating lakehouse table", extra=with_ctx(table=TABLE_NAME))
            return self._catalog.create_table(
                TABLE_NAME, schema=ICEBERG_SCHEMA_V1, partition_spec=PARTITION_SPEC
            )

    # ------------------------------------------------------------------- loop

    def run(self, max_batches: int | None = None, idle_timeout_sec: float | None = None) -> None:
        self._consumer.subscribe([topics.RIDES_EVENTS])
        batches = 0
        last_data = time.monotonic()
        self._log.info("iceberg sink running", extra=with_ctx(group=self._cfg.group_id))
        try:
            while max_batches is None or batches < max_batches:
                msgs = self._consumer.consume(500, self._cfg.poll_timeout_sec)
                if msgs:
                    last_data = time.monotonic()
                    self._ingest(msgs)
                    batches += 1
                elif idle_timeout_sec is not None and (
                    time.monotonic() - last_data > idle_timeout_sec
                ):
                    break
                if self._should_flush():
                    self.flush()
            self.flush()
        finally:
            if self._dlq is not None:
                self._dlq.flush()
            self._consumer.close()

    def _ingest(self, msgs: list[Any]) -> None:
        for msg in msgs:
            if msg.error() is not None:
                raise RuntimeError(f"consumer error: {msg.error()}")
            try:
                value = self._deserialize(msg.topic(), msg.value())
            except Exception as error:  # poison -> DLQ, exactly like the other consumers
                self.poison_seen += 1
                self._on_poison(msg, error)
                continue
            self._buffer.append(value)

    def _should_flush(self) -> bool:
        if len(self._buffer) >= self._cfg.batch_max_records:
            return True
        return bool(self._buffer) and (
            time.monotonic() - self._last_flush > self._cfg.flush_interval_sec
        )

    def flush(self) -> None:
        if not self._buffer:
            return
        if should_evolve(self._buffer, self._has_promo):
            with self._table.update_schema() as update:
                update.add_column("promo_code", StringType())
            self._has_promo = True
            self.evolutions += 1
            self._log.info("schema evolved in place", extra=with_ctx(column="promo_code"))
        batch = to_arrow(self._buffer, self._has_promo)
        self._table.append(batch)
        # Offsets commit only AFTER the append: at-least-once, never data loss.
        self._consumer.commit(asynchronous=False)
        self.rows_appended += batch.num_rows
        self._log.info(
            "appended batch",
            extra=with_ctx(rows=batch.num_rows, total=self.rows_appended),
        )
        self._buffer = []
        self._last_flush = time.monotonic()


def main() -> int:
    IcebergSink(IcebergSinkConfig.from_env()).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
