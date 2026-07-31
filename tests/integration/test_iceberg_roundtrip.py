"""Iceberg REST catalog round trip: append, time travel, live schema evolution.

Runs against the compose stack's apache/iceberg-rest-fixture + MinIO. This is
the compatibility gate the spec demands before trusting the sink: catalog and
client versions actually creating, evolving, and time-travelling a table.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import IntegerType, NestedField, StringType, TimestamptzType

from sinks.iceberg_sink import IcebergSinkConfig, open_catalog

pytestmark = pytest.mark.integration


def _batch(ids: list[str], day: int) -> pa.Table:
    return pa.table(
        {
            "event_id": ids,
            "city_id": [1] * len(ids),
            "event_ts": [datetime(2026, 1, day, tzinfo=UTC)] * len(ids),
        },
        schema=pa.schema(
            [
                pa.field("event_id", pa.string()),
                pa.field("city_id", pa.int32()),
                pa.field("event_ts", pa.timestamp("us", tz="UTC")),
            ]
        ),
    )


def test_catalog_roundtrip_time_travel_and_evolution() -> None:
    catalog = open_catalog(IcebergSinkConfig())
    catalog.create_namespace_if_not_exists("it")
    name = f"it.rt_{uuid.uuid4().hex[:8]}"
    schema = Schema(
        NestedField(1, "event_id", StringType(), required=False),
        NestedField(2, "city_id", IntegerType(), required=False),
        NestedField(3, "event_ts", TimestamptzType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="event_day")
    )
    table = catalog.create_table(name, schema=schema, partition_spec=spec)
    try:
        table.append(_batch(["a", "b"], day=1))
        first_snapshot = table.snapshots()[0].snapshot_id
        table.append(_batch(["c"], day=2))

        assert table.scan().to_arrow().num_rows == 3
        old = table.scan(snapshot_id=first_snapshot).to_arrow()
        assert old.num_rows == 2, "time travel must see only the first snapshot"

        with table.update_schema() as update:
            update.add_column("promo_code", StringType())
        evolved = table.scan().to_arrow()
        assert evolved.num_rows == 3
        assert "promo_code" in evolved.column_names
        assert evolved.column("promo_code").null_count == 3, "old files read with null new column"
        # Pre-evolution snapshot still readable after the schema change.
        assert table.scan(snapshot_id=first_snapshot).to_arrow().num_rows == 2
    finally:
        catalog.drop_table(name)
