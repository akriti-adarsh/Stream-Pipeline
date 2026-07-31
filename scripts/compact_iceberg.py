"""Compact small files in the Iceberg lakehouse table, one partition at a time.

PyIceberg 0.11 has no first-class rewrite-data-files operation, so compaction
is implemented per spec review round 2: for every partition holding more than
``--min-files`` data files, read the partition and transactionally OVERWRITE
exactly that partition with the same rows rewritten as fewer files. Row counts
are asserted equal before the overwrite is considered a success, before/after
file counts and byte sizes are logged from real catalog metadata, and every
pre-compaction snapshot remains readable via time travel (the overwrite only
adds a new snapshot).

Usage:
    uv run python scripts/compact_iceberg.py [--min-files 4] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from datetime import time as dtime
from typing import Any

from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

from common.logging import configure_logging, with_ctx
from sinks.iceberg_sink import TABLE_NAME, IcebergSinkConfig, open_catalog


def partition_inventory(table: Any) -> dict[tuple[str, int], dict[str, int]]:
    """(event_day_iso, city_id) -> {files, bytes, rows} from catalog metadata."""
    inventory: dict[tuple[str, int], dict[str, int]] = defaultdict(
        lambda: {"files": 0, "bytes": 0, "rows": 0}
    )
    files = table.inspect.files()
    for row in files.to_pylist():
        partition = row["partition"]
        key = (str(partition["event_day"]), int(partition["city_id_part"]))
        inventory[key]["files"] += 1
        inventory[key]["bytes"] += int(row["file_size_in_bytes"])
        inventory[key]["rows"] += int(row["record_count"])
    return dict(inventory)


def day_filter(day_iso: str, city_id: int) -> Any:
    day = datetime.combine(datetime.fromisoformat(day_iso).date(), dtime(0), tzinfo=UTC)
    next_day = day + timedelta(days=1)
    return And(
        And(
            GreaterThanOrEqual("event_ts", day.isoformat()),
            LessThan("event_ts", next_day.isoformat()),
        ),
        EqualTo("city_id", city_id),
    )


def compact(min_files: int, dry_run: bool) -> dict[str, Any]:
    log = configure_logging("iceberg-compaction")
    catalog = open_catalog(IcebergSinkConfig.from_env())
    table = catalog.load_table(TABLE_NAME)

    before = partition_inventory(table)
    candidates = {key: stats for key, stats in before.items() if stats["files"] > min_files}
    log.info(
        "compaction scan",
        extra=with_ctx(
            partitions=len(before),
            candidates=len(candidates),
            min_files=min_files,
            dry_run=dry_run,
        ),
    )
    compacted = []
    for (day_iso, city_id), stats in sorted(candidates.items()):
        if dry_run:
            compacted.append({"partition": [day_iso, city_id], **stats, "action": "would-rewrite"})
            continue
        predicate = day_filter(day_iso, city_id)
        frame = table.scan(row_filter=predicate).to_arrow()
        if frame.num_rows != stats["rows"]:
            raise RuntimeError(
                f"refusing to overwrite {day_iso}/{city_id}: scan returned {frame.num_rows}"
                f" rows but metadata says {stats['rows']}"
            )
        table.overwrite(frame, overwrite_filter=predicate)
        table = catalog.load_table(TABLE_NAME)
        after_part = partition_inventory(table).get(
            (day_iso, city_id), {"files": 0, "bytes": 0, "rows": 0}
        )
        if after_part["rows"] != stats["rows"]:
            raise RuntimeError(
                f"row count changed compacting {day_iso}/{city_id}:"
                f" {stats['rows']} -> {after_part['rows']}"
            )
        log.info(
            "partition compacted",
            extra=with_ctx(
                partition=f"{day_iso}/city={city_id}",
                files_before=stats["files"],
                files_after=after_part["files"],
                bytes_before=stats["bytes"],
                bytes_after=after_part["bytes"],
                rows=after_part["rows"],
            ),
        )
        compacted.append(
            {
                "partition": [day_iso, city_id],
                "files_before": stats["files"],
                "files_after": after_part["files"],
                "bytes_before": stats["bytes"],
                "bytes_after": after_part["bytes"],
                "rows": after_part["rows"],
            }
        )
    after = partition_inventory(catalog.load_table(TABLE_NAME))
    summary = {
        "partitions_total": len(before),
        "partitions_compacted": len([c for c in compacted if "files_after" in c]),
        "files_before_total": sum(s["files"] for s in before.values()),
        "files_after_total": sum(s["files"] for s in after.values()),
        "dry_run": dry_run,
        "details": compacted,
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-files", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    summary = compact(args.min_files, args.dry_run)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
