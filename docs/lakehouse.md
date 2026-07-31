# The Iceberg lakehouse layer

Raw `rides.events` history lands in an Apache Iceberg table on MinIO, written
by `src/sinks/iceberg_sink.py` through the `apache/iceberg-rest-fixture`
REST catalog. Everything below is real output from a live run on this stack
(18,571 events ingested from the topic, 39 poison messages diverted to the
DLQ, one live schema evolution mid-run).

## Table layout and partitioning

- Table: `lakehouse.rides_events`, warehouse `s3://lakehouse/` on MinIO.
- Partitioned by `days(event_ts)` and `city_id` (identity). Day partitioning
  matches the dominant query pattern (event-time ranges); the city identity
  partition keeps per-city scans to one directory without exploding partition
  counts (five cities).
- Delivery is AT-LEAST-ONCE by design: broker offsets commit only after a
  successful append, so a crash between append and commit replays a batch.
  The exactly-once budget was spent on the Postgres serving path, where
  duplicates would corrupt marts; here the append-only snapshot log makes any
  replay visible and reversible, and event_id gives consumers a dedup key.
  The module docstring in `src/sinks/iceberg_sink.py` carries the full
  argument.

## Schema evolution, live

The sink creates the table with the v1 schema (no `promo_code`). During the
demo run the generator switched to payload_version 2 fifteen seconds in
(`--evolve-after 15`), and the sink evolved the table in place without any
restart:

```
{"msg":"schema evolved in place","column":"promo_code","service":"iceberg-sink"}
APPENDED 18571 POISON 39 EVOLUTIONS 1
```

After evolution the current scan carries the column and old files read
cleanly with nulls:

```
CURRENT rows: 18571 | promo_code in columns: True
promo non-null: 568
```

## Time travel

Query the table as of its FIRST append snapshot (pre-evolution, 1,997 rows,
no promo_code column in that schema version):

```python
table.scan(snapshot_id=2711832667989405984).to_arrow()
```

```
TIME TRAVEL to first snapshot 2711832667989405984 -> rows: 1997 | has promo: False
```

The same query still returns exactly those 1,997 rows AFTER compaction
rewrote every partition (snapshot isolation: overwrites add snapshots, they
never destroy history):

```
snapshots now: 20
first-append snapshot 2711832667989405984 -> 1997 rows
pre-compaction snapshot 6391804647119832333 -> 18571 rows
current -> 18571 rows, 5 files
```

## Compaction

PyIceberg 0.11 has no first-class rewrite-data-files operation, so
`scripts/compact_iceberg.py` implements the spec's fallback: per partition,
read then transactionally overwrite that partition only, asserting row counts
before and after. The demo run had 50 small files (10 per partition, written
with a deliberately small flush interval); one pass collapsed each partition
to a single file, rows byte-for-byte identical:

```
{"msg":"partition compacted","partition":"2026-07-31/city=1","files_before":10,"files_after":1,"bytes_before":191077,"bytes_after":138024,"rows":4840}
{"msg":"partition compacted","partition":"2026-07-31/city=2","files_before":10,"files_after":1,"bytes_before":183262,"bytes_after":130785,"rows":4527}
{"msg":"partition compacted","partition":"2026-07-31/city=3","files_before":10,"files_after":1,"bytes_before":167820,"bytes_after":115345,"rows":3898}
```

A dry run (`--dry-run`) reports the plan without touching the table, and a
re-run after compaction is a no-op (`partitions_compacted: 0`): the tool is
idempotent.

## Catalog dev-stack caveat

The REST fixture image keeps table METADATA POINTERS in an in-memory SQLite
catalog: restarting the `iceberg-rest` container forgets tables, while all
data and metadata FILES survive in `s3://lakehouse/`. Fine for a dev stack
(the sink recreates the table and re-ingests); production would point the
fixture's `jdbc.*` properties at Postgres. Recorded in the compose file
comment.
