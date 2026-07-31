# Runbook

Diagnostic-first: every scenario opens with the commands that tell you which
world you are in, then the fix per world. Ports are the host ports from
docker-compose.yml (Kafka 19092, Schema Registry 18081, Postgres 5433, Flink
UI 18083, Prometheus 19090, Grafana 13000).

## Consumer lag climbing

Confirm and localise:

```bash
# Which group, which topic, how bad (broker's own view):
docker compose exec redpanda rpk group describe sessionizer
docker compose exec redpanda rpk group describe pg-sink

# The pg-sink group's truth lives in Postgres, not the broker:
docker compose exec -T postgres psql -U stream -d stream -c \
  "SELECT topic, partition, kafka_offset, updated_at FROM serving.consumer_offsets ORDER BY topic"

# Prometheus view (fed by the lag exporter):
curl -s 'localhost:19090/api/v1/query?query=sp_consumer_lag' | python -m json.tool | head -40
```

Worlds and fixes:

- **Lag on every group and rising ping volume**: the generator is simply
  outrunning the consumers (crank `--speed` down, or accept catch-up time;
  the processors are batch-efficient and drain fast once production stops).
- **Lag only on `sessionizer`**: check its process is alive and committing:
  `curl -s localhost:9102/metrics | grep sp_transactions_committed_total`.
  A flat commit counter with a live process means a stuck transaction: look
  for `commit_transaction` errors in its stderr; restarting is SAFE (the
  whole design exists so a restart replays cleanly).
- **Lag only on `pg-sink`**: almost always Postgres. `docker compose exec
  postgres pg_isready`; look for lock waits:
  `SELECT * FROM pg_stat_activity WHERE wait_event IS NOT NULL;`.
- **Lag frozen with consumers dead**: see crash-looping below.

## DLQ filling

Confirm and read the evidence (the envelope carries everything):

```bash
docker compose exec redpanda rpk topic describe rides.events.dlq -p
# Look at WHAT is failing and WHY (error field, consumer group, offsets):
docker compose exec redpanda rpk topic consume rides.events.dlq -n 3 -o start \
  | python -c "import json,sys;
[print(json.loads(json.loads(l)['value'])['error']) for l in sys.stdin if l.strip().startswith('{')]"
```

- **`Unknown magic byte` / deserialisation errors at a steady ~0.2 percent**:
  that is the generator's documented malformed rate doing its job. No action;
  this is the platform proving its poison handling.
- **A spike beyond the documented rate**: something upstream changed its wire
  format. Diff the registry: `curl -s localhost:18081/subjects/rides.events-value/versions`.
  After fixing the producer, REPLAY the quarantined messages:
  `uv run python scripts/replay_dlq.py --topic rides.events --dry-run` first,
  then with `--repair-magic-byte` (framing corruption) or
  `--transform module:function` (anything richer). Replay is idempotent
  downstream: the sink dedups on (ride_id, event_seq).

## Processor crash-looping

```bash
# Is it actually looping? Watch restarts and the last words:
docker compose ps
docker compose logs --tail 50 <service>       # containerised processors
# Host-run processors: the JSON logs go to stderr; the last 'partition assigned'
# line shows the recovery decision (resume offset, rolled_back_journal counts).
```

- **Loops on the SAME offset with a deserialisation error**: a poison message
  in a path without DLQ tolerance. For the Python processors this cannot
  happen (poison goes to the DLQ); for Flink SQL it can, which is exactly why
  Flink reads `rides.events.clean`. If a Flink job loops: check
  `curl -s localhost:18083/jobs/overview`, then the taskmanager logs:
  `docker compose logs flink-taskmanager | grep -A5 'Caused by'`. Resubmit
  after the fix: `docker compose --profile full run --rm flink-sql-submit`
  (idempotent; RUNNING pipelines are skipped).
- **Sessionizer loops with `ProducerFencedException`**: TWO instances share
  transactional.id. Kill the zombie; the fencing is the system working.
- **Loops with SQLite errors**: the state file is corrupt beyond the design's
  guarantees (disk full, manual edits). It is SAFE to delete
  `state/sessionizer.db`: the store is rebuilt from the Kafka-committed
  offsets, and the sink's idempotency absorbs any re-emission.
- **pg-sink loops on a constraint violation**: that unique index on event_id
  is the platform's tripwire; it means an idempotency invariant broke. Do NOT
  drop the index. Capture the conflicting row and the offsets table, then
  compare against the topic at those offsets.

## dbt test failure

```bash
# Which test, which rows:
uv run dbt build --project-dir dbt --profiles-dir dbt 2>&1 | grep -E "FAIL|ERROR"
# Every failed test compiles to SQL you can run directly, e.g.:
docker compose exec -T postgres psql -U stream -d stream -f - \
  < dbt/target/compiled/stream_pipeline/tests/assert_payment_amount_matches_fare.sql
```

- **Singular test failures**: the compiled query RETURNS the offending rows;
  trace one ride_id backwards: `SELECT * FROM raw.rides_events WHERE ride_id = '...'`
  then the topic itself at the recorded kafka_partition/kafka_offset.
- **Freshness or volume anomalies (also visible as DQ failures)**: check the
  pipeline is actually running before debugging models: `docker compose ps`,
  then the DQ history: `SELECT suite, expectation, success, run_at FROM
  serving.dq_results ORDER BY run_at DESC LIMIT 20;`.
- **`relationships` failures on city_id**: the seed changed without a
  `dbt seed` rerun. `uv run dbt seed --project-dir dbt --profiles-dir dbt`.

## Full reset (nuclear, local only)

```bash
uv run python scripts/reset_platform.py     # topics, groups, PG raw, state dir
docker compose --profile full down -v       # and volumes, if you want true zero
```
