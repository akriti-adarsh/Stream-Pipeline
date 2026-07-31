Build `stream-pipeline`, an end-to-end real-time data platform: event producers → Kafka → a stateful stream processor with exactly-once semantics → a Postgres serving layer and an Iceberg-style lakehouse → dbt models → a live dashboard, with data-quality gates, schema evolution, dead-letter handling, and replay. This is a data-engineering portfolio piece, so operational concerns matter more than feature count.

**Precedence rule:** the "Review round" sections at the end of this file amend the sections above them; where they conflict, the review sections govern.

### 0. Absolute constraints

1. **`docker compose up` must bring up the entire platform and start producing data within 60 seconds**, with no cloud account. Use Redpanda (Kafka API compatible, single binary, fast start) rather than Kafka + ZooKeeper.
2. **Correctness over throughput, but measure throughput.** The processor must be exactly-once with respect to the sink, and you must prove it with a test that kills the processor mid-batch and asserts no duplicates and no loss.
3. **No unfinished code, no fake data paths.** The synthetic generator must produce realistic, statistically coherent data including the messy cases (late events, out-of-order events, duplicates, malformed payloads, schema drift).
4. Full type hints, `mypy --strict` on `src/`, ruff clean, ≥80% coverage on Python.
5. Pin everything. Commit `uv.lock` and pinned Docker image tags — never `:latest`.
6. Commit per section 11.

### 1. Stack

Python 3.11+, `uv`. Redpanda (Kafka API) + Redpanda Console. Schema Registry with **Avro** for the main topics and one topic deliberately using JSON Schema so the repo demonstrates both. `confluent-kafka` (librdkafka) for producers/consumers — not `kafka-python`, because the transactional API matters here. Stream processing in **Faust-style hand-rolled consumers with Kafka transactions** plus a **Flink SQL** job for the windowed aggregation path (via the Flink SQL client in a container), so the repo shows both a code-level and a declarative approach. Postgres 16 as the serving store. MinIO + Apache Iceberg (via PyIceberg + a REST catalog) as the lakehouse. dbt-postgres for modelling. Great Expectations for quality gates. Prometheus + Grafana for metrics, with committed provisioned dashboards. Streamlit for the live dashboard.

### 2. The domain — pick something with real stream semantics

Model a **ride-hailing / delivery operations stream**. It gives you naturally joined streams, session windows, geospatial aggregation, and a real reason for late data. Three source topics:

- `rides.events` (Avro) — lifecycle events: `requested`, `matched`, `driver_arrived`, `started`, `completed`, `cancelled`. Fields: `event_id`, `ride_id`, `event_type`, `event_ts`, `rider_id`, `driver_id?`, `city_id`, `pickup_lat/lon`, `dropoff_lat/lon?`, `fare_cents?`, `surge_multiplier`, `payload_version`.
- `drivers.locations` (Avro) — high-volume pings: `driver_id`, `ts`, `lat`, `lon`, `speed_kmh`, `heading`, `status`. Target 500+ msg/sec from the generator.
- `payments.transactions` (JSON Schema) — `txn_id`, `ride_id`, `amount_cents`, `status`, `method`, `ts`. Deliberately arrives 5–90 seconds after ride completion so the join must handle it.

### 3. The generator — this is where most projects cheat

`src/generator/` must produce data that looks real:
- **Coherent state machine:** a ride cannot be `completed` without `started`. Track per-ride state; emit only valid transitions. ~8% of rides cancel, at realistic points in the lifecycle.
- **Diurnal traffic pattern:** event rate follows a sinusoidal daily curve with morning and evening peaks, scaled by a `--speed` factor so a day compresses into minutes.
- **Geospatially plausible:** drivers move along smoothed random walks constrained to a city bounding box, with speed drawn from a status-conditional distribution. Pickup/dropoff points sampled from a mixture of Gaussians representing city hotspots.
- **Deliberate imperfections, each with a config flag and a documented rate:** duplicate events (0.5%, same `event_id`), out-of-order events (2%, timestamp skew up to 30s), late events (1%, up to 10 minutes late), malformed payloads (0.2%, unparseable — these must land in the DLQ), null fields where nullable, and a **schema evolution event**: after `--evolve-after` seconds, the generator starts emitting `payload_version: 2` with a new optional field `promo_code`, proving the consumers handle it without redeployment.
- CLI: `python -m generator --speed 60 --duration 600 --seed 42`. Same seed produces the same stream.

### 4. Stream processing

**Path A — transactional Python consumer** (`src/processors/ride_sessionizer.py`):
- Consumes `rides.events`, builds per-ride session state in a RocksDB-like local store (use `plyvel`-free approach: SQLite with WAL as the state store, keyed by `ride_id`, checkpointed with the Kafka offset in the same transaction)
- Uses Kafka **transactions**: `init_transactions`, `begin_transaction`, produce to output topic + commit offsets in the transaction, `commit_transaction`. Exactly-once from input topic to output topic.
- For the Postgres sink, achieve effective exactly-once with an idempotent upsert keyed on `(ride_id, event_seq)` plus an offset-tracking table written in the same DB transaction as the data. Document why this is the standard pattern when the sink isn't transactional with Kafka.
- Emits `rides.sessions` (Avro): one record per completed/cancelled ride with derived fields — `time_to_match_sec`, `time_to_pickup_sec`, `ride_duration_sec`, `haversine_distance_km`, `avg_speed_kmh`, `is_late_arrival`, `terminal_state`
- Watermark handling: a ride with no terminal event after `session_timeout` (default 2h event-time) is closed as `abandoned`
- Handles the schema-evolution event by reading with a reader schema that tolerates the new optional field

**Path B — Flink SQL** (`flink/sql/`):
- `01_sources.sql` — Kafka table definitions with watermarks (`WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND`)
- `02_city_metrics.sql` — 1-minute tumbling windows per city: ride count, completion rate, cancellation rate, mean surge, p50/p95 time-to-match. Include the `EMIT` semantics discussion in a comment.
- `03_driver_utilisation.sql` — session windows over `drivers.locations` with a 5-minute gap, computing active minutes and distance travelled
- `04_ride_payment_join.sql` — interval join between `rides.sessions` and `payments.transactions` with a 15-minute bound, plus a left-outer variant so unpaid rides surface
- Sink all of these to Postgres via the JDBC connector
- A `flink/submit.sh` that submits the jobs, and a documented note on what happens to late data past the watermark (it goes to a side output topic `late.events` — implement that)

**DLQ:** malformed messages go to `<topic>.dlq` with an envelope containing the raw bytes (base64), the error, the consumer group, and the original partition/offset. `scripts/replay_dlq.py` replays a DLQ topic back to source after a fix, with a `--dry-run` and a filter predicate. Test the full poison-message → DLQ → fix → replay cycle.

### 5. Lakehouse layer

`src/sinks/iceberg_sink.py` — a consumer that batches `rides.events` into Iceberg tables on MinIO via PyIceberg with a REST catalog:
- Partitioned by `days(event_ts)` and `city_id`
- Hourly compaction job (`scripts/compact_iceberg.py`) that rewrites small files, with before/after file-count and size logged
- Schema evolution exercised: add the `promo_code` column to the Iceberg table and show old files still read correctly
- A time-travel demo in `docs/lakehouse.md`: query the table as of a prior snapshot, with the actual output pasted in

### 6. dbt models

`dbt/models/` in a medallion structure:
- `staging/` — one view per source table, renaming to snake_case, casting types, no business logic. `stg_rides__events`, `stg_drivers__locations`, `stg_payments__transactions`.
- `intermediate/` — `int_rides__enriched` (session + payment + city dimension), `int_drivers__shifts`
- `marts/` — `fct_rides`, `fct_driver_shifts`, `dim_cities`, `dim_drivers` (SCD Type 2 on driver status/tier, using dbt snapshots — implement the snapshot properly), `agg_city_hourly`
- Incremental materialisation on `fct_rides` with `unique_key='ride_id'` and a `merge` strategy, plus an `is_incremental()` filter on event_ts with a lookback window to catch late data. **Document the lookback reasoning in the model file.**
- `schema.yml` with descriptions on every column and tests: `not_null`, `unique`, `accepted_values`, `relationships`, plus 4 custom singular tests in `tests/` — e.g. `assert_no_negative_fares.sql`, `assert_completed_rides_have_duration.sql`, `assert_no_future_event_ts.sql`, `assert_payment_amount_matches_fare.sql`
- `dbt docs generate` output committed under `docs/dbt/` (or a screenshot of the lineage graph in the README — the lineage DAG is a strong visual)

### 7. Data quality

Great Expectations suites run as a pipeline step, not decoratively:
- A suite on the raw landing table: expected columns, types, non-null keys, value ranges on lat/lon and fares, categorical membership on `event_type`
- A suite on `fct_rides`: row-count anomaly detection against a rolling baseline, uniqueness on `ride_id`, freshness (max `event_ts` within N minutes of now)
- **Failure behaviour matters:** the pipeline must be configurable to `warn` or `fail`, failures must write a machine-readable report to Postgres table `dq_results`, and a Prometheus gauge must expose the pass rate. Show a screenshot of the Grafana panel with a real failure spike after you deliberately inject bad data via a generator flag.

### 8. Observability

- Every service exposes Prometheus metrics: consumer lag per topic/partition/group, messages processed, processing latency histogram, DLQ rate, transaction commit/abort counts, state-store size, dbt run duration, DQ pass rate
- `grafana/dashboards/` — two committed provisioned dashboards: "Pipeline Health" (lag, throughput, error rates, DLQ) and "Business Metrics" (rides/min by city, completion rate, surge, driver utilisation)
- **Consumer lag alerting rule** in `prometheus/alerts.yml` firing when lag exceeds a threshold for 2 minutes
- Structured JSON logs with correlation on `ride_id` across services

### 9. Dashboard

`ui/streamlit_app.py` — reads from Postgres, auto-refreshing: a live map of driver positions (pydeck), rides/minute time series by city, completion-rate gauge, p95 time-to-match, and a DQ status strip. It must degrade gracefully when a table is empty rather than throwing.

### 10. Tests

- ≥80% coverage on `src/`
- **Unit:** generator state machine legality (property test: no invalid transition is ever emitted, for any seed), haversine correctness against known city-pair distances, Avro serde round-trip including the v1→v2 evolution, DLQ envelope construction, watermark/lateness logic
- **Integration (marked `@pytest.mark.integration`, run against docker-compose in CI):**
  - `test_exactly_once.py` — produce 10,000 known events, kill the processor process with SIGKILL at a random point, restart, wait for completion, then assert the Postgres row count equals exactly 10,000 distinct rides with no duplicates. Run it 3 times with different kill points. **This is the single most valuable test in the repo.**
  - `test_dlq_replay.py` — inject malformed messages, assert they land in DLQ, replay, assert they're processed
  - `test_schema_evolution.py` — start the consumer on v1, trigger evolution, assert no consumer crash and that `promo_code` is populated for v2 records
  - `test_late_data.py` — inject an event 10 minutes late, assert it appears in `late.events` and that the incremental dbt lookback picks it up
  - `test_dbt_build.py` — `dbt build` runs clean against a seeded database, all tests pass
- A `make e2e` target that runs the whole platform, waits for data, asserts row counts across every layer, and tears down. CI runs this on a schedule.

### 11. Commit plan

1. `chore: scaffold, tooling, ci` 2. `feat(schemas): avro and json schemas + registry setup` 3. `feat(generator): coherent ride state machine` 4. `feat(generator): geospatial driver movement and diurnal load` 5. `feat(generator): configurable imperfections and schema evolution` 6. `infra: redpanda, console, postgres, minio in compose` 7. `feat(processors): sessionizer with local state store` 8. `feat(processors): kafka transactions for exactly-once` 9. `feat(processors): idempotent postgres sink with offset table` 10. `feat(dlq): dead letter envelope and replay script` 11. `feat(flink): source tables with watermarks` 12. `feat(flink): windowed city metrics and driver utilisation` 13. `feat(flink): interval join with late side output` 14. `feat(sinks): iceberg writer with partitioning` 15. `feat(sinks): iceberg compaction job` 16. `feat(dbt): staging models` 17. `feat(dbt): intermediate and marts with scd2 snapshot` 18. `feat(dbt): incremental fct_rides with late-data lookback` 19. `test(dbt): schema tests and four singular tests` 20. `feat(quality): great expectations suites and dq_results sink` 21. `feat(observability): prometheus metrics across services` 22. `feat(observability): provisioned grafana dashboards and alerts` 23. `feat(ui): live streamlit dashboard` 24. `test: exactly-once kill-restart integration test` 25. `test: dlq replay, schema evolution, late data` 26. `ci: lint, unit, and scheduled e2e` 27. `docs: architecture, lakehouse, runbook, adrs` 28. `docs: readme with measured throughput and screenshots`

### 12. Definition of done

- [ ] `docker compose up -d && make seed` → data flowing through every layer within 90 seconds
- [ ] The exactly-once kill test passes 3/3 runs
- [ ] `make e2e` green
- [ ] Throughput measured and reported: sustained msg/sec, p95 end-to-end latency from produce to appearing in `fct_rides`
- [ ] `dbt build` clean, lineage graph screenshot in README
- [ ] Grafana dashboards load with real data; DQ failure screenshot included
- [ ] Iceberg time-travel query output pasted in docs
- [ ] DLQ replay cycle documented with real command output
- [ ] `docs/runbook.md` covers: consumer lag climbing, DLQ filling, processor crash-looping, dbt test failure — with actual diagnostic commands
- [ ] No `:latest` image tags anywhere
- [ ] CI green

### 13. Session plan

**This file is fully self-contained — no companion document is required.** Hand this single file to Claude Code and run the build as the sessions below, under this protocol:

**Session 1, before any code:** save this prompt as `docs/BUILD_SPEC.md`, create `DEVIATIONS.md` (header only), and create `CLAUDE.md` exactly as follows — commit all three as part of commit 1, and keep CLAUDE.md's State section current at every commit thereafter.

```markdown
# CLAUDE.md — standing rules and state
The spec is docs/BUILD_SPEC.md. This file is rules and state; the spec defines the work.

## Rules — non-negotiable
1. No TODO, FIXME, NotImplementedError, or stub bodies anywhere in src/. Ever.
2. Dependency versions come from the resolver (`uv add` / `npm install`); commit the lockfile.
   Never hand-type a version number the resolver has not produced.
3. Nothing is "done" until its command has run in THIS session with the real output shown —
   the actual pytest summary line, the actual exit status. "Should pass" is not a status.
4. Every number in a README or doc must exist in a committed artifact (eval_results/,
   benchmarks/results/, a CI log). An estimated or remembered number is a defect.
5. When a library, API, or dataset differs from the spec — renamed function, changed endpoint,
   auth now required — adapt to reality and add one line to DEVIATIONS.md
   (spec said / reality is / what was done). Never mock a real path to fake compliance.
6. Never weaken, skip, or delete a test to make it pass. Fix the code or flag the conflict.
7. One commit per plan milestone; the full test suite runs green before every commit.
8. If the next milestone will not fit in the session's remaining capacity, stop at the last
   green commit and update State. Do not start work you cannot finish.

## State (update at every commit)
- Plan position: <n> of <total>. Last completed: "<commit message>"
- Suite at last commit: <pytest summary line> · Coverage: <n>%
- Open deviations: <count> · Next up: commits <n+1>–<m>
- Notes for next session: <blockers, decisions pending>
```

**Every session after the first opens with this message** (the human pastes it, filling the brackets):

> Read CLAUDE.md, DEVIATIONS.md, docs/BUILD_SPEC.md (skim), and `git log --oneline -15`. We are at commit [n] of the plan. First action: run `make test` and paste the summary line. If it is not green, fixing that is the entire session — no new work on a red suite. If green, proceed with commits [n+1]–[m] only, under the CLAUDE.md rules. Stop at the last green commit before context runs low and update State.

**Between sessions (human, ~15 minutes):** run `make test`; run `grep -rnE "TODO|FIXME|NotImplementedError" src/`; compare `git log --oneline` against the commit plan; open the newest test file and check it asserts something real rather than that a mock returned what it was told; pick one number from the README and trace it to its committed artifact. Any failure means the next session opens with "fix these findings" instead of new work.

**If the build starts thrashing** — rewriting working code, a test flip-flopping between attempts, quiet "simplifications" of the spec — stop, `git reset --hard <last-green-commit>`, and open a fresh session scoped to one milestone with the exact error text pasted in.

**The session slices for this project:**

| Session | Commits | Boundary check before closing |
|---|---|---|
| A | 1–10 (schemas → generator → infra → sessionizer → exactly-once → DLQ) | The exactly-once kill test passes locally at least once, live: SIGKILL the processor mid-run, restart, counts reconcile. Don't leave session A without this — everything downstream trusts it. |
| B | 11–15 (Flink jobs → Iceberg sink → compaction) | All four Flink jobs running; inject one deliberately late event and show it landing in `late.events`; an Iceberg snapshot queryable with time travel. |
| C | 16–23 (dbt staging → marts → GE gates → observability → dashboard) | `dbt build` fully clean; the Streamlit dashboard shows live data; inject bad data via the generator flag and show the DQ failure appear in Grafana — screenshot it now, while it's easy. |
| D | 24–28 (integration tests → CI → docs → README) + acceptance | `make e2e` green three consecutive times; kill test green 3/3 with different kill points; measured throughput and p95 end-to-end latency recorded into the README from real runs. |

### 14. Failure recovery — project-specific

- **Docker memory:** this is the heaviest stack in the set — Redpanda + Flink + Postgres + MinIO + Prometheus + Grafana wants **10–12 GB allocated to Docker**. Define two compose profiles: `core` (generator, Redpanda, Python processor, Postgres, dbt — runs in ~6 GB and demonstrates the exactly-once heart of the project) and `full` (everything). The README quickstart uses `core`; the Flink/Grafana sections say `--profile full`. Any published number states which profile produced it.
- **Flink → Postgres JDBC sink:** the Flink image does not ship the Postgres JDBC driver or the flink-connector-jdbc jar. Build a small custom image (`FROM flink:<pinned>`, `ADD` both jars into `/opt/flink/lib/`) in the repo's `flink/Dockerfile` — do not volume-mount jars ad hoc, it breaks the fresh-clone test. Match connector version to Flink version via the resolver-era docs, not memory.
- **Iceberg REST catalog:** use a maintained REST-catalog image (`tabulario/iceberg-rest` or the Apache fixture image — check which is current at build time) with a pinned tag, and verify PyIceberg client compatibility by actually creating and reading a table in an integration test before building the sink on top. Catalog/client version mismatch is the classic time sink here.
- **`confluent-kafka` wheels:** prebuilt for common platforms; if the platform lacks one, the fix is `apt-get install librdkafka-dev build-essential` (documented in the README's troubleshooting note), never a switch to `kafka-python` — the transactional API this project depends on isn't there.
- **CI integration-test flake:** the e2e job must wait on container health checks with explicit timeouts, not sleeps. If infrastructure flakes (image pull, port race), retry the *job* once — never wrap the exactly-once assertion itself in a retry, and never widen its tolerance from "exactly 10,000" to "approximately". The assertion being brittle to real duplicates is its entire value.
- **Redpanda vs Kafka:** if anything Kafka-API-specific misbehaves on Redpanda (rare, but transactions coordination has had edge cases across versions), pin a recent Redpanda release first; only if a genuine incompatibility persists, swap the compose service for `apache/kafka` in KRaft mode (no ZooKeeper) and record the swap in DEVIATIONS.md — the application code shouldn't change either way.

### 15. Review round 2 — added depth and corrections

- **Schema registry — use Redpanda's built-in:** Redpanda ships a schema registry on the same binary (REST API, port 8081); no separate Confluent container. `confluent-kafka`'s Avro serializer/deserializer point at it unchanged. One fewer service, same interface.
- **Iceberg compaction, realistically:** if the pinned PyIceberg lacks a first-class rewrite operation, implement compaction per partition as read partition → write compacted parquet → transactional overwrite of that partition, one partition at a time. The test asserts file count drops, row counts match exactly, and pre-compaction snapshots still read correctly via time travel.
- **The sink's crux, spelled out in code:** the offsets table row is `(consumer_group, topic, partition, offset)`, upserted in the same Postgres transaction as the data rows; on startup the consumer seeks to stored offset + 1 and deliberately ignores broker-committed offsets for this path. Put that sentence in the module docstring — it *is* the exactly-once argument, and it's the interview question.

### 16. Review round 3 — final audit findings

- **`isolation.level=read_committed`** on every consumer of a transactionally-produced topic — the Postgres sink and anything reading `rides.sessions`. Kafka's consumer default is read_uncommitted, which would surface aborted-transaction messages and quietly void the exactly-once claim. Add a test: begin a transaction, produce, abort, assert the downstream consumer never sees those records.
- **Kill-test bookkeeping:** the harness records every event_id the producer received an ack for, and the assertion compares Postgres contents against exactly that acked set — valid regardless of imperfection flags. Run the kill test once with imperfections off (legibility) and once with duplicates on (proves idempotent upsert under crash, not just under clean replay).
- **Flink session windows:** session-window support in the table-valued-function syntax arrived late and partially; if the pinned Flink version lacks it, use the legacy `GROUP BY SESSION(...)` group-window syntax for `03_driver_utilisation.sql` and say so in the file's header comment rather than silently switching to tumbling windows.

### 17. Review round 4 — closing ambiguities

- **State-store recovery rule — the two-store gap:** the Kafka transaction covers output records and consumed offsets, but the SQLite state store is a second store whose commits can land ahead of a Kafka transaction that then aborts on crash. Close it structurally: tag every state mutation with the offset that caused it; on startup, after Kafka's transactional offsets determine the resume position, delete any state rows tagged at or beyond that position before consuming. Add a crash test aimed at exactly that window — SIGKILL after the SQLite commit but before `commit_transaction` — proving no duplicate session emission and no corrupted ride state.

---

## After the build

**Description:** `Real-time data platform: Redpanda → transactional stream processing with exactly-once semantics → Iceberg lakehouse + Postgres marts, with dbt models, Great Expectations gates, DLQ replay, and Grafana observability.`

**Topics:** `data-engineering` `kafka` `streaming` `exactly-once` `flink` `dbt` `iceberg` `data-quality`

**LinkedIn entry:**

> Built a real-time data platform processing XXX events/sec end to end: Redpanda ingestion with Avro schema registry, a transactional stream processor providing exactly-once delivery (verified by a chaos test that SIGKILLs the consumer mid-batch and asserts zero duplicates across 10,000 events), Flink SQL windowed aggregations with watermark-based late-data side outputs, an Iceberg lakehouse on MinIO with partition compaction and time travel, and dbt marts including an SCD Type 2 dimension and an incremental fact table with a late-data lookback window. Added Great Expectations quality gates writing to a queryable results table, dead-letter replay tooling, and provisioned Grafana dashboards with consumer-lag alerting.

**Be ready for:** Explain exactly-once from Kafka to Postgres — Kafka transactions don't cover the external sink, so what actually guarantees it? Why a lookback window on the incremental model? What's your watermark delay and what does that cost you? What happens when the schema registry is unavailable? Why Redpanda over Kafka here (answer: developer ergonomics and start time for a portfolio project; in production the calculus is different and you should say so).
