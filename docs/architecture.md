# Architecture

```
                         rides.events (Avro, dirty by design)
  generator ────────────► drivers.locations (Avro, 500+/s) ──────────────┐
  (state machine,        payments.transactions (plain JSON, late) ─┐     │
   GPS walks,                                                      │     │
   imperfections)                                                  │     │
                                                                   │     │
        rides.events ──► SESSIONIZER (Kafka txn, SQLite journal)   │     │
                          │ rides.sessions (exactly once)          │     │
                          │ rides.events.clean (poison firewall)   │     │
                          │ rides.events.dlq (envelopes)           │     │
                          ▼                                        ▼     ▼
                        FLINK SQL (clean mirror + payments + sessions + locations)
                          │ tumbling city metrics, session windows,
                          │ interval join, late side output -> late.events
                          ▼
        PG-SINK (offsets-in-txn, idempotent upserts) ──► POSTGRES (raw.*)
        ICEBERG-SINK (at-least-once, batched) ─────────► MinIO (lakehouse.*)
                                                           │
        POSTGRES raw.* ──► dbt (staging/intermediate/marts, │ SCD2, incremental)
                       ──► Great Expectations (dq_results, gauges)
                       ──► Streamlit (live ops dashboard)
        everything ──► Prometheus ──► Grafana (+ lag alerting)
```

## The four delivery contracts

The platform deliberately implements four different delivery guarantees and
labels each, because "exactly-once" is a property of a PATH, not a platform:

1. **rides.events -> rides.sessions / rides.events.clean (Kafka to Kafka)**:
   exactly-once via Kafka transactions: output records and consumed offsets
   commit atomically; a stable transactional.id fences zombies. The local
   SQLite state store is offset-tagged and rolled back on recovery, closing
   the two-store gap (see the sessionizer module docstring and the
   SIGKILL-parametrised integration test).
2. **Kafka -> Postgres**: effectively exactly-once WITHOUT Kafka transactions:
   the offsets table row lives in the same Postgres transaction as the data,
   the consumer resumes from stored offset + 1 and ignores broker offsets,
   and upserts are idempotent on natural keys. This is the standard pattern
   when the sink cannot join a Kafka transaction; the module docstring in
   pg_sink.py is the canonical statement.
3. **Kafka -> Iceberg**: at-least-once, on purpose: offsets commit after the
   append, duplicates are possible on crash, and the docstring says exactly
   why that trade-off is right for an archival lakehouse.
4. **Flink -> Postgres**: at-least-once with idempotent JDBC upserts (primary
   keys on every windowed sink), which flattens replays into overwrites.

## Why a clean mirror exists

Open-source Flink's avro-confluent format cannot skip an unparseable record;
one poison message fails the whole declarative job. Rather than pretending
the input is clean (it is deliberately not), the transactional sessionizer
re-publishes every successfully-decoded event to `rides.events.clean` inside
its transaction. Declarative consumers get a schema-clean, exactly-once feed;
the raw topic keeps its warts for the imperative consumers that can handle
them. DEVIATIONS.md records the reasoning.

## Time

Event time is a simulation clock, compressed by `--speed` (60x default) and
deterministic per (seed, anchor). Everything downstream is event-time native:
watermarks with a 30 s bound (the generator's documented skew), a 2 h
event-time session timeout closing abandoned rides, a 15 minute interval-join
bound, and a 60 minute incremental lookback in dbt. Wall-clock freshness is
asserted on ingestion timestamps instead of event time, because sim time
legitimately outruns the wall clock.

## Operational surfaces

- Metrics: every service exposes sp_* metrics (ports 9101 to 9109); the lag
  exporter derives sp_consumer_lag from broker AND Postgres-stored offsets.
- Quality: Great Expectations suites write machine-readable rows to
  serving.dq_results; the runner exits nonzero in --mode fail; pass rate is a
  Prometheus gauge with a Grafana panel.
- Replay: scripts/replay_dlq.py closes the poison loop; scripts/
  reset_platform.py and scripts/e2e.sh bound the blast radius of experiments.
