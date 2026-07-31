# ADR 0003: A transactional clean mirror instead of tolerant Flink formats

Status: accepted

## Context

The generator injects unparseable records on purpose (0.2 percent). The
Python consumers divert them to the DLQ. Open-source Flink's avro-confluent
format has no skip-on-parse-error option: one poison record fails the whole
SQL job, and restart-from-offset loops on it forever.

## Decision

The sessionizer, which already deserialises every rides.events record and
owns the DLQ split, re-publishes each CLEAN event to `rides.events.clean`
inside its existing Kafka transaction. Flink reads the mirror; the raw topic
keeps its poison for the consumers built to handle it. Payments (JSON) stay
raw because Flink's json format does have ignore-parse-errors.

## Alternatives considered

- Cleansing the raw topic (no poison at all): destroys the DLQ story, the
  replay tooling, and the honesty of the generator.
- A dedicated relay service: one more consumer group, one more failure mode,
  and it would need its own exactly-once machinery; the sessionizer already
  has all of it.
- Waiting for FLIP-level error handling in the Avro format: not shippable.

## Consequences

- Declarative consumers get an exactly-once, schema-clean feed with
  identical event times; late and out-of-order records pass through, so
  watermark semantics and the late side output still demonstrate real
  lateness.
- The mirror doubles topic storage for rides.events (cheap at this scale)
  and adds one produce per event to the sessionizer's transaction (measured:
  no visible throughput change).
- DEVIATIONS.md records the departure from the spec's literal wiring.
