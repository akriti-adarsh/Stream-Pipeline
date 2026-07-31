# ADR 0001: Redpanda over Apache Kafka for the local platform

Status: accepted

## Context

The platform needs a Kafka-API broker plus a schema registry, must cold-start
inside 60 seconds on a laptop, and exercises the transactional API heavily
(exactly-once is the centrepiece claim).

## Decision

Run Redpanda (single binary, built-in Confluent-compatible schema registry)
pinned at v25.3.15, single node, dev-container mode.

## Consequences

- Cold start measured at ~15 s for the core profile; one fewer container
  (no separate registry, no ZooKeeper).
- The transactional API, read_committed isolation, and fencing behaved
  correctly under every kill test run against it.
- This is a DEV-ERGONOMICS choice, not a production endorsement: in
  production the calculus involves ecosystem tooling, tiered storage,
  operator maturity, and team experience, and Apache Kafka in KRaft mode is
  the documented swap (the application code would not change; spec section 14
  records the escape hatch).
