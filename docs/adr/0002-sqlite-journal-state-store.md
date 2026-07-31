# ADR 0002: Offset-tagged SQLite journal as the sessionizer state store

Status: accepted

## Context

The sessionizer needs durable per-ride state that survives crashes WITHOUT
breaking the exactly-once claim. Kafka transactions cover produced records
and consumed offsets, but any local store is a second commit domain: its
writes can land ahead of a Kafka transaction that then aborts (the two-store
gap, spec review round 4).

## Decision

SQLite in WAL mode with synchronous=FULL, holding an event JOURNAL and a
CLOSES table, every row tagged with the (partition, offset) that caused it.
On startup, after reading Kafka's transactionally-committed offsets, delete
every row tagged at or beyond the resume position; rebuild in-memory state by
folding the surviving journal.

## Alternatives considered

- RocksDB/plyvel: heavier dependency, same two-store gap, worse Windows
  story; the spec explicitly steers to a plyvel-free approach.
- Kafka Streams-style changelog topic inside the transaction: the purest
  design, but it reimplements a large part of Kafka Streams by hand and
  makes local debugging opaque; the journal gives the same rollback
  guarantee with inspectable SQL.
- No local store (rebuild from topic on every start): correct but O(topic)
  restart cost, and the abandoned-ride timeout needs long-lived state.

## Consequences

- Recovery is structural, not heuristic: the crash window is closed by
  deletion-before-consume, proven by a unit test that dies inside the gap
  and by the SIGKILL integration matrix.
- The journal doubles as a debugging artifact (plain SQL).
- Cost: every batch writes SQLite before Kafka; measured overhead is
  negligible at this throughput.
