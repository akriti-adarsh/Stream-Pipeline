"""Consumer-lag exporter: ``sp_consumer_lag{group, topic, partition}``.

Every cycle (default 15 s) the exporter computes, for each watched consumer
group and each partition of the platform topics:

    lag = end_offset - next_offset_to_consume        (clamped at zero)

End offsets come from a throwaway probe consumer's ``get_watermark_offsets``
(a broker metadata query; the probe never joins a group or commits anything).
Where the NEXT offset comes from depends on the group, and the difference is
the point:

- ``sessionizer`` and ``iceberg-sink`` commit their progress to the broker
  (the sessionizer via ``send_offsets_to_transaction``), so the coordinator's
  committed offset is the truth and is read with ``Consumer.committed()``.
- ``pg-sink`` DELIBERATELY ignores broker commits: Kafka transactions cannot
  span an external database, so the sink stores its position in
  ``serving.consumer_offsets`` inside the same Postgres transaction as the
  data rows (see src/processors/pg_sink.py). Asking the broker for that
  group's offsets would report a lag the sink does not have; the only honest
  source is Postgres itself. The stored value is the LAST PROCESSED offset,
  so next-to-consume is stored + 1.

Run it with ``python -m observability.lag_exporter``; ``METRICS_PORT=9105``
serves the gauge (see common/metrics.py for the port conventions).
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import psycopg
from confluent_kafka import Consumer, TopicPartition

from common.kafka import TOPIC_PARTITIONS, consumer_config
from common.logging import configure_logging, with_ctx
from common.metrics import CONSUMER_LAG, maybe_start_metrics_server

BROKER_GROUPS: tuple[str, ...] = ("sessionizer", "iceberg-sink")
PG_GROUP = "pg-sink"
PROBE_GROUP = "sp-lag-exporter-probe"

PG_OFFSETS_QUERY = (
    "SELECT topic, partition, kafka_offset FROM serving.consumer_offsets WHERE consumer_group = %s"
)


@dataclass(frozen=True)
class LagSample:
    group: str
    topic: str
    partition: int
    lag: int


# ------------------------------------------------------------- pure shaping


def committed_next_from_broker(offsets: Iterable[Any]) -> dict[tuple[str, int], int | None]:
    """Shape ``Consumer.committed()`` results into next-to-consume offsets.

    Broker-committed offsets already carry next-to-consume semantics; negative
    sentinels (OFFSET_INVALID and friends) mean "nothing committed" -> None.
    """
    return {(tp.topic, tp.partition): (tp.offset if tp.offset >= 0 else None) for tp in offsets}


def committed_next_from_pg(
    rows: Iterable[tuple[str, int, int]],
) -> dict[tuple[str, int], int | None]:
    """Shape serving.consumer_offsets rows into next-to-consume offsets.

    The table stores the LAST PROCESSED offset (the sink seeks to stored + 1
    on startup), so next-to-consume is stored + 1.
    """
    return {(topic, partition): stored + 1 for topic, partition, stored in rows}


def compute_lags(
    group: str,
    end_offsets: Mapping[tuple[str, int], int],
    committed_next: Mapping[tuple[str, int], int | None],
) -> list[LagSample]:
    """One LagSample per partition the group has a committed position for.

    Partitions without a committed offset are skipped rather than reported as
    fully lagging: a group that never consumed a topic has no lag ON that
    topic, and inventing one would page somebody for nothing.
    """
    samples: list[LagSample] = []
    for (topic, partition), end in sorted(end_offsets.items()):
        next_offset = committed_next.get((topic, partition))
        if next_offset is None:
            continue
        samples.append(LagSample(group, topic, partition, max(end - next_offset, 0)))
    return samples


def publish(samples: Iterable[LagSample]) -> None:
    for sample in samples:
        CONSUMER_LAG.labels(
            group=sample.group, topic=sample.topic, partition=str(sample.partition)
        ).set(sample.lag)


# ------------------------------------------------------------------ harness


class LagExporter:
    def __init__(
        self,
        bootstrap: str,
        dsn: str,
        interval_sec: float = 15.0,
        *,
        probe: Any | None = None,
        group_consumers: dict[str, Any] | None = None,
        pg_rows: Callable[[], list[tuple[str, int, int]]] | None = None,
    ) -> None:
        self._log = configure_logging("lag-exporter")
        self._dsn = dsn
        self._interval = interval_sec
        self._probe = (
            probe if probe is not None else Consumer(consumer_config(bootstrap, PROBE_GROUP))
        )
        self._group_consumers: dict[str, Any] = (
            group_consumers
            if group_consumers is not None
            else {group: Consumer(consumer_config(bootstrap, group)) for group in BROKER_GROUPS}
        )
        self._pg_rows: Callable[[], list[tuple[str, int, int]]] = (
            pg_rows if pg_rows is not None else self._pg_rows_from_db
        )

    def watched_partitions(self) -> list[TopicPartition]:
        """Every partition of every platform topic that exists on the broker."""
        metadata = self._probe.list_topics(timeout=10)
        parts: list[TopicPartition] = []
        for topic in TOPIC_PARTITIONS:
            topic_md = metadata.topics.get(topic)
            if topic_md is None:
                continue
            parts.extend(TopicPartition(topic, p) for p in sorted(topic_md.partitions))
        return parts

    def end_offsets(self, parts: list[TopicPartition]) -> dict[tuple[str, int], int]:
        ends: dict[tuple[str, int], int] = {}
        for tp in parts:
            _low, high = self._probe.get_watermark_offsets(tp, timeout=10, cached=False)
            ends[(tp.topic, tp.partition)] = int(high)
        return ends

    def _pg_rows_from_db(self) -> list[tuple[str, int, int]]:
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(PG_OFFSETS_QUERY, (PG_GROUP,))
            return [(str(t), int(p), int(o)) for t, p, o in cur.fetchall()]

    def collect(self) -> list[LagSample]:
        parts = self.watched_partitions()
        ends = self.end_offsets(parts)
        samples: list[LagSample] = []
        for group, consumer in self._group_consumers.items():
            committed = committed_next_from_broker(consumer.committed(parts, timeout=10))
            samples.extend(compute_lags(group, ends, committed))
        samples.extend(compute_lags(PG_GROUP, ends, committed_next_from_pg(self._pg_rows())))
        return samples

    def run_forever(self) -> None:
        self._log.info("lag exporter running", extra=with_ctx(interval_sec=self._interval))
        while True:
            try:
                samples = self.collect()
                publish(samples)
                self._log.info(
                    "lag cycle",
                    extra=with_ctx(
                        samples=len(samples),
                        max_lag=max((s.lag for s in samples), default=0),
                    ),
                )
            except Exception as error:  # network blips must not kill the exporter
                self._log.warning("lag cycle failed", extra=with_ctx(error=str(error)))
            time.sleep(self._interval)


def main() -> int:
    log = configure_logging("lag-exporter")
    port = maybe_start_metrics_server("lag-exporter")
    log.info("metrics endpoint", extra=with_ctx(port=port))
    exporter = LagExporter(
        bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        dsn=os.environ.get("POSTGRES_DSN", "postgresql://stream:stream@localhost:5433/stream"),
        interval_sec=float(os.environ.get("LAG_INTERVAL_SEC", "15")),
    )
    exporter.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
