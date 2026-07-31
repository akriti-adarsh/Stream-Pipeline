"""Prometheus metrics shared by every host-side service: one fixed contract.

Every metric name carries the ``sp_`` prefix and is defined HERE, once, so no
service can drift from the contract. Services import the objects they need and
label them with their own ``service`` name; nothing hand-types a metric name
twice. The quality runner (src/quality) predates this module and exposes its
own ``sp_dq_*`` gauges on its own port; that is intentional and left alone.

Port conventions (host processes, scraped by the compose Prometheus through
host.docker.internal, see prometheus/prometheus.yml):

    generator      9101
    sessionizer    9102
    pg-sink        9103
    iceberg-sink   9104
    lag-exporter   9105
    dq-runner      9109

:func:`maybe_start_metrics_server` reads the ``METRICS_PORT`` environment
variable. Unset (or empty) means "no server": unit tests and library imports
stay silent and never open a socket. Counter updates without a server are
plain in-process arithmetic, so instrumented code paths cost nothing
observable when metrics are off.
"""

from __future__ import annotations

import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server

MESSAGES_PROCESSED = Counter(
    "sp_messages_processed",
    "Messages consumed and processed, per service and source topic",
    ["service", "topic"],
)
PROCESSING_LATENCY = Histogram(
    "sp_processing_latency_seconds",
    "Wall-clock seconds spent processing one batch",
    ["service"],
)
DLQ_MESSAGES = Counter(
    "sp_dlq_messages",
    "Poison messages routed to a dead-letter topic",
    ["service", "topic"],
)
TRANSACTIONS_COMMITTED = Counter(
    "sp_transactions_committed",
    "Kafka transactions committed",
    ["service"],
)
TRANSACTIONS_ABORTED = Counter(
    "sp_transactions_aborted",
    "Kafka transactions aborted",
    ["service"],
)
STATE_JOURNAL_ROWS = Gauge(
    "sp_state_journal_rows",
    "Rows in the sessionizer state-store journal table",
    ["service"],
)
STATE_CLOSES_ROWS = Gauge(
    "sp_state_closes_rows",
    "Rows in the sessionizer state-store closes table",
    ["service"],
)
SESSIONS_EMITTED = Counter(
    "sp_sessions_emitted",
    "Ride sessions emitted (transactionally committed)",
    ["service"],
)
ROWS_WRITTEN = Counter(
    "sp_rows_written",
    "Rows written to a sink table",
    ["service", "table"],
)
EVENTS_PRODUCED = Counter(
    "sp_events_produced",
    "Events acknowledged by the broker",
    ["service", "topic"],
)
EVENTS_ACK_FAILED = Counter(
    "sp_events_ack_failed",
    "Events whose delivery report carried an error",
    ["service", "topic"],
)
CONSUMER_LAG = Gauge(
    "sp_consumer_lag",
    "End offset minus next-to-consume offset, per group, topic, and partition",
    ["group", "topic", "partition"],
)
UP = Gauge(
    "sp_up",
    "1 while the service's metrics endpoint is being served",
    ["service"],
)


def maybe_start_metrics_server(service: str) -> int | None:
    """Start the Prometheus HTTP endpoint if METRICS_PORT is set.

    Returns the port when a server was started, ``None`` otherwise. Reading
    the environment here (rather than taking a constructor argument) keeps
    instrumentation out of every service's dependency-injection surface: unit
    tests construct services exactly as before and no socket ever opens.
    """
    raw = os.environ.get("METRICS_PORT", "").strip()
    if not raw:
        return None
    port = int(raw)
    if port <= 0:
        return None
    start_http_server(port)
    UP.labels(service=service).set(1)
    return port
