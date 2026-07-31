"""Metric contract and the METRICS_PORT gate in common.metrics."""

from __future__ import annotations

import socket
import urllib.request

import pytest
from prometheus_client import REGISTRY

from common import metrics


def test_no_server_when_metrics_port_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("METRICS_PORT", raising=False)
    assert metrics.maybe_start_metrics_server("unit-test") is None


def test_no_server_when_metrics_port_empty_or_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("METRICS_PORT", "  ")
    assert metrics.maybe_start_metrics_server("unit-test") is None
    monkeypatch.setenv("METRICS_PORT", "0")
    assert metrics.maybe_start_metrics_server("unit-test") is None


def test_server_starts_and_serves_sp_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    monkeypatch.setenv("METRICS_PORT", str(port))
    assert metrics.maybe_start_metrics_server("unit-test") == port
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    assert 'sp_up{service="unit-test"} 1.0' in body


def test_metric_families_carry_the_contract_names() -> None:
    """The sp_ contract: every family name is fixed; a rename is a defect."""
    metrics.MESSAGES_PROCESSED.labels(service="contract", topic="t").inc()
    metrics.DLQ_MESSAGES.labels(service="contract", topic="t.dlq").inc()
    metrics.TRANSACTIONS_COMMITTED.labels(service="contract").inc()
    metrics.TRANSACTIONS_ABORTED.labels(service="contract").inc()
    metrics.SESSIONS_EMITTED.labels(service="contract").inc(2)
    metrics.ROWS_WRITTEN.labels(service="contract", table="raw.t").inc(3)
    metrics.EVENTS_PRODUCED.labels(service="contract", topic="t").inc()
    metrics.EVENTS_ACK_FAILED.labels(service="contract", topic="t").inc()
    metrics.PROCESSING_LATENCY.labels(service="contract").observe(0.5)
    metrics.STATE_JOURNAL_ROWS.labels(service="contract").set(7)
    metrics.STATE_CLOSES_ROWS.labels(service="contract").set(4)
    metrics.CONSUMER_LAG.labels(group="g", topic="t", partition="0").set(11)

    labels = {"service": "contract"}
    assert REGISTRY.get_sample_value("sp_messages_processed_total", {**labels, "topic": "t"}) == 1
    assert REGISTRY.get_sample_value("sp_dlq_messages_total", {**labels, "topic": "t.dlq"}) == 1
    assert REGISTRY.get_sample_value("sp_transactions_committed_total", labels) == 1
    assert REGISTRY.get_sample_value("sp_transactions_aborted_total", labels) == 1
    assert REGISTRY.get_sample_value("sp_sessions_emitted_total", labels) == 2
    assert REGISTRY.get_sample_value("sp_rows_written_total", {**labels, "table": "raw.t"}) == 3
    assert REGISTRY.get_sample_value("sp_events_produced_total", {**labels, "topic": "t"}) == 1
    assert REGISTRY.get_sample_value("sp_events_ack_failed_total", {**labels, "topic": "t"}) == 1
    assert REGISTRY.get_sample_value("sp_processing_latency_seconds_count", labels) == 1
    assert REGISTRY.get_sample_value("sp_state_journal_rows", labels) == 7
    assert REGISTRY.get_sample_value("sp_state_closes_rows", labels) == 4
    assert (
        REGISTRY.get_sample_value("sp_consumer_lag", {"group": "g", "topic": "t", "partition": "0"})
        == 11
    )
