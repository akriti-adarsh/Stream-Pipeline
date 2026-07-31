"""Great Expectations runner: a pipeline step with teeth, not decoration.

Runs the two suites against live Postgres, writes every expectation result to
``serving.dq_results`` (machine-readable, queryable, the dashboard's DQ strip
reads it), exposes pass-rate gauges for Prometheus, and honors a failure mode:

- ``--mode warn``: failures are recorded and logged, exit code 0
- ``--mode fail``: any failed expectation exits 2 (CI gates on this)

``--interval N`` loops forever every N seconds (the compose service mode);
``--serve-metrics PORT`` starts the Prometheus endpoint (default 9109). The
row-count baseline for fct_rides is ROLLING: each run stores its observed
count in dq_results and the next run derives its bounds from that history
(see quality.suites.rowcount_bounds).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any

import great_expectations as gx
import psycopg
from prometheus_client import Counter, Gauge, start_http_server

from common.logging import configure_logging, with_ctx
from quality.suites import build_suite, fct_rides_expectations, raw_rides_events_expectations

DQ_DDL = """
CREATE SCHEMA IF NOT EXISTS serving;
CREATE TABLE IF NOT EXISTS serving.dq_results (
    id bigserial PRIMARY KEY,
    run_id text NOT NULL,
    suite text NOT NULL,
    expectation text NOT NULL,
    column_name text,
    success boolean NOT NULL,
    element_count bigint,
    unexpected_count bigint,
    observed text,
    mode text NOT NULL,
    run_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dq_results_run ON serving.dq_results (suite, run_at);
"""

PASS_RATE = Gauge("sp_dq_pass_rate", "Share of expectations passing", ["suite"])
FAILED = Gauge("sp_dq_failed_expectations", "Failed expectations in the last run", ["suite"])
RUNS = Counter("sp_dq_runs_total", "Quality runs executed", ["suite", "outcome"])


@dataclass
class SuiteOutcome:
    suite: str
    passed: int
    failed: int
    rows: list[tuple[Any, ...]]

    @property
    def pass_rate(self) -> float:
        total = self.passed + self.failed
        return 1.0 if total == 0 else self.passed / total


def result_rows(
    run_id: str, suite_name: str, mode: str, results: list[Any]
) -> list[tuple[Any, ...]]:
    """Flatten a GX validation result into dq_results rows."""
    rows: list[tuple[Any, ...]] = []
    for result in results:
        config = result["expectation_config"]
        outcome = result.get("result", {}) or {}
        observed = outcome.get("observed_value")
        rows.append(
            (
                run_id,
                suite_name,
                config["type"],
                (config.get("kwargs") or {}).get("column"),
                bool(result["success"]),
                outcome.get("element_count"),
                outcome.get("unexpected_count"),
                None if observed is None else json.dumps(observed, default=str),
                mode,
            )
        )
    return rows


class QualityRunner:
    def __init__(self, dsn: str, mode: str, freshness_minutes: int) -> None:
        self._dsn = dsn
        self._mode = mode
        self._freshness_minutes = freshness_minutes
        self._log = configure_logging("dq-runner")
        self._conn = psycopg.connect(dsn)
        with self._conn.cursor() as cur:
            cur.execute(DQ_DDL)
        self._conn.commit()

    # ------------------------------------------------------------------ state

    def rowcount_history(self, suite: str = "fct_rides") -> list[int]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT observed FROM serving.dq_results"
                " WHERE suite = %s AND expectation = 'expect_table_row_count_to_be_between'"
                " AND observed IS NOT NULL ORDER BY run_at DESC LIMIT 10",
                (suite,),
            )
            values: list[int] = []
            for (observed,) in cur.fetchall():
                try:
                    values.append(int(json.loads(observed)))
                except (ValueError, TypeError):
                    continue
        self._conn.commit()
        return list(reversed(values))

    # -------------------------------------------------------------------- run

    def run_once(self) -> list[SuiteOutcome]:
        run_id = uuid.uuid4().hex[:12]
        context = gx.get_context(mode="ephemeral")
        source = context.data_sources.add_postgres(
            name="pg", connection_string=self._sqlalchemy_dsn()
        )
        specs = [
            ("raw_rides_events", "raw", "rides_events", raw_rides_events_expectations()),
            (
                "fct_rides",
                "analytics_marts",
                "fct_rides",
                fct_rides_expectations(self.rowcount_history(), self._freshness_minutes),
            ),
        ]
        outcomes: list[SuiteOutcome] = []
        for suite_name, schema, table, expectations in specs:
            asset = source.add_table_asset(name=suite_name, table_name=table, schema_name=schema)
            batch = asset.add_batch_definition_whole_table(f"{suite_name}-all")
            suite = context.suites.add(build_suite(f"{suite_name}-suite", expectations))
            definition = context.validation_definitions.add(
                gx.ValidationDefinition(data=batch, suite=suite, name=f"{suite_name}-vd")
            )
            validation = definition.run()
            rows = result_rows(run_id, suite_name, self._mode, list(validation.results))
            passed = sum(1 for row in rows if row[4])
            outcome = SuiteOutcome(suite_name, passed, len(rows) - passed, rows)
            outcomes.append(outcome)
            self._persist(outcome)
            PASS_RATE.labels(suite=suite_name).set(outcome.pass_rate)
            FAILED.labels(suite=suite_name).set(outcome.failed)
            RUNS.labels(suite=suite_name, outcome="pass" if outcome.failed == 0 else "fail").inc()
            self._log.info(
                "suite finished",
                extra=with_ctx(
                    suite=suite_name,
                    passed=outcome.passed,
                    failed=outcome.failed,
                    pass_rate=round(outcome.pass_rate, 4),
                    run_id=run_id,
                ),
            )
        return outcomes

    def _persist(self, outcome: SuiteOutcome) -> None:
        with self._conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO serving.dq_results (run_id, suite, expectation, column_name,"
                " success, element_count, unexpected_count, observed, mode)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                outcome.rows,
            )
        self._conn.commit()

    def _sqlalchemy_dsn(self) -> str:
        return self._dsn.replace("postgresql://", "postgresql+psycopg://", 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dq-runner", description=__doc__)
    parser.add_argument("--dsn", default="postgresql://stream:stream@localhost:5433/stream")
    parser.add_argument("--mode", choices=["warn", "fail"], default="warn")
    parser.add_argument("--interval", type=float, default=0.0, help="loop every N seconds; 0 once")
    parser.add_argument("--serve-metrics", type=int, default=0, help="Prometheus port; 0 off")
    parser.add_argument("--freshness-minutes", type=int, default=30)
    args = parser.parse_args(argv)

    if args.serve_metrics:
        start_http_server(args.serve_metrics)
    runner = QualityRunner(args.dsn, args.mode, args.freshness_minutes)
    while True:
        outcomes = runner.run_once()
        failed = sum(outcome.failed for outcome in outcomes)
        if args.interval <= 0:
            return 2 if (failed and args.mode == "fail") else 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
