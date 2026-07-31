"""Schema evolution end to end: a consumer started on v1 traffic survives the
switch to v2 mid-stream and populates promo_code for v2 records."""

from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
import pytest

from processors.pg_sink import PgSink, PgSinkConfig
from processors.ride_sessionizer import Sessionizer, SessionizerConfig

pytestmark = [pytest.mark.integration, pytest.mark.timeout(420)]

DSN = "postgresql://stream:stream@localhost:5433/stream"
REPO = Path(__file__).resolve().parents[2]


def test_consumer_survives_v1_to_v2_evolution(tmp_path: Path) -> None:
    # A dedicated anchor keys this run's data so assertions target it alone.
    anchor_ms = 1_700_000_000_000 + (uuid.uuid4().int % 1000) * 86_400_000
    subprocess.run(
        [
            sys.executable,
            "-m",
            "generator",
            "--sink",
            "kafka",
            "--no-pace",
            "--speed",
            "60",
            "--duration",
            "40",
            "--seed",
            "4242",
            "--anchor",
            str(anchor_ms),
            "--rides-per-min",
            "30",
            "--drivers",
            "20",
            "--evolve-after",
            "12",
            "--perfect",
        ],
        cwd=REPO,
        check=True,
        capture_output=True,
        timeout=240,
    )

    # The sessionizer reads with the v2 reader schema across the boundary: it
    # must not crash, and it must keep closing rides on both sides of it.
    sess = Sessionizer(SessionizerConfig(state_path=str(tmp_path / "s.db")))
    sess.run(idle_timeout_sec=12.0)
    assert sess.sessions_emitted > 0

    sink = PgSink(PgSinkConfig())
    sink.run(idle_timeout_sec=12.0)

    lo = anchor_ms
    hi = anchor_ms + 40 * 60 * 1000 + 3_600_000
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT payload_version, count(*), count(promo_code)"
            " FROM raw.rides_events"
            " WHERE event_ts >= to_timestamp(%s / 1000.0)"
            " AND event_ts < to_timestamp(%s / 1000.0)"
            " GROUP BY payload_version ORDER BY payload_version",
            (lo, hi),
        )
        by_version = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

    assert 1 in by_version, "no v1 rows: evolution boundary produced nothing before it"
    assert 2 in by_version, "no v2 rows: evolution never happened"
    v1_rows, v1_promos = by_version[1]
    v2_rows, v2_promos = by_version[2]
    assert v1_rows > 0 and v1_promos == 0, "v1 records can never carry promo_code"
    assert v2_rows > 0
    assert v2_promos > 0, "v2 records must populate promo_code for promo rides"
