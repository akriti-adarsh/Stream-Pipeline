"""THE test: SIGKILL the processors mid-batch and prove exactly-once anyway.

Protocol per run (spec section 10 + review round 3):

1. Reset the platform to zero (topics, groups, Postgres, state).
2. Produce a known stream with imperfections OFF, recording every event id the
   broker ACKED to a manifest. The acked set is the only honest definition of
   "what was produced".
3. Start the sessionizer and the Postgres sink as real subprocesses.
4. At a run-specific point, hard-kill a processor (TerminateProcess on
   Windows, the SIGKILL equivalent: no cleanup, no atexit, no flush).
5. Restart it and wait until Postgres stops changing.
6. Assert raw.rides_events holds EXACTLY the acked event-id set: no loss, no
   duplicates. Assert every terminal ride has exactly one session row.

The parametrised kill points cover: the sink mid-write, the sessionizer inside
its transaction window, and both at once. A fourth variant (duplicates ON)
proves the idempotent upsert dedupes under crash, not just under clean replay.

Run against the CORE profile only (CI does): each test deletes and recreates
the topics, and live Flink jobs consuming them turn into crash-looping
reconnect storms that starve the single-core dev broker; the resulting
metadata timeouts fail the harness (never the exactly-once assertions, but a
red run is a red run). The compose file disables broker topic auto-creation
for the same reason: a live consumer must not resurrect a deleted topic with
default partitions underneath the reset.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from reset_platform import reset_all

pytestmark = [pytest.mark.integration, pytest.mark.timeout(600)]

BOOTSTRAP = "localhost:19092"
DSN = "postgresql://stream:stream@localhost:5433/stream"
ANCHOR_0830_UTC = 1_767_255_000_000  # 2026-01-01T08:10:00Z, near the morning peak
REPO = Path(__file__).resolve().parents[2]

RIDE_TERMINAL_SEQS = {"5", "6"}  # completed, cancelled


def _run_generator(state_dir: Path, *, with_duplicates: bool) -> set[str]:
    """Produce the known stream, return the broker-acked rides.events ids."""
    manifest = state_dir / "acked.jsonl"
    cmd = [
        sys.executable,
        "-m",
        "generator",
        "--sink",
        "kafka",
        "--no-pace",
        "--speed",
        "60",
        "--duration",
        "45",
        "--seed",
        "1337",
        "--anchor",
        str(ANCHOR_0830_UTC),
        "--rides-per-min",
        "40",
        "--drivers",
        "40",
        "--acked-manifest",
        str(manifest),
    ]
    if with_duplicates:
        # Duplicates ON (same event_id re-sent), everything else off: proves
        # the idempotent upsert under crash, per review round 3.
        cmd += [
            "--dup-rate",
            "0.02",
            "--ooo-rate",
            "0",
            "--late-rate",
            "0",
            "--malformed-rate",
            "0",
            "--null-rate",
            "0",
            "--abandon-rate",
            "0",
        ]
    else:
        cmd.append("--perfect")
    subprocess.run(cmd, cwd=REPO, check=True, capture_output=True, timeout=240)
    acked: set[str] = set()
    with manifest.open(encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            if record["topic"] == "rides.events":
                acked.add(record["id"])
    return acked


def _spawn(module: str, env_extra: dict[str, str], log_dir: Path) -> subprocess.Popen[bytes]:
    env = {**os.environ, **env_extra}
    log_path = log_dir / f"{module.rsplit('.', 1)[-1]}-{int(time.time() * 1000)}.log"
    log_file = log_path.open("ab")
    return subprocess.Popen(
        [sys.executable, "-m", module],
        cwd=REPO,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )


def _pg_snapshot() -> tuple[int, int, int]:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT event_id) FROM raw.rides_events")
        total, distinct = cur.fetchone()
        cur.execute("SELECT count(*) FROM raw.ride_sessions")
        sessions = cur.fetchone()[0]
        return int(total), int(distinct), int(sessions)


def _wait_for_stability(
    expected_events: int, expected_sessions: int, timeout_sec: float = 300.0
) -> tuple[int, int, int]:
    """Wait until events AND sessions reach their expected counts and hold
    still, or time out. Waiting on both matters: a consumer mid-recovery can
    go quiet for tens of seconds, and returning on the first lull would fail
    the run with a premature snapshot rather than a real invariant breach.
    On timeout the last snapshot returns and the assertions tell the truth."""
    deadline = time.monotonic() + timeout_sec
    last = (-1, -1, -1)
    stable_since = time.monotonic()
    while time.monotonic() < deadline:
        snap = _pg_snapshot()
        if snap != last:
            last = snap
            stable_since = time.monotonic()
        elif (
            snap[0] >= expected_events
            and snap[2] >= expected_sessions
            and time.monotonic() - stable_since > 12.0
        ):
            return snap
        time.sleep(2.0)
    return last


def _pg_event_ids() -> set[str]:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT event_id FROM raw.rides_events")
        return {row[0] for row in cur.fetchall()}


@pytest.mark.parametrize(
    ("kill_target", "kill_after_sec", "with_duplicates"),
    [
        ("pg-sink", 3.0, False),
        ("sessionizer", 5.0, False),
        ("both", 7.0, False),
        ("pg-sink", 4.0, True),
    ],
    ids=["kill-sink-early", "kill-sessionizer-mid", "kill-both-late", "kill-sink-with-dupes"],
)
def test_exactly_once_survives_sigkill(
    tmp_path: Path, kill_target: str, kill_after_sec: float, with_duplicates: bool
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    reset_all(BOOTSTRAP, DSN, str(state_dir))

    acked = _run_generator(state_dir, with_duplicates=with_duplicates)
    assert len(acked) > 5_000, f"only {len(acked)} events acked; stream too small to prove much"

    env = {
        "KAFKA_BOOTSTRAP": BOOTSTRAP,
        "SCHEMA_REGISTRY_URL": "http://localhost:18081",
        "POSTGRES_DSN": DSN,
        "STATE_PATH": str(state_dir / "sessionizer.db"),
    }
    log_dir = state_dir
    procs: dict[str, subprocess.Popen[bytes]] = {
        "sessionizer": _spawn("processors.ride_sessionizer", env, log_dir),
        "pg-sink": _spawn("processors.pg_sink", env, log_dir),
    }
    try:
        time.sleep(kill_after_sec)
        victims = ["sessionizer", "pg-sink"] if kill_target == "both" else [kill_target]
        for name in victims:
            procs[name].kill()  # TerminateProcess: the Windows SIGKILL, no cleanup runs
            procs[name].wait(timeout=30)
        jitter = random.uniform(0.5, 2.0)
        time.sleep(jitter)
        for name in victims:
            procs[name] = _spawn(
                "processors.ride_sessionizer" if name == "sessionizer" else "processors.pg_sink",
                env,
                log_dir,
            )

        terminal_rides = {
            event_id.rsplit(".", 1)[0]
            for event_id in acked
            if event_id.rsplit(".", 1)[1] in RIDE_TERMINAL_SEQS
        }
        total, distinct, sessions = _wait_for_stability(
            expected_events=len(acked), expected_sessions=len(terminal_rides)
        )

        for name, proc in procs.items():
            if proc.poll() is not None:
                logs = "\n".join(
                    f.read_text(errors="replace")[-2000:] for f in sorted(log_dir.glob("*.log"))
                )
                raise AssertionError(
                    f"{name} exited unexpectedly with rc={proc.returncode}; logs:\n{logs}"
                )

        assert total == distinct, f"duplicate rows in Postgres: {total} rows, {distinct} distinct"
        assert total == len(acked), f"count mismatch: pg={total} acked={len(acked)}"
        assert _pg_event_ids() == acked, "Postgres holds a different event set than was acked"

        with psycopg.connect(DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), count(DISTINCT ride_id) FROM raw.ride_sessions")
            session_rows, session_rides = cur.fetchone()
        assert session_rows == session_rides, "duplicate session rows"
        assert sessions == len(terminal_rides), (
            f"sessions {sessions} != terminal rides {len(terminal_rides)}"
        )
    finally:
        for proc in procs.values():
            proc.kill()
