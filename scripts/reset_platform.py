"""Reset the platform to a blank slate: topics, consumer groups, Postgres, state.

Used by the exactly-once kill test and the e2e harness so every run starts
from zero. Destructive by design; only ever pointed at the local compose
stack.
"""

from __future__ import annotations

import argparse
import contextlib
import shutil
import sys
import time
from pathlib import Path

import psycopg
from confluent_kafka.admin import AdminClient

from common.kafka import TOPIC_PARTITIONS, ensure_topics

RAW_TABLES = (
    "raw.rides_events",
    "raw.ride_sessions",
    "raw.payments_transactions",
    "raw.driver_locations",
)


def reset_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    doomed = [t for t in TOPIC_PARTITIONS if t in existing]
    if doomed:
        for future in admin.delete_topics(doomed, operation_timeout=30).values():
            future.result(timeout=30)
    # Deletion is async on the broker; wait until they are really gone.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        left = set(admin.list_topics(timeout=10).topics) & set(TOPIC_PARTITIONS)
        if not left:
            break
        time.sleep(0.5)
    ensure_topics(bootstrap)
    # Recreation is async too: a produce burst against leaderless partitions
    # fails deliveries and silently shrinks the acked set. Wait for leaders.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=10)
        ready = True
        for topic, partitions in TOPIC_PARTITIONS.items():
            meta = metadata.topics.get(topic)
            if meta is None or len(meta.partitions) != partitions:
                ready = False
                break
            if any(p.leader < 0 for p in meta.partitions.values()):
                ready = False
                break
        if ready:
            return
        time.sleep(0.5)
    raise RuntimeError("topics recreated but leaders not ready within 30s")


def reset_groups(
    bootstrap: str,
    groups: tuple[str, ...] = ("sessionizer", "pg-sink"),
    wait_sec: float = 90.0,
) -> None:
    """Delete the processor consumer groups, waiting out zombie members.

    A hard-killed consumer never leaves its group; the broker only expires it
    after session.timeout.ms (45 s by default). Right after a kill test the
    group is therefore briefly "not empty" with no live process behind it, so
    NON_EMPTY_GROUP gets a bounded retry instead of an instant failure.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + wait_sec
    remaining = set(groups)
    last_error: Exception | None = None
    while remaining and time.monotonic() < deadline:
        try:
            listed = admin.list_consumer_groups(request_timeout=10).result(10)
            known = {g.group_id for g in listed.valid}
        except Exception:
            known = set()
        remaining &= known
        if not remaining:
            return
        for group, future in admin.delete_consumer_groups(
            list(remaining), request_timeout=30
        ).items():
            try:
                future.result(timeout=30)
                remaining.discard(group)
            except Exception as exc:
                if "NON_EMPTY_GROUP" in str(exc):
                    last_error = exc  # zombie member still inside its session timeout
                else:
                    raise RuntimeError(f"cannot delete consumer group {group}: {exc}") from exc
        if remaining:
            time.sleep(3.0)
    if remaining:
        raise RuntimeError(
            f"groups still not deletable after {wait_sec}s: {remaining}"
        ) from last_error


def reset_postgres(dsn: str) -> None:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for table in RAW_TABLES:
            cur.execute(f"TRUNCATE {table}")  # fixed identifiers, not user input
        cur.execute("TRUNCATE serving.consumer_offsets")
        conn.commit()


def reset_state_dir(path: str) -> None:
    state = Path(path)
    if state.exists():
        shutil.rmtree(state)
    state.mkdir(parents=True, exist_ok=True)


def reset_all(bootstrap: str, dsn: str, state_dir: str) -> None:
    reset_groups(bootstrap)
    reset_topics(bootstrap)
    # First boot: the sink has not created its tables yet.
    with contextlib.suppress(psycopg.errors.UndefinedTable):
        reset_postgres(dsn)
    reset_state_dir(state_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--dsn", default="postgresql://stream:stream@localhost:5433/stream")
    parser.add_argument("--state-dir", default="state")
    args = parser.parse_args()
    reset_all(args.bootstrap, args.dsn, args.state_dir)
    print("platform reset: topics, groups, postgres raw tables, state dir")
    return 0


if __name__ == "__main__":
    sys.exit(main())
