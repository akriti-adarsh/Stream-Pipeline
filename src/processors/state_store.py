"""Offset-tagged SQLite state store for the sessionizer.

Why this design exists (the two-store gap, spec review round 4): the Kafka
transaction covers produced session records and consumed offsets, but this
local store is a SECOND store. Its commits can land ahead of a Kafka
transaction that then aborts because the process died in between. The store
therefore never keeps derived state it cannot roll back:

- every journal row and every close is tagged with the (partition, offset) of
  the input event that caused it;
- on startup, after Kafka's transactionally-committed offsets determine the
  resume position, :meth:`rollback_from` deletes anything tagged at or beyond
  that position, erasing the half-applied batch as if it never happened;
- journal rows for a ride are only purged once its close is CONFIRMED, i.e.
  tagged strictly below the committed resume position; closes at or beyond it
  are rolled back along with their journal rows, so the ride simply reopens
  and is re-derived when the batch replays.

SQLite runs in WAL mode with synchronous=FULL so a committed batch survives a
hard process kill; the roll-forward/roll-back decision then belongs entirely
to Kafka's committed offsets, which is the whole point.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class JournalRow:
    partition: int
    offset: int
    ride_id: str
    value: dict[str, Any]


@dataclass(frozen=True)
class CloseRow:
    ride_id: str
    partition: int
    offset: int
    session: dict[str, Any]


class StateStore:
    def __init__(self, path: Path | str) -> None:
        self._path = str(path)
        if self._path != ":memory:":
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self._path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS journal (
                partition INTEGER NOT NULL,
                kafka_offset INTEGER NOT NULL,
                ride_id TEXT NOT NULL,
                event_json TEXT NOT NULL,
                PRIMARY KEY (partition, kafka_offset)
            );
            CREATE INDEX IF NOT EXISTS journal_ride ON journal (ride_id);
            CREATE TABLE IF NOT EXISTS closes (
                ride_id TEXT PRIMARY KEY,
                partition INTEGER NOT NULL,
                kafka_offset INTEGER NOT NULL,
                session_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS closes_position ON closes (partition, kafka_offset);
            """
        )
        self._db.commit()

    # ----------------------------------------------------------------- writes

    def append_batch(self, journal: list[JournalRow], closes: list[CloseRow]) -> None:
        """Persist one processed batch atomically, tagged with its offsets.

        INSERT OR IGNORE on the journal: a replayed batch (after a Kafka abort
        and rollback) re-inserts identical rows; a duplicate event occupying
        the same (partition, offset) cannot happen, and the same event content
        arriving at a NEW offset is a legitimate broker-level duplicate that
        the fold layer dedupes by event id.
        """
        with self._db:
            self._db.executemany(
                "INSERT OR IGNORE INTO journal (partition, kafka_offset, ride_id, event_json)"
                " VALUES (?, ?, ?, ?)",
                [(r.partition, r.offset, r.ride_id, json.dumps(r.value)) for r in journal],
            )
            self._db.executemany(
                "INSERT OR REPLACE INTO closes (ride_id, partition, kafka_offset, session_json)"
                " VALUES (?, ?, ?, ?)",
                [(c.ride_id, c.partition, c.offset, json.dumps(c.session)) for c in closes],
            )

    def rollback_from(self, partition: int, resume_offset: int) -> tuple[int, int]:
        """Erase every trace of offsets >= resume_offset on this partition.

        Called on assignment, BEFORE consuming: anything the store learned
        from offsets the Kafka transaction did not commit is deleted, closing
        the crash window between the SQLite commit and commit_transaction.
        Returns (journal_rows_deleted, closes_deleted).
        """
        with self._db:
            j = self._db.execute(
                "DELETE FROM journal WHERE partition = ? AND kafka_offset >= ?",
                (partition, resume_offset),
            ).rowcount
            c = self._db.execute(
                "DELETE FROM closes WHERE partition = ? AND kafka_offset >= ?",
                (partition, resume_offset),
            ).rowcount
        return j, c

    def purge_confirmed(self, partition: int, below_offset: int) -> int:
        """Drop journal rows of rides whose close is confirmed committed
        (close tagged strictly below the Kafka-committed resume position)."""
        with self._db:
            count = self._db.execute(
                "DELETE FROM journal WHERE ride_id IN ("
                " SELECT ride_id FROM closes WHERE partition = ? AND kafka_offset < ?)",
                (partition, below_offset),
            ).rowcount
        return count

    # ------------------------------------------------------------------ reads

    def open_rides(self, partition: int) -> dict[str, list[dict[str, Any]]]:
        """Journal events of rides on this partition without a recorded close,
        in offset order: the fold input for startup recovery."""
        rows = self._db.execute(
            "SELECT j.ride_id, j.event_json FROM journal j"
            " WHERE j.partition = ? AND j.ride_id NOT IN (SELECT ride_id FROM closes)"
            " ORDER BY j.kafka_offset",
            (partition,),
        ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for ride_id, event_json in rows:
            grouped.setdefault(ride_id, []).append(json.loads(event_json))
        return grouped

    def is_closed(self, ride_id: str) -> bool:
        row = self._db.execute("SELECT 1 FROM closes WHERE ride_id = ?", (ride_id,)).fetchone()
        return row is not None

    def max_journal_ts(self, partition: int) -> int | None:
        """Highest event_ts recorded on this partition (watermark recovery)."""
        row = self._db.execute(
            "SELECT MAX(json_extract(event_json, '$.event_ts')) FROM journal WHERE partition = ?",
            (partition,),
        ).fetchone()
        return None if row is None or row[0] is None else int(row[0])

    def counts(self) -> tuple[int, int]:
        j = self._db.execute("SELECT COUNT(*) FROM journal").fetchone()[0]
        c = self._db.execute("SELECT COUNT(*) FROM closes").fetchone()[0]
        return int(j), int(c)

    def close(self) -> None:
        self._db.close()
