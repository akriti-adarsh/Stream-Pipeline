"""State store crash-consistency: the offset-tag rollback contract."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from processors.state_store import CloseRow, JournalRow, StateStore


def _event(ride_id: str, event_type: str, ts: int) -> dict[str, Any]:
    return {"ride_id": ride_id, "event_type": event_type, "event_ts": ts}


def _rows(partition: int, start_offset: int, *specs: tuple[str, str, int]) -> list[JournalRow]:
    return [
        JournalRow(partition, start_offset + i, ride_id, _event(ride_id, kind, ts))
        for i, (ride_id, kind, ts) in enumerate(specs)
    ]


def test_append_and_open_rides_roundtrip(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    store.append_batch(
        _rows(
            0, 100, ("r-1", "requested", 1000), ("r-2", "requested", 1100), ("r-1", "matched", 1200)
        ),
        closes=[],
    )
    open_rides = store.open_rides(0)
    assert set(open_rides) == {"r-1", "r-2"}
    assert [e["event_type"] for e in open_rides["r-1"]] == ["requested", "matched"]
    store.close()


def test_closed_rides_are_excluded_from_open(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    store.append_batch(
        _rows(0, 100, ("r-1", "requested", 1000), ("r-1", "completed", 2000)),
        closes=[CloseRow("r-1", 0, 101, {"ride_id": "r-1", "terminal_state": "completed"})],
    )
    assert store.open_rides(0) == {}
    assert store.is_closed("r-1")
    store.close()


def test_rollback_from_erases_uncommitted_batch(tmp_path: Path) -> None:
    """The review-round-4 window: SQLite committed, Kafka transaction did not."""
    store = StateStore(tmp_path / "state.db")
    store.append_batch(_rows(0, 100, ("r-1", "requested", 1000)), closes=[])
    # Batch 2 (offsets 101..102) closes r-1; the Kafka txn for it will abort.
    store.append_batch(
        _rows(0, 101, ("r-1", "started", 1500), ("r-1", "completed", 2000)),
        closes=[CloseRow("r-1", 0, 102, {"ride_id": "r-1", "terminal_state": "completed"})],
    )
    assert store.is_closed("r-1")
    # Restart: Kafka's committed resume position is 101.
    journal_deleted, closes_deleted = store.rollback_from(0, 101)
    assert (journal_deleted, closes_deleted) == (2, 1)
    assert not store.is_closed("r-1")
    open_rides = store.open_rides(0)
    assert [e["event_type"] for e in open_rides["r-1"]] == ["requested"]
    store.close()


def test_purge_confirmed_only_drops_committed_closes(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    store.append_batch(
        _rows(
            0,
            100,
            ("r-1", "requested", 1000),
            ("r-1", "completed", 1500),
            ("r-2", "requested", 1600),
        ),
        closes=[CloseRow("r-1", 0, 101, {"ride_id": "r-1"})],
    )
    # Resume position 102: the close at 101 is confirmed committed.
    purged = store.purge_confirmed(0, 102)
    assert purged == 2  # both r-1 journal rows
    journal_count, closes_count = store.counts()
    assert (journal_count, closes_count) == (1, 1)  # r-2 row + r-1 tombstone
    # A close AT the resume position would not be purged.
    store.append_batch(
        _rows(0, 200, ("r-3", "requested", 2000), ("r-3", "cancelled", 2100)),
        closes=[CloseRow("r-3", 0, 201, {"ride_id": "r-3"})],
    )
    assert store.purge_confirmed(0, 201) == 0
    store.close()


def test_partitions_are_isolated(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    store.append_batch(_rows(0, 10, ("r-a", "requested", 1000)), closes=[])
    store.append_batch(_rows(1, 10, ("r-b", "requested", 1000)), closes=[])
    store.rollback_from(0, 0)
    assert store.open_rides(0) == {}
    assert set(store.open_rides(1)) == {"r-b"}
    store.close()


def test_state_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    store = StateStore(path)
    store.append_batch(_rows(0, 5, ("r-9", "requested", 7000)), closes=[])
    store.close()
    reopened = StateStore(path)
    assert set(reopened.open_rides(0)) == {"r-9"}
    assert reopened.max_journal_ts(0) == 7000
    reopened.close()


def test_replayed_batch_is_idempotent(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    batch = _rows(0, 50, ("r-5", "requested", 1000), ("r-5", "matched", 1500))
    store.append_batch(batch, closes=[])
    store.append_batch(batch, closes=[])  # replay after abort+rollback elsewhere
    journal_count, _ = store.counts()
    assert journal_count == 2
    store.close()


def test_max_journal_ts_empty_partition(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.db")
    assert store.max_journal_ts(3) is None
    store.close()
