"""Property and statistical tests for the ride simulator.

The central claim of the generator is coherence: for ANY seed, no ride ever
emits an illegal transition, timestamps never run backwards within a ride, and
payments always trail completion by the documented window.
"""

from __future__ import annotations

from collections import defaultdict
from itertools import pairwise
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from common import topics
from generator.config import GeneratorConfig
from generator.events import SourceEvent
from generator.simulate import simulate, ticks_for
from generator.state_machine import LEGAL_TRANSITIONS, TERMINAL_STATES, RideEventType

ANCHOR_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z, fixed for reproducibility

FAST_CFG = GeneratorConfig(
    seed=42, anchor_ms=ANCHOR_MS, base_rides_per_min=6.0, duration_sec=60.0, speed=60.0
)


def _events(cfg: GeneratorConfig, ticks: int) -> list[SourceEvent]:
    return list(simulate(cfg, ticks=ticks))


def _rides(events: list[SourceEvent]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if event.topic == topics.RIDES_EVENTS:
            grouped[event.value["ride_id"]].append(event.value)
    return grouped


@settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(seed=st.integers(min_value=0, max_value=2**32 - 1))
def test_no_seed_produces_an_illegal_ride(seed: int) -> None:
    cfg = GeneratorConfig(seed=seed, anchor_ms=ANCHOR_MS, base_rides_per_min=6.0)
    for ride_events in _rides(_events(cfg, ticks=1800)).values():
        kinds = [RideEventType(e["event_type"]) for e in ride_events]
        assert kinds[0] is RideEventType.REQUESTED
        for prev, nxt in pairwise(kinds):
            assert nxt in LEGAL_TRANSITIONS[prev], f"illegal {prev.value} -> {nxt.value}"
        assert sum(1 for k in kinds if k in TERMINAL_STATES) <= 1
        timestamps = [e["event_ts"] for e in ride_events]
        assert timestamps == sorted(timestamps), "time ran backwards within a ride"
        seqs = [int(e["event_id"].split(".")[-1]) for e in ride_events]
        assert len(set(seqs)) == len(seqs), "duplicate event_seq within a ride"


def test_cancellation_rate_is_realistic() -> None:
    events = _events(GeneratorConfig(seed=7, anchor_ms=ANCHOR_MS, base_rides_per_min=32.0), 3600)
    rides = _rides(events)
    terminal = {
        rid: RideEventType(evs[-1]["event_type"])
        for rid, evs in rides.items()
        if RideEventType(evs[-1]["event_type"]) in TERMINAL_STATES
    }
    assert len(terminal) > 300, "not enough closed rides to measure"
    cancel_share = sum(1 for t in terminal.values() if t is RideEventType.CANCELLED) / len(terminal)
    assert 0.03 < cancel_share < 0.15, cancel_share


def test_every_completed_ride_gets_exactly_one_payment_in_window() -> None:
    events = _events(GeneratorConfig(seed=11, anchor_ms=ANCHOR_MS, base_rides_per_min=20.0), 5400)
    completed_at: dict[str, int] = {}
    fares: dict[str, int] = {}
    for event in events:
        if (
            event.topic == topics.RIDES_EVENTS
            and event.value["event_type"] == RideEventType.COMPLETED.value
        ):
            completed_at[event.value["ride_id"]] = event.value["event_ts"]
            fares[event.value["ride_id"]] = event.value["fare_cents"]
    payments: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if event.topic == topics.PAYMENTS_TRANSACTIONS:
            payments[event.value["ride_id"]].append(event.value)

    assert len(completed_at) > 100
    # Payments near the end of the window may still be queued when the run stops;
    # every payment that DID arrive must belong to a completed ride, once, in window.
    for ride_id, txns in payments.items():
        assert ride_id in completed_at
        assert len(txns) == 1
        delay_sec = (txns[0]["ts"] - completed_at[ride_id]) / 1000
        assert 5.0 <= delay_sec <= 90.0
        assert txns[0]["amount_cents"] == fares[ride_id]
    # And the vast majority of rides completed early enough must have been paid.
    horizon = ANCHOR_MS + (5400 - 120) * 1000
    early_completed = {r for r, ts in completed_at.items() if ts < horizon}
    assert early_completed, "no early completions to check"
    paid = sum(1 for r in early_completed if r in payments)
    assert paid == len(early_completed)


def test_same_seed_same_stream_different_seed_different_stream() -> None:
    run_a = _events(FAST_CFG, ticks=600)
    run_b = _events(FAST_CFG, ticks=600)
    assert run_a == run_b
    run_c = _events(
        GeneratorConfig(seed=43, anchor_ms=ANCHOR_MS, base_rides_per_min=6.0), ticks=600
    )
    assert run_a != run_c


def test_fares_present_only_on_completed() -> None:
    events = _events(GeneratorConfig(seed=3, anchor_ms=ANCHOR_MS, base_rides_per_min=20.0), 3600)
    for event in events:
        if event.topic != topics.RIDES_EVENTS:
            continue
        if event.value["event_type"] == RideEventType.COMPLETED.value:
            assert event.value["fare_cents"] is not None
            assert event.value["fare_cents"] > 0
        else:
            assert event.value["fare_cents"] is None


def test_driver_absent_until_matched() -> None:
    events = _events(FAST_CFG, ticks=1200)
    for ride_events in _rides(events).values():
        for value in ride_events:
            if value["event_type"] == RideEventType.REQUESTED.value:
                assert value["driver_id"] is None
            if value["event_type"] in (
                RideEventType.DRIVER_ARRIVED.value,
                RideEventType.STARTED.value,
                RideEventType.COMPLETED.value,
            ):
                assert value["driver_id"] is not None


def test_ticks_for_maths() -> None:
    cfg = GeneratorConfig(speed=60.0, duration_sec=600.0, tick_ms=1000)
    assert ticks_for(cfg) == 36_000
