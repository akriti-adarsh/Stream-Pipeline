"""Rates, bounds, and reproducibility of the imperfection layer, plus schema evolution."""

from __future__ import annotations

from random import Random

from common import topics
from generator.config import GeneratorConfig
from generator.events import SourceEvent, ride_event
from generator.imperfections import Imperfector
from generator.simulate import simulate
from generator.state_machine import RideEventType

ANCHOR_MS = 1_767_225_600_000

CFG = GeneratorConfig(seed=42, anchor_ms=ANCHOR_MS)


def _requested(i: int, ts_ms: int) -> SourceEvent:
    return ride_event(
        ride_id=f"r-{i:08d}",
        event_type=RideEventType.REQUESTED,
        event_ts_ms=ts_ms,
        rider_id="u-1",
        city_id=1,
        pickup=(12.97, 77.59),
        dropoff=(12.93, 77.62),
        surge_multiplier=1.0,
    )


def test_bucket_rates_match_documented_expectations() -> None:
    imperfector = Imperfector(CFG, Random(1))
    n = 40_000
    for i in range(n):
        imperfector.process(_requested(i, i * 10), now_ms=i * 10)
    stats = imperfector.stats
    buckets = (
        stats["malformed"] + stats["duplicate"] + stats["out_of_order"] + stats["late"]
    ) + stats["clean"]
    assert buckets == n
    # Expectations at documented rates: malformed 80, duplicates 200,
    # out-of-order 800, late 400 (all on n = 40000).
    assert 40 <= stats["malformed"] <= 140
    assert 120 <= stats["duplicate"] <= 300
    assert 640 <= stats["out_of_order"] <= 980
    assert 300 <= stats["late"] <= 520
    # Held events: one per duplicate (the copy), one per ooo, one per late.
    assert imperfector.pending() == stats["duplicate"] + stats["out_of_order"] + stats["late"]


def test_duplicates_are_identical_and_release_within_ten_seconds() -> None:
    imperfector = Imperfector(CFG, Random(2))
    immediate: list[SourceEvent] = []
    for i in range(20_000):
        immediate.extend(imperfector.process(_requested(i, 0), now_ms=0))
    originals = {e.value["event_id"]: e.value for e in immediate}
    later = imperfector.release_due(now_ms=10_000)
    # Duplicates were emitted immediately AND held; ooo/late were only held,
    # so membership in `originals` identifies the duplicate copies exactly.
    dup_copies = [e for e in later if e.value["event_id"] in originals]
    assert len(dup_copies) == imperfector.stats["duplicate"] > 0
    for copy in dup_copies:
        assert copy.value == originals[copy.value["event_id"]]


def test_hold_windows_respect_bounds() -> None:
    imperfector = Imperfector(CFG, Random(3))
    for i in range(30_000):
        imperfector.process(_requested(i, 0), now_ms=0)
    stats = dict(imperfector.stats)
    within_skew = len(imperfector.release_due(now_ms=int(CFG.ooo_max_skew_sec * 1000)))
    # Everything released inside the 30 s skew window is dup or ooo, never late.
    assert within_skew <= stats["duplicate"] + stats["out_of_order"]
    rest = imperfector.release_due(now_ms=int(CFG.late_max_sec * 1000) + 1000)
    assert len(rest) == stats["duplicate"] + stats["out_of_order"] + stats["late"] - within_skew
    assert imperfector.pending() == 0


def test_null_field_rate_applies_to_requested_dropoff() -> None:
    imperfector = Imperfector(CFG, Random(4))
    nulled = 0
    for i in range(30_000):
        for event in imperfector.process(_requested(i, 0), now_ms=0):
            if event.value["dropoff_lat"] is None:
                nulled += 1
                assert event.value["dropoff_lon"] is None
    assert 200 <= imperfector.stats["null_field"] <= 420  # expectation 300
    assert nulled > 0  # held (ooo/late) copies surface later; the immediate ones prove the path


def test_perfect_config_disables_everything() -> None:
    imperfector = Imperfector(CFG.perfect(), Random(5))
    outs: list[SourceEvent] = []
    for i in range(5_000):
        outs.extend(imperfector.process(_requested(i, 0), now_ms=0))
    assert len(outs) == 5_000
    assert imperfector.pending() == 0
    assert imperfector.stats["clean"] == 5_000
    assert not any(e.corrupt for e in outs)


def test_schema_evolution_switches_versions_mid_stream() -> None:
    cfg = GeneratorConfig(
        seed=9,
        anchor_ms=ANCHOR_MS,
        base_rides_per_min=20.0,
        evolve_after_sec=5.0,  # at speed 60 the boundary is 300 sim seconds in
        speed=60.0,
    )
    boundary_ms = ANCHOR_MS + 300_000
    events = [e for e in simulate(cfg, ticks=900) if e.topic == topics.RIDES_EVENTS]
    before = [e for e in events if e.value["event_ts"] < boundary_ms]
    after = [e for e in events if e.value["event_ts"] >= boundary_ms]
    assert before and after
    assert all(e.value["payload_version"] == 1 for e in before)
    assert all("promo_code" not in e.value for e in before)
    assert all(e.value["payload_version"] == 2 for e in after)
    assert all("promo_code" in e.value for e in after)
    assert any(e.value["promo_code"] is not None for e in after), "no promo codes sampled"
    per_ride: dict[str, set[str | None]] = {}
    for e in after:
        per_ride.setdefault(e.value["ride_id"], set()).add(e.value["promo_code"])
    assert all(len(codes) == 1 for codes in per_ride.values())


def test_imperfect_stream_is_still_deterministic() -> None:
    cfg = GeneratorConfig(seed=21, anchor_ms=ANCHOR_MS, base_rides_per_min=10.0)
    assert list(simulate(cfg, ticks=400)) == list(simulate(cfg, ticks=400))
