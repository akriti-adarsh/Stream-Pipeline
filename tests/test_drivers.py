"""Fleet behaviour: geospatial plausibility, ride coupling, and ping economics."""

from __future__ import annotations

from collections import defaultdict
from itertools import pairwise

import pytest

from common import topics
from common.geo import haversine_km
from generator.cities import CITY_BY_ID
from generator.config import GeneratorConfig
from generator.events import SourceEvent
from generator.simulate import simulate
from generator.state_machine import RideEventType

ANCHOR_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z

CFG = GeneratorConfig(
    seed=42,
    anchor_ms=ANCHOR_MS,
    base_rides_per_min=6.0,
    drivers_total=60,
    ping_interval_sec=30.0,
)


@pytest.fixture(scope="module")
def events() -> list[SourceEvent]:
    return list(simulate(CFG, ticks=1800))


def _pings(events: list[SourceEvent]) -> list[SourceEvent]:
    return [e for e in events if e.topic == topics.DRIVERS_LOCATIONS]


def test_pings_stay_inside_their_city_box(events: list[SourceEvent]) -> None:
    for ping in _pings(events):
        city = CITY_BY_ID[ping.value["city_id"]]
        assert city.min_lat <= ping.value["lat"] <= city.max_lat
        assert city.min_lon <= ping.value["lon"] <= city.max_lon


def test_ping_volume_matches_interval_maths(events: list[SourceEvent]) -> None:
    # 60 drivers, 1800 sim seconds, one ping per 30 s: about 3600 pings,
    # minus drivers that drifted offline.
    count = len(_pings(events))
    assert 2400 <= count <= 3800, count


def test_speeds_are_status_conditional_and_bounded(events: list[SourceEvent]) -> None:
    by_status: dict[str, list[float]] = defaultdict(list)
    for ping in _pings(events):
        by_status[ping.value["status"]].append(ping.value["speed_kmh"])
        assert 0.0 <= ping.value["speed_kmh"] <= 60.0
        assert 0.0 <= ping.value["heading"] <= 360.0
    assert by_status["idle"], "no idle pings seen"
    assert by_status["on_trip"], "no on_trip pings seen"
    mean = lambda xs: sum(xs) / len(xs)  # noqa: E731
    assert mean(by_status["on_trip"]) > mean(by_status["idle"])


def test_movement_is_smooth_between_pings(events: list[SourceEvent]) -> None:
    tracks: dict[str, list[SourceEvent]] = defaultdict(list)
    for ping in _pings(events):
        tracks[ping.value["driver_id"]].append(ping)
    checked = 0
    for track in tracks.values():
        for a, b in pairwise(track):
            dt_h = (b.value["ts"] - a.value["ts"]) / 3_600_000.0
            if dt_h <= 0:
                continue
            dist = haversine_km(a.value["lat"], a.value["lon"], b.value["lat"], b.value["lon"])
            # Generous bound: max speed plus jitter allowance.
            assert dist <= 75.0 * dt_h + 0.15, (a.value, b.value)
            checked += 1
    assert checked > 1000


def test_assigned_driver_pings_en_route_then_on_trip(events: list[SourceEvent]) -> None:
    completed = [
        e.value
        for e in events
        if e.topic == topics.RIDES_EVENTS and e.value["event_type"] == RideEventType.COMPLETED.value
    ]
    assert completed, "no completed rides in the window"
    matched: dict[str, int] = {}
    for e in events:
        if e.topic == topics.RIDES_EVENTS and e.value["event_type"] == RideEventType.MATCHED.value:
            matched[e.value["ride_id"]] = e.value["event_ts"]
    coupled = 0
    for ride in completed:
        driver_id = ride["driver_id"]
        window = [
            p.value
            for p in _pings(events)
            if p.value["driver_id"] == driver_id
            and matched[ride["ride_id"]] <= p.value["ts"] <= ride["event_ts"]
        ]
        statuses = {p["status"] for p in window}
        if {"en_route_to_pickup", "on_trip"} <= statuses:
            coupled += 1
    assert coupled / len(completed) > 0.5, f"only {coupled}/{len(completed)} rides show coupling"


def test_default_config_hits_throughput_target() -> None:
    cfg = GeneratorConfig()
    real_rate = cfg.drivers_total * cfg.speed / cfg.ping_interval_sec
    assert real_rate >= 500.0
