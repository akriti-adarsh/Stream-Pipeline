"""Tick loop turning config into an event stream. Pure: no I/O, no pacing.

The transport layer (CLI) decides how fast to replay ticks and where events
go; unit tests iterate this generator directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from random import Random

from generator.clock import SimClock
from generator.config import GeneratorConfig
from generator.drivers import DriverFleet
from generator.events import SourceEvent
from generator.imperfections import Imperfector
from generator.rides import RideSimulator


def wall_anchor_ms() -> int:
    """Default sim origin: the current wall-clock minute, so downstream
    freshness checks see recent event times. Tests pass an explicit anchor."""
    now = datetime.now(tz=UTC).replace(second=0, microsecond=0)
    return int(now.timestamp() * 1000)


def build_simulator(cfg: GeneratorConfig, anchor_ms: int) -> tuple[RideSimulator, Imperfector]:
    """Wire the rng streams: one master seed fans out to independent per-subsystem
    streams so a change in one subsystem's draw count cannot ripple into another."""
    master = Random(cfg.seed)
    rides_rng = Random(master.getrandbits(64))
    fleet_rng = Random(master.getrandbits(64))
    imperfection_rng = Random(master.getrandbits(64))
    fleet = DriverFleet(cfg, fleet_rng, anchor_ms)
    return RideSimulator(cfg, rides_rng, fleet, anchor_ms), Imperfector(cfg, imperfection_rng)


def simulate_ticks(
    cfg: GeneratorConfig, *, ticks: int, anchor_ms: int | None = None
) -> Iterator[tuple[int, list[SourceEvent]]]:
    """Yield (sim_now_ms, events) per tick, piped through the imperfection layer."""
    anchor = (
        anchor_ms
        if anchor_ms is not None
        else (cfg.anchor_ms if cfg.anchor_ms is not None else wall_anchor_ms())
    )
    clock = SimClock(anchor, cfg.tick_ms)
    sim, imperfector = build_simulator(cfg, anchor)
    while clock.ticks < ticks:
        now_ms = clock.now_ms
        batch = imperfector.release_due(now_ms)
        for event in sim.on_tick(now_ms):
            batch.extend(imperfector.process(event, now_ms))
        yield now_ms, batch
        clock.advance()


def simulate(cfg: GeneratorConfig, *, ticks: int) -> Iterator[SourceEvent]:
    """Yield every event for ``ticks`` simulation ticks from a fresh simulator."""
    for _, batch in simulate_ticks(cfg, ticks=ticks):
        yield from batch


def ticks_for(cfg: GeneratorConfig) -> int:
    """Number of ticks covered by duration_sec of wall time at the given speed."""
    sim_ms = cfg.duration_sec * cfg.speed * 1000.0
    return max(int(sim_ms / cfg.tick_ms), 1)
