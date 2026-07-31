"""Simulation clock: sim time advances in fixed ticks, wall pacing is separate.

The event stream's content depends only on the tick sequence (and the seed), so
the same seed and anchor always produce the same stream no matter how fast it
is replayed. ``--speed`` compresses wall time: at speed 60, one simulated
minute passes per real second of pacing.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SimClock:
    anchor_ms: int
    tick_ms: int = 1000
    ticks: int = field(default=0, init=False)

    @property
    def now_ms(self) -> int:
        return self.anchor_ms + self.ticks * self.tick_ms

    def advance(self) -> int:
        self.ticks += 1
        return self.now_ms

    def real_seconds_per_tick(self, speed: float) -> float:
        """Wall-clock pause between ticks at the given speed factor."""
        return (self.tick_ms / 1000.0) / speed
