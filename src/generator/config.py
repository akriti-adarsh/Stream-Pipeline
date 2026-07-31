"""Generator configuration. Every knob the CLI exposes lives here with its default."""

from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(frozen=True)
class GeneratorConfig:
    """All rates are expressed in simulation time unless suffixed otherwise.

    Determinism contract: the emitted stream is a pure function of
    (seed, anchor_ms, tick count). ``speed`` only controls how fast ticks are
    replayed against the wall clock, never what they contain.
    """

    seed: int = 42
    speed: float = 60.0
    duration_sec: float = 600.0
    anchor_ms: int | None = None
    tick_ms: int = 1000
    base_rides_per_min: float = 40.0
    abandon_rate: float = 0.003
    cancel_probability: dict[str, float] | None = None
    # Fleet: at the default speed of 60 the ping throughput is
    # drivers_total * speed / ping_interval_sec = 300 * 60 / 30 = 600 msg/sec.
    drivers_total: int = 300
    ping_interval_sec: float = 30.0
    offline_prob_per_ping: float = 0.004
    diurnal: bool = True
    # Deliberate imperfections, each with a documented rate. Applied to
    # rides.events (malformed also to payments). Exclusive per event: a single
    # roll lands in at most one bucket, so rates do not overlap.
    dup_rate: float = 0.005
    ooo_rate: float = 0.02
    ooo_max_skew_sec: float = 30.0
    late_rate: float = 0.01
    late_max_sec: float = 600.0
    malformed_rate: float = 0.002
    null_field_rate: float = 0.01
    # Schema evolution: after this many REAL seconds (sim elapsed = value * speed)
    # the generator switches to payload_version 2 with the promo_code field.
    evolve_after_sec: float | None = None

    def perfect(self) -> GeneratorConfig:
        """Copy with every imperfection disabled; the exactly-once kill test
        uses this so its ledger of acked event ids is exact."""
        return replace(
            self,
            dup_rate=0.0,
            ooo_rate=0.0,
            late_rate=0.0,
            malformed_rate=0.0,
            null_field_rate=0.0,
            abandon_rate=0.0,
        )

    def cancel_prob(self, stage: str) -> float:
        """Cancellation probability entering a lifecycle stage (about 8 percent overall)."""
        table = self.cancel_probability or {
            "requested": 0.040,
            "matched": 0.025,
            "driver_arrived": 0.010,
            "started": 0.005,
        }
        return table.get(stage, 0.0)
