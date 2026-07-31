"""The imperfection layer: the part most streaming demos skip.

Every real event stream contains duplicates, reordering, lateness, corruption,
and half-filled records. This stage injects each of them at a configured,
documented rate so every downstream component has to earn its keep:

- duplicates (same event_id, identical payload) re-sent 1 to 10 seconds later
- out-of-order: emission held up to 30 seconds while event_ts stays put
- late: emission held 30 seconds to 10 minutes (past any sane watermark)
- malformed: flagged for wire-level corruption in the transport; must end
  up in the dead-letter queue, never silently dropped
- null fields: nullable columns actually arrive null sometimes

The buckets are exclusive per event (one uniform roll against cumulative
thresholds), so configured rates are exact expectations, not approximations
that overlap. All decisions come from the seeded rng: the imperfect stream is
as reproducible as the perfect one.
"""

from __future__ import annotations

from dataclasses import replace
from random import Random

from common import topics
from generator.config import GeneratorConfig
from generator.events import SourceEvent
from generator.state_machine import RideEventType


class Imperfector:
    def __init__(self, cfg: GeneratorConfig, rng: Random) -> None:
        self._cfg = cfg
        self._rng = rng
        self._held: list[tuple[int, SourceEvent]] = []
        self.stats: dict[str, int] = {
            "duplicate": 0,
            "out_of_order": 0,
            "late": 0,
            "malformed": 0,
            "null_field": 0,
            "clean": 0,
        }

    # ---------------------------------------------------------------- process

    def process(self, event: SourceEvent, now_ms: int) -> list[SourceEvent]:
        if event.topic == topics.PAYMENTS_TRANSACTIONS:
            return [self._maybe_corrupt_only(event)]
        if event.topic != topics.RIDES_EVENTS:
            return [event]

        event = self._maybe_null_fields(event)
        roll = self._rng.random()
        cfg = self._cfg
        out: list[SourceEvent]

        if roll < cfg.malformed_rate:
            self.stats["malformed"] += 1
            out = [replace(event, corrupt=True)]
        elif roll < cfg.malformed_rate + cfg.dup_rate:
            self.stats["duplicate"] += 1
            delay_ms = int(self._rng.uniform(1.0, 10.0) * 1000)
            self._held.append((now_ms + delay_ms, event))
            out = [event]
        elif roll < cfg.malformed_rate + cfg.dup_rate + cfg.ooo_rate:
            self.stats["out_of_order"] += 1
            skew_ms = int(self._rng.uniform(1.0, cfg.ooo_max_skew_sec) * 1000)
            self._held.append((now_ms + skew_ms, event))
            out = []
        elif roll < cfg.malformed_rate + cfg.dup_rate + cfg.ooo_rate + cfg.late_rate:
            self.stats["late"] += 1
            delay_ms = int(self._rng.uniform(cfg.ooo_max_skew_sec + 1.0, cfg.late_max_sec) * 1000)
            self._held.append((now_ms + delay_ms, event))
            out = []
        else:
            self.stats["clean"] += 1
            out = [event]
        return out

    def _maybe_corrupt_only(self, event: SourceEvent) -> SourceEvent:
        if self._rng.random() < self._cfg.malformed_rate:
            self.stats["malformed"] += 1
            return replace(event, corrupt=True)
        return event

    def _maybe_null_fields(self, event: SourceEvent) -> SourceEvent:
        if (
            event.value.get("event_type") == RideEventType.REQUESTED.value
            and self._rng.random() < self._cfg.null_field_rate
        ):
            self.stats["null_field"] += 1
            value = {**event.value, "dropoff_lat": None, "dropoff_lon": None}
            return replace(event, value=value)
        return event

    # ---------------------------------------------------------------- release

    def release_due(self, now_ms: int) -> list[SourceEvent]:
        due = [event for at_ms, event in self._held if at_ms <= now_ms]
        self._held = [(at_ms, event) for at_ms, event in self._held if at_ms > now_ms]
        return due

    def pending(self) -> int:
        return len(self._held)
