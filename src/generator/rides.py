"""The ride lifecycle simulator: coherent state, realistic timing, real payments.

Every ride is driven through :class:`RideStateMachine`, so illegal transitions
are structurally impossible. Timing draws come from documented distributions:

- time to match: exponential, mean 30 s, clipped to [3 s, 240 s]
- driver travel to pickup: true distance from the assigned driver's position
  over a clipped normal road speed, so pings and lifecycle events agree
- arrival to start: uniform 10 s to 90 s
- ride duration: haversine distance over a clipped normal speed
  (mean 26 km/h, sigma 7), plus 10 percent noise
- payment: uniform 5 s to 90 s after completion, by design late

About 8 percent of rides cancel, at stage-dependent rates from
``GeneratorConfig.cancel_prob``. A small configurable fraction abandon
mid-lifecycle and never emit a terminal event; the sessionizer's event-time
timeout exists precisely to close those.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from random import Random

from common.geo import haversine_km
from generator import diurnal
from generator.cities import (
    CITIES,
    CITY_BY_ID,
    TOTAL_DEMAND_WEIGHT,
    City,
    sample_hotspot_point,
)
from generator.config import GeneratorConfig
from generator.drivers import DriverFleet
from generator.events import SourceEvent, payment_event, ride_event
from generator.state_machine import RideEventType, RideStateMachine

BASE_FARE_CENTS = 5000
PER_KM_CENTS = 1500
PER_MIN_CENTS = 200


@dataclass
class ActiveRide:
    ride_id: str
    rider_id: str
    city_id: int
    pickup: tuple[float, float]
    dropoff: tuple[float, float]
    surge: float
    machine: RideStateMachine = field(default_factory=RideStateMachine)
    driver_id: str | None = None
    requested_ms: int = 0
    started_ms: int = 0
    fare_cents: int | None = None
    next_at_ms: int = 0
    next_event: RideEventType | None = None
    stop_after: RideEventType | None = None
    distance_km: float = 0.0
    duration_sec: float = 0.0


class RideSimulator:
    def __init__(self, cfg: GeneratorConfig, rng: Random, fleet: DriverFleet) -> None:
        self._cfg = cfg
        self._rng = rng
        self._fleet = fleet
        self._active: dict[str, ActiveRide] = {}
        self._payments: list[tuple[int, SourceEvent]] = []
        self._counter = 0
        self._spawn_carry = 0.0
        self.completed_rides = 0
        self.cancelled_rides = 0

    # ------------------------------------------------------------------ ticks

    def on_tick(self, now_ms: int) -> list[SourceEvent]:
        out: list[SourceEvent] = list(self._fleet.on_tick(now_ms))
        out.extend(self._spawn(now_ms))
        out.extend(self._progress(now_ms))
        out.extend(self._due_payments(now_ms))
        return out

    def demand_factor(self, now_ms: int) -> float:
        """Traffic multiplier at a sim instant: the diurnal curve unless disabled."""
        return diurnal.demand_factor(now_ms) if self._cfg.diurnal else 1.0

    def active_count(self) -> int:
        return len(self._active)

    # ------------------------------------------------------------------ spawn

    def _spawn(self, now_ms: int) -> list[SourceEvent]:
        rate_per_tick = (
            self._cfg.base_rides_per_min / 60.0 * (self._cfg.tick_ms / 1000.0)
        ) * self.demand_factor(now_ms)
        self._spawn_carry += rate_per_tick
        events: list[SourceEvent] = []
        while self._spawn_carry >= 1.0:
            self._spawn_carry -= 1.0
            events.append(self._create_ride(now_ms))
        return events

    def _create_ride(self, now_ms: int) -> SourceEvent:
        self._counter += 1
        city = self._pick_city()
        ride_id = f"r-{self._counter:08d}"
        pickup = self.sample_point(city)
        dropoff = self.sample_point(city)
        if haversine_km(*pickup, *dropoff) < 0.5:
            dropoff = self.sample_point(city)
        ride = ActiveRide(
            ride_id=ride_id,
            rider_id=f"u-{self._rng.randrange(200_000):06d}",
            city_id=city.city_id,
            pickup=pickup,
            dropoff=dropoff,
            surge=self.surge_for(city, now_ms),
        )
        ride.requested_ms = now_ms + self._rng.randrange(self._cfg.tick_ms)
        ride.distance_km = haversine_km(*pickup, *dropoff)
        ride.machine.advance(RideEventType.REQUESTED)
        if self._rng.random() < self._cfg.abandon_rate:
            ride.stop_after = self._rng.choice(
                [RideEventType.MATCHED, RideEventType.DRIVER_ARRIVED]
            )
        self._schedule_after_requested(ride)
        self._active[ride_id] = ride
        return ride_event(
            ride_id=ride_id,
            event_type=RideEventType.REQUESTED,
            event_ts_ms=ride.requested_ms,
            rider_id=ride.rider_id,
            city_id=ride.city_id,
            pickup=ride.pickup,
            dropoff=ride.dropoff,
            surge_multiplier=ride.surge,
        )

    def _pick_city(self) -> City:
        roll = self._rng.random() * TOTAL_DEMAND_WEIGHT
        acc = 0.0
        for city in CITIES:
            acc += city.demand_weight
            if roll <= acc:
                return city
        return CITIES[-1]

    def sample_point(self, city: City) -> tuple[float, float]:
        return sample_hotspot_point(city, self._rng)

    def surge_for(self, city: City, now_ms: int) -> float:
        """Surge follows demand, damped, with noise: about 1.0 off-peak, 1.2 to 1.6 at rush."""
        demand = self.demand_factor(now_ms)
        surge = 0.55 + 0.45 * demand + self._rng.gauss(0.0, 0.10)
        return round(min(max(surge, 1.0), 3.0), 2)

    # --------------------------------------------------------------- progress

    def _schedule_after_requested(self, ride: ActiveRide) -> None:
        if self._rng.random() < self._cfg.cancel_prob("requested"):
            delay = min(self._exp(90.0), 600.0)
            self._plan(ride, RideEventType.CANCELLED, ride.requested_ms + int(delay * 1000))
        else:
            delay = min(max(self._exp(30.0), 3.0), 240.0)
            self._plan(ride, RideEventType.MATCHED, ride.requested_ms + int(delay * 1000))

    def _plan(self, ride: ActiveRide, event: RideEventType, at_ms: int) -> None:
        ride.next_event = event
        ride.next_at_ms = at_ms

    def _progress(self, now_ms: int) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        done: list[str] = []
        for ride in self._active.values():
            while self._has_due_event(ride, now_ms):
                events.append(self._fire(ride))
                if ride.machine.is_terminal or ride.stop_after == ride.machine.state:
                    done.append(ride.ride_id)
                    break
        for ride_id in done:
            del self._active[ride_id]
        return events

    def _has_due_event(self, ride: ActiveRide, now_ms: int) -> bool:
        return ride.next_event is not None and ride.next_at_ms <= now_ms

    def _fire(self, ride: ActiveRide) -> SourceEvent:
        assert ride.next_event is not None
        event_type = ride.next_event
        at_ms = ride.next_at_ms
        ride.machine.advance(event_type)
        ride.next_event = None

        if event_type is RideEventType.MATCHED:
            self._after_matched(ride, at_ms)
        elif event_type is RideEventType.DRIVER_ARRIVED:
            if ride.stop_after is RideEventType.DRIVER_ARRIVED:
                self._release_driver(ride, at_ms)
            else:
                self._after_arrived(ride, at_ms)
        elif event_type is RideEventType.STARTED:
            ride.started_ms = at_ms
            self._after_started(ride, at_ms)
        elif event_type is RideEventType.COMPLETED:
            ride.fare_cents = self._fare(ride)
            self.completed_rides += 1
            self._queue_payment(ride, at_ms)
            self._release_driver(ride, at_ms, at=ride.dropoff)
        elif event_type is RideEventType.CANCELLED:
            self.cancelled_rides += 1
            self._release_driver(ride, at_ms)

        include_dropoff = event_type in (
            RideEventType.REQUESTED,
            RideEventType.STARTED,
            RideEventType.COMPLETED,
        )
        return ride_event(
            ride_id=ride.ride_id,
            event_type=event_type,
            event_ts_ms=at_ms,
            rider_id=ride.rider_id,
            city_id=ride.city_id,
            pickup=ride.pickup,
            dropoff=ride.dropoff if include_dropoff else None,
            surge_multiplier=ride.surge,
            driver_id=ride.driver_id,
            fare_cents=ride.fare_cents if event_type is RideEventType.COMPLETED else None,
        )

    def _after_matched(self, ride: ActiveRide, at_ms: int) -> None:
        driver_id, travel_sec = self._fleet.assign(ride.city_id, ride.pickup, at_ms)
        ride.driver_id = driver_id
        if ride.stop_after is RideEventType.MATCHED:
            self._release_driver(ride, at_ms)
            return
        if self._rng.random() < self._cfg.cancel_prob("matched"):
            self._plan(ride, RideEventType.CANCELLED, at_ms + int(self._exp(60.0) * 1000))
            return
        self._plan(ride, RideEventType.DRIVER_ARRIVED, at_ms + int(travel_sec * 1000))

    def _release_driver(
        self, ride: ActiveRide, at_ms: int, at: tuple[float, float] | None = None
    ) -> None:
        if ride.driver_id is not None:
            self._fleet.release(ride.driver_id, at_ms, at=at)

    def _after_arrived(self, ride: ActiveRide, at_ms: int) -> None:
        if self._rng.random() < self._cfg.cancel_prob("driver_arrived"):
            self._plan(ride, RideEventType.CANCELLED, at_ms + int(self._exp(45.0) * 1000))
            return
        self._plan(ride, RideEventType.STARTED, at_ms + int(self._rng.uniform(10.0, 90.0) * 1000))

    def _after_started(self, ride: ActiveRide, at_ms: int) -> None:
        speed_kmh = min(max(self._rng.gauss(26.0, 7.0), 10.0), 55.0)
        duration = ride.distance_km / speed_kmh * 3600.0
        duration *= 1.0 + self._rng.gauss(0.0, 0.10)
        ride.duration_sec = max(duration, 60.0)
        if ride.driver_id is not None:
            self._fleet.ride_started(ride.driver_id, ride.dropoff, ride.duration_sec, at_ms)
        if self._rng.random() < self._cfg.cancel_prob("started"):
            cancel_at = at_ms + int(ride.duration_sec * self._rng.uniform(0.1, 0.8) * 1000)
            self._plan(ride, RideEventType.CANCELLED, cancel_at)
            return
        self._plan(ride, RideEventType.COMPLETED, at_ms + int(ride.duration_sec * 1000))

    def _fare(self, ride: ActiveRide) -> int:
        minutes = ride.duration_sec / 60.0
        raw = BASE_FARE_CENTS + PER_KM_CENTS * ride.distance_km + PER_MIN_CENTS * minutes
        return int(raw * ride.surge)

    # --------------------------------------------------------------- payments

    def _queue_payment(self, ride: ActiveRide, completed_ms: int) -> None:
        due_ms = completed_ms + int(self._rng.uniform(5.0, 90.0) * 1000)
        roll = self._rng.random()
        status = "completed" if roll < 0.96 else ("failed" if roll < 0.99 else "refunded")
        method_roll = self._rng.random()
        method = "card" if method_roll < 0.55 else ("wallet" if method_roll < 0.85 else "cash")
        assert ride.fare_cents is not None
        self._payments.append(
            (
                due_ms,
                payment_event(
                    txn_id=f"t-{ride.ride_id}",
                    ride_id=ride.ride_id,
                    amount_cents=ride.fare_cents,
                    status=status,
                    method=method,
                    ts_ms=due_ms,
                ),
            )
        )

    def _due_payments(self, now_ms: int) -> list[SourceEvent]:
        due = [event for due_ms, event in self._payments if due_ms <= now_ms]
        self._payments = [(d, e) for d, e in self._payments if d > now_ms]
        return due

    # ------------------------------------------------------------------ misc

    def _exp(self, mean: float) -> float:
        return self._rng.expovariate(1.0 / mean)

    def city(self, city_id: int) -> City:
        return CITY_BY_ID[city_id]
