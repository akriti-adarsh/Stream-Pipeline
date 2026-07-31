"""The driver fleet: geospatially plausible movement coupled to the ride lifecycle.

Idle drivers cruise on smoothed random walks constrained to their city's
bounding box; speed is drawn from a status-conditional distribution (idle
drivers dawdle, drivers with a job move with purpose). When the ride simulator
matches a ride, the nearest idle driver is put en route to the pickup and the
returned travel estimate becomes the ride's driver_arrived delay, so pings and
lifecycle events tell one coherent story. Drivers drift offline and back on a
seeded schedule; offline drivers go dark rather than pinging, exactly like a
phone in a pocket.
"""

from __future__ import annotations

from dataclasses import dataclass
from random import Random

from common.geo import destination_point, haversine_km, initial_bearing_deg
from generator.cities import (
    CITIES,
    CITY_BY_ID,
    TOTAL_DEMAND_WEIGHT,
    City,
    sample_hotspot_point,
)
from generator.config import GeneratorConfig
from generator.events import SourceEvent, driver_location_event

IDLE = "idle"
EN_ROUTE = "en_route_to_pickup"
ON_TRIP = "on_trip"
OFFLINE = "offline"


@dataclass
class Driver:
    driver_id: str
    city_id: int
    lat: float
    lon: float
    heading: float
    speed_kmh: float = 0.0
    status: str = IDLE
    ride_id: str | None = None
    target: tuple[float, float] | None = None
    leg_start: tuple[float, float] | None = None
    leg_start_ms: int = 0
    leg_end_ms: int = 0
    next_ping_ms: int = 0
    back_online_ms: int = 0


class DriverFleet:
    def __init__(self, cfg: GeneratorConfig, rng: Random, anchor_ms: int) -> None:
        self._cfg = cfg
        self._rng = rng
        self._drivers: dict[str, Driver] = {}
        self._by_city: dict[int, list[Driver]] = {c.city_id: [] for c in CITIES}
        self._overflow = 0
        for city in CITIES:
            share = city.demand_weight / TOTAL_DEMAND_WEIGHT
            count = max(round(cfg.drivers_total * share), 4)
            for n in range(count):
                self._spawn_driver(city, f"d-{city.city_id}-{n:04d}", anchor_ms)

    # ----------------------------------------------------------------- fleet

    def _spawn_driver(self, city: City, driver_id: str, now_ms: int) -> Driver:
        lat, lon = sample_hotspot_point(city, self._rng)
        driver = Driver(
            driver_id=driver_id,
            city_id=city.city_id,
            lat=lat,
            lon=lon,
            heading=self._rng.uniform(0.0, 360.0),
            next_ping_ms=now_ms + int(self._rng.uniform(0.0, self._cfg.ping_interval_sec) * 1000),
        )
        self._drivers[driver_id] = driver
        self._by_city[city.city_id].append(driver)
        return driver

    # ------------------------------------------------------------ assignment

    def assign(self, city_id: int, pickup: tuple[float, float], now_ms: int) -> tuple[str, float]:
        """Pick the nearest idle driver (spawning one if the city is dry), put it
        en route, and return (driver_id, travel_seconds)."""
        city = CITY_BY_ID[city_id]
        idle = [d for d in self._by_city[city_id] if d.status == IDLE]
        if idle:
            driver = min(idle, key=lambda d: haversine_km(d.lat, d.lon, *pickup))
        else:
            self._overflow += 1
            driver = self._spawn_driver(city, f"d-{city_id}-x{self._overflow:04d}", now_ms)
        distance = haversine_km(driver.lat, driver.lon, *pickup)
        speed = min(max(self._rng.gauss(30.0, 6.0), 15.0), 50.0)
        travel_sec = max(distance / speed * 3600.0, 20.0)
        self._begin_leg(driver, EN_ROUTE, pickup, now_ms, int(travel_sec * 1000))
        return driver.driver_id, travel_sec

    def ride_started(
        self, driver_id: str, dropoff: tuple[float, float], duration_sec: float, now_ms: int
    ) -> None:
        driver = self._drivers[driver_id]
        self._begin_leg(driver, ON_TRIP, dropoff, now_ms, int(duration_sec * 1000))

    def release(self, driver_id: str, now_ms: int, at: tuple[float, float] | None = None) -> None:
        """Ride over (completed or cancelled): drop back to idle cruising."""
        driver = self._drivers[driver_id]
        if at is not None:
            driver.lat, driver.lon = at
        driver.status = IDLE
        driver.ride_id = None
        driver.target = None
        driver.speed_kmh = min(max(self._rng.gauss(12.0, 5.0), 0.0), 30.0)

    def _begin_leg(
        self, driver: Driver, status: str, target: tuple[float, float], now_ms: int, leg_ms: int
    ) -> None:
        driver.status = status
        driver.target = target
        driver.leg_start = (driver.lat, driver.lon)
        driver.leg_start_ms = now_ms
        driver.leg_end_ms = now_ms + max(leg_ms, 1000)

    # ---------------------------------------------------------------- ticking

    def on_tick(self, now_ms: int) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        interval_ms = int(self._cfg.ping_interval_sec * 1000)
        for driver in self._drivers.values():
            if driver.status == OFFLINE:
                if now_ms >= driver.back_online_ms:
                    driver.status = IDLE
                    driver.next_ping_ms = now_ms
                else:
                    continue
            while driver.next_ping_ms <= now_ms:
                ping_ms = driver.next_ping_ms
                self._move(driver, ping_ms)
                events.append(self._ping(driver, ping_ms))
                driver.next_ping_ms = ping_ms + interval_ms
                if self._maybe_go_offline(driver, ping_ms):
                    events.append(self._ping(driver, ping_ms))
                    break
        return events

    def _maybe_go_offline(self, driver: Driver, now_ms: int) -> bool:
        if driver.status != IDLE or self._rng.random() >= self._cfg.offline_prob_per_ping:
            return False
        driver.status = OFFLINE
        driver.speed_kmh = 0.0
        driver.back_online_ms = now_ms + int(
            min(max(self._rng.expovariate(1.0 / 900.0), 120.0), 3600.0) * 1000
        )
        return True

    def _move(self, driver: Driver, now_ms: int) -> None:
        if driver.status in (EN_ROUTE, ON_TRIP) and driver.target is not None:
            self._move_along_leg(driver, now_ms)
        elif driver.status == IDLE:
            self._cruise(driver)

    def _move_along_leg(self, driver: Driver, now_ms: int) -> None:
        assert driver.target is not None
        assert driver.leg_start is not None
        span = max(driver.leg_end_ms - driver.leg_start_ms, 1)
        progress = min(max((now_ms - driver.leg_start_ms) / span, 0.0), 1.0)
        start_lat, start_lon = driver.leg_start
        target_lat, target_lon = driver.target
        jitter = self._rng.gauss(0.0, 0.0003)
        city = CITY_BY_ID[driver.city_id]
        driver.lat, driver.lon = city.clamp(
            start_lat + (target_lat - start_lat) * progress + jitter,
            start_lon + (target_lon - start_lon) * progress + jitter,
        )
        remaining_km = haversine_km(driver.lat, driver.lon, target_lat, target_lon)
        remaining_h = max((driver.leg_end_ms - now_ms) / 3_600_000.0, 1e-4)
        base = 45.0 if driver.status == EN_ROUTE else 50.0
        driver.speed_kmh = min(max(remaining_km / remaining_h, 8.0), base + 10.0)
        driver.heading = initial_bearing_deg(driver.lat, driver.lon, target_lat, target_lon)

    def _cruise(self, driver: Driver) -> None:
        city = CITY_BY_ID[driver.city_id]
        driver.heading = (driver.heading + self._rng.gauss(0.0, 30.0)) % 360.0
        driver.speed_kmh = min(max(self._rng.gauss(14.0, 6.0), 0.0), 30.0)
        step_km = driver.speed_kmh * self._cfg.ping_interval_sec / 3600.0
        lat, lon = destination_point(driver.lat, driver.lon, driver.heading, step_km)
        clamped_lat, clamped_lon = city.clamp(lat, lon)
        if clamped_lat != lat or clamped_lon != lon:
            driver.heading = (driver.heading + 180.0) % 360.0
        driver.lat, driver.lon = clamped_lat, clamped_lon

    def _ping(self, driver: Driver, ts_ms: int) -> SourceEvent:
        return driver_location_event(
            driver_id=driver.driver_id,
            ts_ms=ts_ms,
            lat=driver.lat,
            lon=driver.lon,
            speed_kmh=round(driver.speed_kmh, 1),
            heading=round(driver.heading, 1),
            status=driver.status,
            city_id=driver.city_id,
        )

    # ------------------------------------------------------------------ intro

    def driver(self, driver_id: str) -> Driver:
        return self._drivers[driver_id]

    def size(self) -> int:
        return len(self._drivers)
