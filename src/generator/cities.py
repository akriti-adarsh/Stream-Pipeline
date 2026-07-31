"""The five simulated cities. Coordinates are the real metros so the live map
renders somewhere recognisable; everything that happens in them is synthetic.

Commit 3 uses only the centres; the bounding boxes and hotspot mixtures drive
the geospatial sampling and driver walks added with the geo milestone.
"""

from __future__ import annotations

from dataclasses import dataclass
from random import Random


@dataclass(frozen=True)
class Hotspot:
    lat: float
    lon: float
    sigma_km: float
    weight: float


@dataclass(frozen=True)
class City:
    city_id: int
    name: str
    center_lat: float
    center_lon: float
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    demand_weight: float
    hotspots: tuple[Hotspot, ...]

    def clamp(self, lat: float, lon: float) -> tuple[float, float]:
        return (
            min(max(lat, self.min_lat), self.max_lat),
            min(max(lon, self.min_lon), self.max_lon),
        )


CITIES: tuple[City, ...] = (
    City(
        1,
        "Bengaluru",
        12.9716,
        77.5946,
        12.83,
        13.14,
        77.46,
        77.78,
        1.00,
        (
            Hotspot(12.9716, 77.5946, 2.5, 0.35),
            Hotspot(12.9352, 77.6245, 2.0, 0.25),
            Hotspot(13.0359, 77.5970, 2.0, 0.20),
            Hotspot(12.9569, 77.7011, 2.5, 0.20),
        ),
    ),
    City(
        2,
        "Mumbai",
        19.0760,
        72.8777,
        18.89,
        19.28,
        72.77,
        73.03,
        0.95,
        (
            Hotspot(19.0760, 72.8777, 2.5, 0.30),
            Hotspot(19.0176, 72.8562, 2.0, 0.30),
            Hotspot(19.1136, 72.8697, 2.0, 0.25),
            Hotspot(19.2183, 72.9781, 3.0, 0.15),
        ),
    ),
    City(
        3,
        "Delhi",
        28.6139,
        77.2090,
        28.40,
        28.88,
        76.94,
        77.41,
        0.90,
        (
            Hotspot(28.6139, 77.2090, 3.0, 0.30),
            Hotspot(28.5562, 77.1000, 2.5, 0.25),
            Hotspot(28.6304, 77.2177, 2.0, 0.25),
            Hotspot(28.4595, 77.0266, 3.0, 0.20),
        ),
    ),
    City(
        4,
        "Hyderabad",
        17.3850,
        78.4867,
        17.20,
        17.60,
        78.24,
        78.65,
        0.60,
        (
            Hotspot(17.3850, 78.4867, 2.5, 0.35),
            Hotspot(17.4435, 78.3772, 2.5, 0.35),
            Hotspot(17.4933, 78.3915, 2.0, 0.30),
        ),
    ),
    City(
        5,
        "Chennai",
        13.0827,
        80.2707,
        12.90,
        13.23,
        80.10,
        80.32,
        0.55,
        (
            Hotspot(13.0827, 80.2707, 2.5, 0.40),
            Hotspot(13.0475, 80.2090, 2.0, 0.30),
            Hotspot(12.9791, 80.2212, 2.5, 0.30),
        ),
    ),
)

CITY_BY_ID: dict[int, City] = {c.city_id: c for c in CITIES}
TOTAL_DEMAND_WEIGHT = sum(c.demand_weight for c in CITIES)

KM_PER_DEG_LAT = 111.32


def sample_hotspot_point(city: City, rng: Random) -> tuple[float, float]:
    """Sample a point from the city's mixture of Gaussian hotspots, clamped to its box."""
    roll = rng.random() * sum(h.weight for h in city.hotspots)
    acc = 0.0
    spot = city.hotspots[-1]
    for candidate in city.hotspots:
        acc += candidate.weight
        if roll <= acc:
            spot = candidate
            break
    sigma_deg = spot.sigma_km / KM_PER_DEG_LAT
    return city.clamp(rng.gauss(spot.lat, sigma_deg), rng.gauss(spot.lon, sigma_deg))
