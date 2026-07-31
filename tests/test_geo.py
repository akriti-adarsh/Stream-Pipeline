"""Haversine and forward-point checks against known city-pair distances."""

from __future__ import annotations

import pytest

from common.geo import destination_point, haversine_km

# Great-circle references computed from the spherical model (R = 6371.0088 km).
KNOWN_PAIRS = [
    ("Bengaluru-Chennai", (12.9716, 77.5946), (13.0827, 80.2707), 290.2),
    ("Delhi-Mumbai", (28.6139, 77.2090), (19.0760, 72.8777), 1153.0),
    ("NewYork-London", (40.7128, -74.0060), (51.5074, -0.1278), 5570.2),
]


@pytest.mark.parametrize(("name", "a", "b", "expected_km"), KNOWN_PAIRS)
def test_haversine_matches_known_distances(
    name: str, a: tuple[float, float], b: tuple[float, float], expected_km: float
) -> None:
    got = haversine_km(a[0], a[1], b[0], b[1])
    assert got == pytest.approx(expected_km, rel=0.01), name


def test_haversine_zero_distance() -> None:
    assert haversine_km(12.97, 77.59, 12.97, 77.59) == 0.0


def test_haversine_is_symmetric() -> None:
    d1 = haversine_km(12.9716, 77.5946, 13.0827, 80.2707)
    d2 = haversine_km(13.0827, 80.2707, 12.9716, 77.5946)
    assert d1 == pytest.approx(d2, rel=1e-12)


def test_destination_point_round_trip() -> None:
    lat, lon = destination_point(12.9716, 77.5946, 45.0, 5.0)
    assert haversine_km(12.9716, 77.5946, lat, lon) == pytest.approx(5.0, rel=1e-6)


def test_destination_point_wraps_longitude() -> None:
    _, lon = destination_point(0.0, 179.9, 90.0, 50.0)
    assert -180.0 <= lon <= 180.0
