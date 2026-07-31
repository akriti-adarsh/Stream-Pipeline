"""Shape checks on the diurnal demand curve."""

from __future__ import annotations

from generator.diurnal import demand_factor, hour_of_day

H = 3_600_000


def test_peaks_dominate_night() -> None:
    night = demand_factor(3 * H)
    morning = demand_factor(int(8.5 * H))
    evening = demand_factor(int(18.5 * H))
    assert morning > 2 * night
    assert evening > 2 * night
    assert evening > morning  # evening rush is the bigger one


def test_curve_is_periodic() -> None:
    for hour in (0, 5, 9, 13, 18, 23):
        assert demand_factor(hour * H) == demand_factor(hour * H + 24 * H)


def test_range_is_sane() -> None:
    values = [demand_factor(int(h * H)) for h in range(24)]
    assert all(0.3 < v < 2.0 for v in values)
    mean = sum(values) / len(values)
    assert 0.6 < mean < 1.4


def test_hour_of_day_wraps() -> None:
    assert hour_of_day(0) == 0.0
    assert hour_of_day(24 * H) == 0.0
    assert hour_of_day(int(25.5 * H)) == 1.5
