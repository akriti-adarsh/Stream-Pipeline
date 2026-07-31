"""The diurnal demand curve: two rush-hour peaks over a 24 hour period.

The curve is a constant base plus two Gaussians centred on the morning and
evening commute. It multiplies both the ride spawn rate and (damped) the surge
multiplier, so load and pricing breathe together through the compressed day.
"""

from __future__ import annotations

import math

MS_PER_HOUR = 3_600_000.0
MORNING_PEAK_H = 8.5
EVENING_PEAK_H = 18.5
BASE = 0.45
MORNING_AMPLITUDE = 0.85
EVENING_AMPLITUDE = 1.05
MORNING_WIDTH_H = 1.8
EVENING_WIDTH_H = 2.2


def hour_of_day(ts_ms: int) -> float:
    """Fractional hour of day in [0, 24) for a UTC epoch-millis instant."""
    return (ts_ms / MS_PER_HOUR) % 24.0


def _peak(hour: float, centre: float, width: float) -> float:
    # Wrap-around distance so the curve is periodic across midnight.
    delta = min(abs(hour - centre), 24.0 - abs(hour - centre))
    return math.exp(-((delta / width) ** 2))


def demand_factor(ts_ms: int) -> float:
    """Traffic multiplier at an instant; roughly 0.45 at night, above 1.5 at peaks."""
    hour = hour_of_day(ts_ms)
    return (
        BASE
        + MORNING_AMPLITUDE * _peak(hour, MORNING_PEAK_H, MORNING_WIDTH_H)
        + EVENING_AMPLITUDE * _peak(hour, EVENING_PEAK_H, EVENING_WIDTH_H)
    )
