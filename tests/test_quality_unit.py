"""Pure-logic tests for the quality layer: baselines, suites, result shaping."""

from __future__ import annotations

import json
from typing import Any

import great_expectations as gx

from quality.runner import result_rows
from quality.suites import (
    RIDE_EVENT_COLUMNS,
    build_suite,
    fct_rides_expectations,
    raw_rides_events_expectations,
    rowcount_bounds,
)


def test_rowcount_bounds_first_run_is_open() -> None:
    assert rowcount_bounds([]) == (0, None)


def test_rowcount_bounds_floor_is_monotone_max() -> None:
    floor, ceiling = rowcount_bounds([1000, 5000, 4800])
    assert floor == 5000, "append-only table: shrinking below the max ever seen is data loss"
    assert ceiling is not None and ceiling > floor


def test_rowcount_bounds_ceiling_tracks_recent_median() -> None:
    history = [10_000] * 10
    _, ceiling = rowcount_bounds(history)
    assert ceiling == 10_000 * 4 + 1000


def test_raw_suite_covers_the_contract() -> None:
    expectations = raw_rides_events_expectations()
    types = [type(e).__name__ for e in expectations]
    assert "ExpectTableColumnsToMatchSet" in types
    assert types.count("ExpectColumnValuesToNotBeNull") >= 6
    assert types.count("ExpectColumnValuesToBeBetween") >= 6  # lat/lon x2, fare, surge
    assert types.count("ExpectColumnValuesToBeInSet") >= 3
    assert len(RIDE_EVENT_COLUMNS) == 19


def test_fct_suite_uses_history_for_rowcount() -> None:
    expectations = fct_rides_expectations([4000, 4200], freshness_minutes=30)
    rowcount = next(e for e in expectations if type(e).__name__ == "ExpectTableRowCountToBeBetween")
    assert rowcount.min_value == 4200


def test_build_suite_registers_all() -> None:
    gx.get_context(mode="ephemeral")  # suite mutation requires an active context
    suite = build_suite("s", raw_rides_events_expectations())
    assert len(suite.expectations) == len(raw_rides_events_expectations())


def test_result_rows_shape() -> None:
    fake_results: list[Any] = [
        {
            "expectation_config": {
                "type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "ride_id"},
            },
            "success": True,
            "result": {"element_count": 100, "unexpected_count": 0},
        },
        {
            "expectation_config": {"type": "expect_table_row_count_to_be_between", "kwargs": {}},
            "success": False,
            "result": {"observed_value": 4321},
        },
    ]
    rows = result_rows("run1", "fct_rides", "warn", fake_results)
    assert rows[0][:5] == (
        "run1",
        "fct_rides",
        "expect_column_values_to_not_be_null",
        "ride_id",
        True,
    )
    assert rows[0][5:7] == (100, 0)
    assert rows[1][4] is False
    assert json.loads(rows[1][7]) == 4321
    assert rows[1][8] == "warn"
