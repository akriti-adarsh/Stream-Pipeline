"""Typed query helpers for the live dashboard.

Every public fetch function is cached with a short TTL and wrapped so that a
missing table, an empty database, or an unreachable Postgres never raises into
the Streamlit page: callers always receive a DataFrame (possibly empty) and
render a friendly placeholder instead of a stack trace.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import psycopg
import streamlit as st

DEFAULT_DSN = "postgresql://stream:stream@localhost:5433/stream"
CONNECT_TIMEOUT_SEC = 4
CACHE_TTL_SEC = 8


def get_dsn() -> str:
    """Return the Postgres DSN, honouring the POSTGRES_DSN env override."""
    return os.environ.get("POSTGRES_DSN", DEFAULT_DSN)


def dsn_display(dsn: str) -> str:
    """Return the DSN with credentials stripped, safe to show on the page."""
    tail = dsn.rsplit("@", 1)[-1]
    return tail if "@" not in dsn else f"postgresql://{tail}"


def _query_df(dsn: str, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Run one query on a short-lived connection.

    Returns an empty DataFrame on any failure (connection refused, missing
    schema or table, bad column). The dashboard must degrade, never traceback.
    """
    try:
        with (
            psycopg.connect(dsn, connect_timeout=CONNECT_TIMEOUT_SEC) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(sql.encode(), params)
            if cur.description is None:
                return pd.DataFrame()
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_health(dsn: str) -> pd.DataFrame:
    """Cheap connectivity probe: one row when Postgres is reachable."""
    return _query_df(dsn, "SELECT 1 AS ok")


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_cities(dsn: str) -> pd.DataFrame:
    """City dimension used for the map selector and view centering."""
    sql = """
        SELECT city_id, city_name, center_lat, center_lon
        FROM analytics_marts.dim_cities
        ORDER BY city_id
    """
    return _query_df(dsn, sql)


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_driver_positions(dsn: str) -> pd.DataFrame:
    """Latest known position per driver from the raw location pings."""
    sql = """
        SELECT DISTINCT ON (driver_id)
            driver_id, ts, lat, lon, speed_kmh, status, city_id
        FROM raw.driver_locations
        ORDER BY driver_id, ts DESC
    """
    return _query_df(dsn, sql)


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_rides_per_minute(dsn: str, hours: int) -> pd.DataFrame:
    """Rides requested per minute per city over the last N sim-hours.

    Event time runs on an accelerated sim clock, so the window is anchored to
    the data's own max(requested_ts), not to the wall clock.
    """
    sql = """
        WITH bounds AS (
            SELECT max(requested_ts) AS max_ts FROM raw.ride_sessions
        )
        SELECT
            date_trunc('minute', s.requested_ts) AS minute_ts,
            s.city_id,
            count(*)::bigint AS rides
        FROM raw.ride_sessions AS s
        CROSS JOIN bounds AS b
        WHERE s.requested_ts > b.max_ts - make_interval(hours => %(hours)s)
        GROUP BY 1, 2
        ORDER BY 1
    """
    return _query_df(dsn, sql, {"hours": hours})


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_ride_stats(dsn: str) -> pd.DataFrame:
    """Completion rate and closed-ride counts for the last two sim-hours.

    One row per bucket: 'cur' is the last sim-hour before the data's max
    requested_ts, 'prev' is the hour before that (used for the metric delta).
    """
    sql = """
        WITH bounds AS (
            SELECT max(requested_ts) AS max_ts FROM raw.ride_sessions
        ),
        recent AS (
            SELECT
                s.terminal_state,
                CASE
                    WHEN s.requested_ts > b.max_ts - interval '1 hour' THEN 'cur'
                    ELSE 'prev'
                END AS bucket
            FROM raw.ride_sessions AS s
            CROSS JOIN bounds AS b
            WHERE s.requested_ts > b.max_ts - interval '2 hours'
        )
        SELECT
            bucket,
            count(*)::bigint AS rides_closed,
            avg((terminal_state = 'completed')::int)::float8 AS completion_rate
        FROM recent
        GROUP BY bucket
    """
    return _query_df(dsn, sql)


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_ride_totals(dsn: str) -> pd.DataFrame:
    """Lifetime closed-ride counts plus the sim clock's current position."""
    sql = """
        SELECT
            count(*)::bigint AS rides_closed,
            count(*) FILTER (WHERE terminal_state = 'completed')::bigint AS rides_completed,
            max(requested_ts) AS max_requested_ts
        FROM raw.ride_sessions
    """
    return _query_df(dsn, sql)


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_p95_time_to_match(dsn: str) -> pd.DataFrame:
    """P95 time to match in seconds, with the source it came from.

    Prefers the Flink windowed metrics (mean of the last 15 minutes of
    1-minute windows); falls back to a direct percentile over fct_rides when
    the Flink view is missing or empty.
    """
    flink_sql = """
        WITH bounds AS (
            SELECT max(window_end) AS max_we FROM flink.city_metrics_full
        )
        SELECT avg(f.p95_time_to_match_sec)::float8 AS p95_sec
        FROM flink.city_metrics_full AS f
        CROSS JOIN bounds AS b
        WHERE f.window_end > b.max_we - interval '15 minutes'
          AND f.p95_time_to_match_sec IS NOT NULL
    """
    flink_df = _query_df(dsn, flink_sql).dropna()
    if not flink_df.empty:
        return pd.DataFrame(
            {"p95_sec": [float(flink_df["p95_sec"].iloc[0])], "source": ["Flink 1-min windows"]}
        )
    fct_sql = """
        SELECT
            percentile_cont(0.95) WITHIN GROUP (ORDER BY time_to_match_sec)::float8 AS p95_sec
        FROM analytics_marts.fct_rides
        WHERE time_to_match_sec IS NOT NULL
    """
    fct_df = _query_df(dsn, fct_sql).dropna()
    if not fct_df.empty:
        return pd.DataFrame(
            {"p95_sec": [float(fct_df["p95_sec"].iloc[0])], "source": ["fct_rides percentile"]}
        )
    return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_dq_latest(dsn: str) -> pd.DataFrame:
    """Latest data-quality run per suite from serving.dq_results.

    One row per suite: expectation counts, pass count, failed expectation
    names, run mode, and when the run happened.
    """
    sql = """
        WITH latest AS (
            SELECT DISTINCT ON (suite) suite, run_id, run_at
            FROM serving.dq_results
            ORDER BY suite, run_at DESC
        )
        SELECT
            r.suite,
            l.run_at,
            max(r.mode) AS mode,
            count(*)::bigint AS n_expectations,
            count(*) FILTER (WHERE r.success)::bigint AS n_pass,
            array_agg(
                r.expectation || coalesce(' [' || r.column_name || ']', '')
            ) FILTER (WHERE NOT r.success) AS failed
        FROM serving.dq_results AS r
        JOIN latest AS l ON l.suite = r.suite AND l.run_id = r.run_id
        GROUP BY r.suite, l.run_at
        ORDER BY r.suite
    """
    return _query_df(dsn, sql)
