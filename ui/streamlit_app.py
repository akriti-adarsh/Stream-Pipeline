"""Live operations dashboard for the stream-pipeline platform.

Reads the Postgres serving layer (raw ingest tables, dbt marts, Flink metric
views, and the data-quality results table) and auto-refreshes every 10
seconds via st.fragment. Every query is wrapped so a missing or empty table
renders a friendly placeholder, never a stack trace.

Run with:
    uv run streamlit run ui/streamlit_app.py

The DSN defaults to postgresql://stream:stream@localhost:5433/stream and can
be overridden with the POSTGRES_DSN environment variable.
"""

from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st
from queries import (
    dsn_display,
    fetch_cities,
    fetch_dq_latest,
    fetch_driver_positions,
    fetch_health,
    fetch_p95_time_to_match,
    fetch_ride_stats,
    fetch_ride_totals,
    fetch_rides_per_minute,
    get_dsn,
)

STATUS_COLORS: dict[str, list[int]] = {
    "idle": [96, 125, 139, 190],
    "en_route_to_pickup": [255, 179, 0, 220],
    "on_trip": [0, 200, 130, 220],
}
FALLBACK_COLOR: list[int] = [158, 158, 158, 170]
ALL_CITIES = "All cities"
INDIA_CENTER = (21.0, 78.5)


def format_seconds(seconds: float) -> str:
    """Render a duration as seconds or minutes, whichever reads better."""
    if seconds < 120:
        return f"{seconds:.0f} s"
    return f"{seconds / 60:.1f} min"


def city_name_map(cities: pd.DataFrame) -> dict[int, str]:
    """Map city_id to display name, tolerating an empty dimension."""
    if cities.empty:
        return {}
    return dict(zip(cities["city_id"].astype(int), cities["city_name"], strict=True))


def compute_view_state(
    cities: pd.DataFrame, positions: pd.DataFrame, selected_city: str
) -> pdk.ViewState:
    """Center the map on the selected city, or frame the whole fleet."""
    if selected_city != ALL_CITIES and not cities.empty:
        row = cities.loc[cities["city_name"] == selected_city]
        if not row.empty:
            return pdk.ViewState(
                latitude=float(row["center_lat"].iloc[0]),
                longitude=float(row["center_lon"].iloc[0]),
                zoom=11,
                pitch=0,
            )
    if not positions.empty:
        return pdk.ViewState(
            latitude=float(positions["lat"].mean()),
            longitude=float(positions["lon"].mean()),
            zoom=4,
            pitch=0,
        )
    return pdk.ViewState(latitude=INDIA_CENTER[0], longitude=INDIA_CENTER[1], zoom=4, pitch=0)


def render_stats_row(dsn: str, positions: pd.DataFrame) -> None:
    """Headline metrics: completion rate, p95 time to match, volume, fleet."""
    stats = fetch_ride_stats(dsn)
    totals = fetch_ride_totals(dsn)
    p95 = fetch_p95_time_to_match(dsn)

    col_rate, col_p95, col_closed, col_fleet = st.columns(4)

    with col_rate:
        cur = stats.loc[stats["bucket"] == "cur"] if not stats.empty else pd.DataFrame()
        prev = stats.loc[stats["bucket"] == "prev"] if not stats.empty else pd.DataFrame()
        if cur.empty:
            st.metric("Completion rate (last sim-hour)", "no data")
        else:
            rate = float(cur["completion_rate"].iloc[0]) * 100.0
            delta: str | None = None
            if not prev.empty:
                diff = rate - float(prev["completion_rate"].iloc[0]) * 100.0
                delta = f"{diff:+.1f} pts vs prior sim-hour"
            st.metric("Completion rate (last sim-hour)", f"{rate:.1f}%", delta=delta)

    with col_p95:
        if p95.empty:
            st.metric("P95 time to match", "no data")
        else:
            st.metric(
                "P95 time to match",
                format_seconds(float(p95["p95_sec"].iloc[0])),
                delta=str(p95["source"].iloc[0]),
                delta_color="off",
            )

    with col_closed:
        if totals.empty:
            st.metric("Rides closed (total)", "no data")
        else:
            closed = int(totals["rides_closed"].iloc[0])
            completed = int(totals["rides_completed"].iloc[0])
            st.metric(
                "Rides closed (total)",
                f"{closed:,}",
                delta=f"{completed:,} completed",
                delta_color="off",
            )

    with col_fleet:
        if positions.empty:
            st.metric("Drivers reporting", "no data")
        else:
            latest = positions["ts"].max()
            recent = positions["ts"] >= latest - pd.Timedelta(minutes=10)
            st.metric(
                "Drivers reporting",
                f"{int(recent.sum()):,}",
                delta=f"{len(positions):,} known drivers",
                delta_color="off",
            )


def render_map(cities: pd.DataFrame, positions: pd.DataFrame, selected_city: str) -> None:
    """Pydeck scatter of the latest position per driver, colored by status."""
    st.subheader("Live driver map")
    if positions.empty:
        st.info(
            "No driver positions yet. The map fills in as raw.driver_locations "
            "receives pings from the generator."
        )
        return

    plot = positions.copy()
    plot["color"] = plot["status"].map(lambda s: STATUS_COLORS.get(str(s), FALLBACK_COLOR))
    plot["last_ping"] = plot["ts"].astype(str)
    plot["speed"] = plot["speed_kmh"].round(1)
    plot = plot[["driver_id", "lat", "lon", "status", "speed", "last_ping", "color"]]

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=plot,
        get_position="[lon, lat]",
        get_fill_color="color",
        get_radius=60,
        radius_min_pixels=2,
        radius_max_pixels=8,
        pickable=True,
    )
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=compute_view_state(cities, positions, selected_city),
        map_style="dark",
        tooltip={"text": "{driver_id}\n{status} at {speed} km/h\nlast ping {last_ping}"},
    )
    st.pydeck_chart(deck, height=420)
    legend = "  |  ".join(f"{status}" for status in STATUS_COLORS)
    st.caption(f"Status colors: {legend} (gray = other). One dot per driver, latest ping only.")


def render_rides_chart(dsn: str, cities: pd.DataFrame, hours: int) -> None:
    """Rides requested per minute per city, anchored to the sim clock."""
    st.subheader(f"Rides per minute by city (last {hours} sim-hours)")
    per_minute = fetch_rides_per_minute(dsn, hours)
    if per_minute.empty:
        st.info(
            "No ride sessions yet. This chart appears once the sessionizer "
            "starts closing rides into raw.ride_sessions."
        )
        return

    names = city_name_map(cities)
    per_minute = per_minute.copy()
    per_minute["city"] = per_minute["city_id"].map(
        lambda cid: names.get(int(cid), f"City {int(cid)}")
    )
    per_minute["minute_ts"] = per_minute["minute_ts"].dt.tz_localize(None)
    pivot = per_minute.pivot_table(
        index="minute_ts", columns="city", values="rides", aggfunc="sum"
    ).fillna(0)
    st.line_chart(pivot, height=260)
    st.caption(
        "Event time is an accelerated sim clock, so the window is anchored to "
        "the newest event in the data, not to wall-clock now."
    )


def render_dq_strip(dsn: str) -> None:
    """Latest Great Expectations run per suite, with failed expectation names."""
    st.subheader("Data quality")
    dq = fetch_dq_latest(dsn)
    if dq.empty:
        st.info(
            "No data-quality runs recorded yet. This strip fills in once the "
            "Great Expectations runner writes to serving.dq_results."
        )
        return

    columns = st.columns(len(dq))
    for column, (_, row) in zip(columns, dq.iterrows(), strict=True):
        with column:
            total = int(row["n_expectations"])
            passed = int(row["n_pass"])
            rate = 100.0 * passed / total if total else 0.0
            st.metric(
                f"Suite: {row['suite']}",
                f"{rate:.0f}% pass",
                delta=f"{passed}/{total} expectations",
                delta_color="normal" if passed == total else "inverse",
            )
            failed = row["failed"]
            if failed is not None and len(failed) > 0:
                st.error("Failed: " + ", ".join(str(name) for name in failed))
            else:
                st.caption("All expectations passed.")
            st.caption(f"Mode: {row['mode']} | run at {row['run_at']:%Y-%m-%d %H:%M:%S %Z}")


@st.fragment(run_every="10s")
def dashboard() -> None:
    """Full dashboard body, re-run every 10 seconds without a page reload."""
    dsn = get_dsn()
    health = fetch_health(dsn)
    if health.empty:
        st.warning(
            f"Cannot reach Postgres at {dsn_display(dsn)} right now. "
            "Showing placeholders; the page retries automatically every 10 seconds."
        )

    cities = fetch_cities(dsn)
    positions = fetch_driver_positions(dsn)

    control_city, control_window, _spacer = st.columns([1, 1, 2])
    with control_city:
        options = [ALL_CITIES]
        if not cities.empty:
            options += [str(name) for name in cities["city_name"]]
        selected_city = st.selectbox("Center map on", options, key="city_select")
    with control_window:
        hours = st.slider("Chart window (sim-hours)", 1, 6, 2, key="window_hours")

    render_stats_row(dsn, positions)
    render_map(cities, positions, selected_city)
    render_rides_chart(dsn, cities, hours)
    render_dq_strip(dsn)

    totals = fetch_ride_totals(dsn)
    if not totals.empty and pd.notna(totals["max_requested_ts"].iloc[0]):
        sim_now = totals["max_requested_ts"].iloc[0]
        st.caption(f"Sim clock at {sim_now:%Y-%m-%d %H:%M:%S %Z} | source {dsn_display(dsn)}")


def main() -> None:
    """Page chrome plus the auto-refreshing dashboard fragment."""
    st.set_page_config(
        page_title="stream-pipeline live ops",
        layout="wide",
    )
    st.title("stream-pipeline live operations")
    st.caption(
        "All data on this page is synthetic, produced by the ride-hailing "
        "simulator on an accelerated sim clock. Auto-refreshes every 10 seconds."
    )
    dashboard()


if __name__ == "__main__":
    main()
