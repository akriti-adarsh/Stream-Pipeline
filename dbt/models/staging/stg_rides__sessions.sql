-- One view per source table, renames and casts only, no business logic.
-- (The spec's staging trio predates the sessions landing table; the same
-- one-view-per-source rule is applied to it.)
select
    ride_id,
    rider_id,
    driver_id,
    city_id,
    terminal_state,
    event_seq,
    requested_ts as requested_at,
    matched_ts as matched_at,
    driver_arrived_ts as driver_arrived_at,
    started_ts as started_at,
    ended_ts as ended_at,
    time_to_match_sec,
    time_to_pickup_sec,
    ride_duration_sec,
    haversine_distance_km,
    avg_speed_kmh,
    is_late_arrival,
    fare_cents::bigint as fare_cents,
    surge_multiplier,
    promo_code,
    pickup_lat,
    pickup_lon,
    dropoff_lat,
    dropoff_lon,
    ingested_at
from {{ source('raw', 'ride_sessions') }}
