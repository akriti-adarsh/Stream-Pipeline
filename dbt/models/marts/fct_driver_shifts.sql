-- One row per driver shift (sessionized ping stream; see int_drivers__shifts).
select
    shift_id,
    driver_id,
    city_id,
    shift_started_at,
    shift_ended_at,
    active_minutes,
    distance_km,
    ping_count,
    avg_speed_kmh,
    on_trip_share
from {{ ref('int_drivers__shifts') }}
