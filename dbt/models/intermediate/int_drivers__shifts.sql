-- Sessionize the GPS ping stream into shifts: a shift is a run of pings for one
-- driver with no gap longer than 30 minutes and no offline marker in between.
-- Classic gaps-and-islands with window functions; the distance is the sum of
-- haversine steps between consecutive pings inside the shift.
with pings as (
    select
        driver_id,
        city_id,
        pinged_at,
        lat,
        lon,
        speed_kmh,
        status,
        lag(pinged_at) over (partition by driver_id order by pinged_at) as prev_at,
        lag(lat) over (partition by driver_id order by pinged_at) as prev_lat,
        lag(lon) over (partition by driver_id order by pinged_at) as prev_lon,
        lag(status) over (partition by driver_id order by pinged_at) as prev_status
    from {{ ref('stg_drivers__locations') }}
),

flagged as (
    select
        *,
        case
            when prev_at is null then 1
            when pinged_at - prev_at > interval '30 minutes' then 1
            when prev_status = 'offline' then 1
            else 0
        end as is_new_shift,
        case
            when prev_lat is null then 0.0
            else 2 * 6371.0088 * asin(
                sqrt(
                    power(sin(radians(lat - prev_lat) / 2), 2)
                    + cos(radians(prev_lat)) * cos(radians(lat))
                    * power(sin(radians(lon - prev_lon) / 2), 2)
                )
            )
        end as step_km
    from pings
),

numbered as (
    select
        *,
        sum(is_new_shift) over (
            partition by driver_id order by pinged_at
            rows between unbounded preceding and current row
        ) as shift_seq
    from flagged
)

select
    driver_id || '.' || shift_seq::text as shift_id,
    driver_id,
    min(city_id) as city_id,
    min(pinged_at) as shift_started_at,
    max(pinged_at) as shift_ended_at,
    extract(epoch from max(pinged_at) - min(pinged_at)) / 60.0 as active_minutes,
    sum(case when is_new_shift = 1 then 0.0 else step_km end) as distance_km,
    count(*) as ping_count,
    avg(speed_kmh) as avg_speed_kmh,
    sum(case when status = 'on_trip' then 1 else 0 end)::float / count(*) as on_trip_share
from numbered
group by driver_id, shift_seq
