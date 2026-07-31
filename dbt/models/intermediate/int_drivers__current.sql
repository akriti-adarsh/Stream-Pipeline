-- Current mutable state per driver: the snapshot source for the SCD2 dimension.
-- status comes from the latest ping; tier is earned from lifetime completed
-- rides, so it CHANGES as the stream runs, which is exactly what gives the
-- SCD2 snapshot real version history to capture.
with latest_ping as (
    select distinct on (driver_id)
        driver_id,
        city_id,
        status,
        pinged_at as last_seen_at
    from {{ ref('stg_drivers__locations') }}
    order by driver_id, pinged_at desc
),

completed as (
    select
        driver_id,
        count(*) as completed_rides
    from {{ ref('stg_rides__sessions') }}
    where terminal_state = 'completed'
    group by driver_id
)

select
    p.driver_id,
    p.city_id,
    p.status,
    p.last_seen_at,
    coalesce(c.completed_rides, 0) as completed_rides,
    case
        when coalesce(c.completed_rides, 0) >= 30 then 'gold'
        when coalesce(c.completed_rides, 0) >= 10 then 'silver'
        else 'bronze'
    end as tier
from latest_ping as p
left join completed as c on p.driver_id = c.driver_id
