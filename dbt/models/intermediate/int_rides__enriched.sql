-- Sessions joined to their payment and city dimension. One row per closed ride.
-- Payments arrive 5 to 90 seconds after completion by design, so a session can
-- legitimately be unpaid at read time; the left join keeps those rides visible
-- instead of silently dropping them.
with sessions as (
    select * from {{ ref('stg_rides__sessions') }}
),

payments as (
    select
        ride_id,
        txn_id,
        amount_cents,
        status as payment_status,
        method as payment_method,
        paid_at
    from {{ ref('stg_payments__transactions') }}
),

cities as (
    select * from {{ ref('dim_cities') }}
)

select
    s.ride_id,
    s.rider_id,
    s.driver_id,
    s.city_id,
    c.city_name,
    s.terminal_state,
    s.event_seq,
    s.requested_at,
    s.matched_at,
    s.driver_arrived_at,
    s.started_at,
    s.ended_at,
    s.time_to_match_sec,
    s.time_to_pickup_sec,
    s.ride_duration_sec,
    s.haversine_distance_km,
    s.avg_speed_kmh,
    s.is_late_arrival,
    s.fare_cents,
    s.surge_multiplier,
    s.promo_code,
    s.pickup_lat,
    s.pickup_lon,
    s.dropoff_lat,
    s.dropoff_lon,
    p.txn_id,
    p.amount_cents as paid_amount_cents,
    p.payment_status,
    p.payment_method,
    p.paid_at,
    p.txn_id is not null as is_paid,
    s.ingested_at
from sessions as s
left join payments as p on s.ride_id = p.ride_id
left join cities as c on s.city_id = c.city_id
