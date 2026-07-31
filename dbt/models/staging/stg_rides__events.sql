-- One view per source table, renames and casts only, no business logic.
select
    ride_id,
    event_seq,
    event_id,
    event_type,
    event_ts as event_at,
    rider_id,
    driver_id,
    city_id,
    pickup_lat,
    pickup_lon,
    dropoff_lat,
    dropoff_lon,
    fare_cents::bigint as fare_cents,
    surge_multiplier,
    payload_version,
    promo_code,
    kafka_partition,
    kafka_offset,
    ingested_at
from {{ source('raw', 'rides_events') }}
