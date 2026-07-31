-- One view per source table, renames and casts only, no business logic.
select
    driver_id,
    ts as pinged_at,
    lat,
    lon,
    speed_kmh,
    heading,
    status,
    city_id
from {{ source('raw', 'driver_locations') }}
