-- SCD Type 2 driver dimension read from the dbt snapshot: every version of
-- every driver's (status, tier, city) with its validity interval.
select
    driver_id || '.' || to_char(dbt_valid_from, 'YYYYMMDDHH24MISSMS') as driver_version_key,
    driver_id,
    city_id,
    status,
    tier,
    completed_rides,
    last_seen_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('drivers_snapshot') }}
