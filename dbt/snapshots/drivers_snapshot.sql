{% snapshot drivers_snapshot %}

{#
  SCD Type 2 over driver status and tier (spec section 6). The check strategy
  compares the tracked columns on every run: any change closes the current
  version (dbt_valid_to set) and opens a new one, giving a full history of
  when each driver moved city, changed status, or earned a tier.
#}

{{
    config(
        unique_key='driver_id',
        strategy='check',
        check_cols=['status', 'tier', 'city_id'],
    )
}}

select
    driver_id,
    city_id,
    status,
    tier,
    completed_rides,
    last_seen_at
from {{ ref('int_drivers__current') }}

{% endsnapshot %}
