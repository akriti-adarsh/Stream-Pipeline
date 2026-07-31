-- The ride fact: one row per closed ride, enriched with payment and city.
--
-- Incremental with MERGE on ride_id, filtered by an EVENT-TIME lookback:
--
--   requested_at >= max(requested_at) in the table minus 60 minutes
--
-- Why a lookback at all: closes arrive out of event-time order. A completed
-- ride's requested_at trails the newest request by its full lifecycle
-- (typically 15 to 25 sim-minutes), the generator delays events up to 10
-- sim-minutes past any watermark, payments trail completion by up to 90
-- seconds, and a session can be re-upserted by a richer close. A naive
-- "only newer than max" filter would silently drop all of those.
--
-- Why 60 minutes: worst realistic staleness is lifecycle (about 25 min) plus
-- deliberate lateness (10 min) plus re-derivation slack; 60 sim-minutes
-- covers that with margin while still pruning the scan. Anything older than
-- the lookback that STILL changes is caught by the merge key on the next
-- full-refresh; the late-data integration test pins the in-window behavior.
{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        incremental_strategy='merge',
    )
}}

select
    ride_id,
    rider_id,
    driver_id,
    city_id,
    city_name,
    terminal_state,
    event_seq,
    requested_at,
    matched_at,
    driver_arrived_at,
    started_at,
    ended_at,
    time_to_match_sec,
    time_to_pickup_sec,
    ride_duration_sec,
    haversine_distance_km,
    avg_speed_kmh,
    is_late_arrival,
    fare_cents,
    surge_multiplier,
    promo_code,
    txn_id,
    paid_amount_cents,
    payment_status,
    payment_method,
    paid_at,
    is_paid,
    ingested_at
from {{ ref('int_rides__enriched') }}

{% if is_incremental() %}
where requested_at >= (
    select coalesce(max(requested_at), '1900-01-01'::timestamptz) - interval '60 minutes'
    from {{ this }}
)
{% endif %}
