-- Hourly per-city rollup of the ride fact, the dashboard's workhorse table.
select
    city_id,
    city_name,
    date_trunc('hour', requested_at) as hour_start,
    count(*) as rides_closed,
    count(*) filter (where terminal_state = 'completed') as rides_completed,
    count(*) filter (where terminal_state = 'cancelled') as rides_cancelled,
    count(*) filter (where terminal_state = 'abandoned') as rides_abandoned,
    count(*) filter (where terminal_state = 'completed')::float
        / nullif(count(*), 0) as completion_rate,
    avg(surge_multiplier) as avg_surge,
    percentile_cont(0.5) within group (order by time_to_match_sec) as p50_time_to_match_sec,
    percentile_cont(0.95) within group (order by time_to_match_sec) as p95_time_to_match_sec,
    sum(fare_cents) filter (where terminal_state = 'completed') as gross_fares_cents,
    sum(paid_amount_cents) filter (where payment_status = 'completed') as collected_cents,
    count(*) filter (where is_late_arrival) as late_arrivals
from {{ ref('fct_rides') }}
group by city_id, city_name, date_trunc('hour', requested_at)
