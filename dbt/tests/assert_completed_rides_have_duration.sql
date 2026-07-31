-- A completed ride whose started event ARRIVED must carry a positive duration:
-- the sessionizer derives it from the two timestamps. The started_at guard is
-- deliberate: this platform diverts malformed events to the DLQ, so a ride
-- can legitimately complete with its started event quarantined, and then a
-- null duration is correct behavior, not a defect. What can never happen is
-- a present start with a missing or non-positive duration.
select ride_id, terminal_state, started_at, ride_duration_sec
from {{ ref('fct_rides') }}
where terminal_state = 'completed'
  and started_at is not null
  and (ride_duration_sec is null or ride_duration_sec <= 0)
