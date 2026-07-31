-- Guard against timestamp corruption (classic failure: epoch seconds written
-- where millis belong, which lands decades away). The platform runs on an
-- ACCELERATED simulation clock: at speed 60 a ten-minute run legitimately
-- produces event times ten HOURS past the wall clock, so "no future" here
-- means "not absurdly future": nothing beyond 14 days, far past any plausible
-- acceleration, far short of any unit mixup.
select event_id, event_at
from {{ ref('stg_rides__events') }}
where event_at > now() + interval '14 days'
