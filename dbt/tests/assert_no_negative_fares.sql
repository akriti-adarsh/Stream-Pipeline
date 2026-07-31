-- A negative fare is impossible in the domain: the pricing formula is a sum of
-- non-negative components times a surge floor-capped at 1.0. Any row here means
-- corruption upstream.
select ride_id, fare_cents
from {{ ref('fct_rides') }}
where fare_cents < 0
