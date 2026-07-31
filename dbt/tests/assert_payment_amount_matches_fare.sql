-- A successfully collected payment must match the completed ride's fare
-- exactly; the generator produces them equal, so any drift is a pipeline bug
-- (rounding, double-application of surge, wrong join).
select ride_id, fare_cents, paid_amount_cents, payment_status
from {{ ref('fct_rides') }}
where terminal_state = 'completed'
  and payment_status = 'completed'
  and paid_amount_cents is distinct from fare_cents
