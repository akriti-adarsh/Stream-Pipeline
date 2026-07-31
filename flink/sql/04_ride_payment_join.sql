-- Ride-payment interval join, 15-minute bound, plus the left-outer variant
-- that surfaces unpaid rides.
--
-- The join window exists because payments are DESIGNED to lag completion by 5
-- to 90 seconds (and real payment systems by much more): an equi-join would
-- either buffer state forever or miss late payments. The interval join keeps
-- only 15 minutes of session state per side and emits unpaid rides exactly
-- once, when the watermark proves no payment can arrive inside the bound
-- anymore.

SET 'pipeline.name' = 'ride-payment-join';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO pg_ride_payments
SELECT
    s.ride_id,
    p.txn_id,
    s.city_id,
    s.terminal_state,
    -- Cast the payment rowtime to a plain timestamp: a sink may carry at
    -- most one rowtime column, and this table needs none as event time.
    s.ended_ts,
    s.fare_cents,
    p.amount_cents,
    p.status AS payment_status,
    p.`method` AS payment_method,
    CAST(p.payment_ts AS TIMESTAMP(3)) AS payment_ts,
    CAST(TIMESTAMPDIFF(SECOND, s.ended_ts, p.payment_ts) AS DOUBLE) AS payment_delay_sec
FROM rides_sessions s
JOIN payments_transactions p
    ON s.ride_id = p.ride_id
    AND p.payment_ts BETWEEN s.session_ts AND s.session_ts + INTERVAL '15' MINUTE;

INSERT INTO pg_unpaid_rides
SELECT
    s.ride_id,
    s.city_id,
    s.terminal_state,
    s.ended_ts,
    s.fare_cents
FROM rides_sessions s
LEFT OUTER JOIN payments_transactions p
    ON s.ride_id = p.ride_id
    AND p.payment_ts BETWEEN s.session_ts AND s.session_ts + INTERVAL '15' MINUTE
WHERE s.terminal_state = 'completed' AND p.txn_id IS NULL;

END;
