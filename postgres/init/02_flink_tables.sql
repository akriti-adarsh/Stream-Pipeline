-- Tables backing the Flink SQL JDBC sinks (schema "flink").
-- The Flink JDBC connector never creates tables; every sink table declared in
-- flink/sql/01_sources.sql must already exist here with a matching shape.
-- Primary keys give the sinks upsert semantics (INSERT .. ON CONFLICT DO
-- UPDATE), so a resubmitted job replaying a window overwrites instead of
-- duplicating.
--
-- Runs automatically on the first boot of an empty postgres volume
-- (docker-entrypoint-initdb.d); safe to re-apply at any time by hand:
--   docker compose exec -T postgres psql -U stream -d stream \
--     -f /docker-entrypoint-initdb.d/02_flink_tables.sql

CREATE SCHEMA IF NOT EXISTS flink;

-- 1-minute tumbling window per city over rides.events
-- (written by flink/sql/02_city_metrics.sql, keyed on window_start+city_id).
CREATE TABLE IF NOT EXISTS flink.city_metrics (
    window_start      timestamp(3) NOT NULL,
    window_end        timestamp(3) NOT NULL,
    city_id           integer      NOT NULL,
    requests          bigint       NOT NULL,
    completes         bigint       NOT NULL,
    cancels           bigint       NOT NULL,
    completion_rate   double precision,
    cancellation_rate double precision,
    mean_surge        double precision,
    PRIMARY KEY (window_start, city_id)
);

-- Histogram of time_to_match_sec per 1-minute window per city, from
-- rides.sessions. Flink 1.20 has no percentile aggregate, so the stream job
-- emits 5-second buckets (bucket = floor(time_to_match_sec / 5), capped at
-- 60 = "300 s or more") and the view below finishes the p50/p95 math.
CREATE TABLE IF NOT EXISTS flink.city_tt_match_hist (
    window_start timestamp(3) NOT NULL,
    window_end   timestamp(3) NOT NULL,
    city_id      integer      NOT NULL,
    bucket       integer      NOT NULL,
    cnt          bigint       NOT NULL,
    PRIMARY KEY (window_start, city_id, bucket)
);

-- p50/p95 time-to-match per window per city, computed from the histogram.
-- The quantile is reported as the UPPER EDGE of the first bucket whose
-- cumulative count crosses the quantile, so values are conservative and at
-- most one bucket width (5 s) above the exact order statistic.
CREATE OR REPLACE VIEW flink.city_tt_match_percentiles AS
WITH cume AS (
    SELECT window_start,
           window_end,
           city_id,
           bucket,
           cnt,
           SUM(cnt) OVER (PARTITION BY window_start, city_id
                          ORDER BY bucket)       AS cum_cnt,
           SUM(cnt) OVER (PARTITION BY window_start, city_id) AS total_cnt
    FROM flink.city_tt_match_hist
)
SELECT window_start,
       window_end,
       city_id,
       MIN(CASE WHEN cum_cnt >= CEIL(0.50 * total_cnt)
                THEN (bucket + 1) * 5.0 END) AS p50_time_to_match_sec,
       MIN(CASE WHEN cum_cnt >= CEIL(0.95 * total_cnt)
                THEN (bucket + 1) * 5.0 END) AS p95_time_to_match_sec,
       MAX(total_cnt)                        AS matched_rides
FROM cume
GROUP BY window_start, window_end, city_id;

-- Convenience join of counts and percentiles per window per city.
CREATE OR REPLACE VIEW flink.city_metrics_full AS
SELECT m.window_start,
       m.window_end,
       m.city_id,
       m.requests,
       m.completes,
       m.cancels,
       m.completion_rate,
       m.cancellation_rate,
       m.mean_surge,
       p.p50_time_to_match_sec,
       p.p95_time_to_match_sec
FROM flink.city_metrics m
LEFT JOIN flink.city_tt_match_percentiles p
       ON p.window_start = m.window_start AND p.city_id = m.city_id;

-- Session windows (5-minute gap) over drivers.locations
-- (written by flink/sql/03_driver_utilisation.sql).
CREATE TABLE IF NOT EXISTS flink.driver_utilisation (
    driver_id      text         NOT NULL,
    session_start  timestamp(3) NOT NULL,
    session_end    timestamp(3) NOT NULL,
    city_id        integer,
    ping_count     bigint       NOT NULL,
    active_minutes double precision NOT NULL,
    distance_km    double precision NOT NULL,
    PRIMARY KEY (driver_id, session_start)
);

-- Interval join rides.sessions x payments.transactions
-- (written by flink/sql/04_ride_payment_join.sql).
CREATE TABLE IF NOT EXISTS flink.ride_payments (
    ride_id           text NOT NULL,
    txn_id            text NOT NULL,
    city_id           integer,
    terminal_state    text,
    ended_ts          timestamp(3),
    fare_cents        bigint,
    amount_cents      bigint,
    payment_status    text,
    payment_method    text,
    payment_ts        timestamp(3),
    payment_delay_sec double precision,
    PRIMARY KEY (ride_id, txn_id)
);

-- Completed rides with no payment within the join bound (left-outer variant).
CREATE TABLE IF NOT EXISTS flink.unpaid_rides (
    ride_id        text NOT NULL,
    city_id        integer,
    terminal_state text,
    ended_ts       timestamp(3),
    fare_cents     bigint,
    PRIMARY KEY (ride_id)
);
