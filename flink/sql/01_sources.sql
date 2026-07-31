-- Source and sink table definitions for every Flink job (loaded via
-- sql-client -i before each job file, so each job session sees them all).
--
-- Formats: rides.events, drivers.locations, rides.sessions are Avro against
-- the Confluent-compatible registry Redpanda ships (avro-confluent).
-- payments.transactions is PLAIN JSON (no registry framing; open-source Flink
-- has no confluent-json format, see DEVIATIONS.md).
--
-- Every consumer of the transactionally-produced rides.sessions topic reads
-- with isolation.level=read_committed, and rides.events gets the same for
-- consistency: aborted transactions must never leak into aggregates.
--
-- Watermarks: event time minus 30 seconds, the documented tolerance for the
-- generator's out-of-order skew. Anything later than that is LATE relative to
-- the watermark and is dropped by windowed aggregates; the late side output
-- job (02) captures exactly those rows into the late.events topic instead of
-- letting them vanish silently.

SET 'table.local-time-zone' = 'UTC';

CREATE TABLE rides_events (
    event_id STRING,
    ride_id STRING,
    event_type STRING,
    event_ts TIMESTAMP(3),
    rider_id STRING,
    driver_id STRING,
    city_id INT,
    pickup_lat DOUBLE,
    pickup_lon DOUBLE,
    dropoff_lat DOUBLE,
    dropoff_lon DOUBLE,
    fare_cents BIGINT,
    surge_multiplier DOUBLE,
    payload_version INT,
    promo_code STRING,
    WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
    -- rides.events.clean: the sessionizer's transactionally-produced poison
    -- firewall mirror. The raw rides.events topic contains deliberately
    -- corrupt records, and open-source Flink's avro-confluent format has no
    -- skip-on-error option: one poison message fails the whole job. The
    -- mirror carries every clean event with identical event times (late and
    -- out-of-order included), exactly once. See DEVIATIONS.md.
    'connector' = 'kafka',
    'topic' = 'rides.events.clean',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-rides-events',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://redpanda:8081'
);

CREATE TABLE drivers_locations (
    driver_id STRING,
    ts TIMESTAMP(3),
    lat DOUBLE,
    lon DOUBLE,
    speed_kmh DOUBLE,
    heading DOUBLE,
    status STRING,
    city_id INT,
    WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'drivers.locations',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-drivers-locations',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://redpanda:8081'
);

CREATE TABLE rides_sessions (
    ride_id STRING,
    rider_id STRING,
    driver_id STRING,
    city_id INT,
    terminal_state STRING,
    event_seq INT,
    requested_ts TIMESTAMP(3),
    matched_ts TIMESTAMP(3),
    driver_arrived_ts TIMESTAMP(3),
    started_ts TIMESTAMP(3),
    ended_ts TIMESTAMP(3),
    time_to_match_sec DOUBLE,
    time_to_pickup_sec DOUBLE,
    ride_duration_sec DOUBLE,
    haversine_distance_km DOUBLE,
    avg_speed_kmh DOUBLE,
    is_late_arrival BOOLEAN,
    fare_cents BIGINT,
    surge_multiplier DOUBLE,
    promo_code STRING,
    pickup_lat DOUBLE,
    pickup_lon DOUBLE,
    dropoff_lat DOUBLE,
    dropoff_lon DOUBLE,
    -- The rowtime lives on a computed column: ended_ts is NULL for abandoned
    -- closes, and a watermark column must be non-null all the way down to the
    -- derived Avro reader schema (a null there kills deserialisation).
    session_ts AS COALESCE(ended_ts, requested_ts),
    WATERMARK FOR session_ts AS session_ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'rides.sessions',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-rides-sessions',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://redpanda:8081'
);

CREATE TABLE payments_transactions (
    txn_id STRING,
    ride_id STRING,
    amount_cents BIGINT,
    status STRING,
    `method` STRING,
    ts BIGINT,
    payment_ts AS CAST(TO_TIMESTAMP_LTZ(ts, 3) AS TIMESTAMP(3)),
    WATERMARK FOR payment_ts AS payment_ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments.transactions',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-payments',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    -- Payments carry the generator's corrupt records too; JSON does have a
    -- skip switch, so the raw topic is consumed directly here.
    'json.ignore-parse-errors' = 'true'
);

-- Side-output destination for rows arriving past the watermark.
CREATE TABLE late_events (
    event_id STRING,
    ride_id STRING,
    event_type STRING,
    event_ts TIMESTAMP(3),
    city_id INT,
    late_by_sec BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'late.events',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'format' = 'json'
);

-- JDBC sinks; the backing tables are created by postgres/init/02_flink_tables.sql.
CREATE TABLE pg_city_metrics (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    city_id INT,
    requests BIGINT,
    completes BIGINT,
    cancels BIGINT,
    completion_rate DOUBLE,
    cancellation_rate DOUBLE,
    mean_surge DOUBLE,
    PRIMARY KEY (window_start, city_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/stream',
    'table-name' = 'flink.city_metrics',
    'username' = 'stream',
    'password' = 'stream'
);

CREATE TABLE pg_city_tt_match_hist (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    city_id INT,
    bucket INT,
    cnt BIGINT,
    PRIMARY KEY (window_start, city_id, bucket) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/stream',
    'table-name' = 'flink.city_tt_match_hist',
    'username' = 'stream',
    'password' = 'stream'
);

CREATE TABLE pg_driver_utilisation (
    driver_id STRING,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    city_id INT,
    ping_count BIGINT,
    active_minutes DOUBLE,
    distance_km DOUBLE,
    PRIMARY KEY (driver_id, session_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/stream',
    'table-name' = 'flink.driver_utilisation',
    'username' = 'stream',
    'password' = 'stream'
);

CREATE TABLE pg_ride_payments (
    ride_id STRING,
    txn_id STRING,
    city_id INT,
    terminal_state STRING,
    ended_ts TIMESTAMP(3),
    fare_cents BIGINT,
    amount_cents BIGINT,
    payment_status STRING,
    payment_method STRING,
    payment_ts TIMESTAMP(3),
    payment_delay_sec DOUBLE,
    PRIMARY KEY (ride_id, txn_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/stream',
    'table-name' = 'flink.ride_payments',
    'username' = 'stream',
    'password' = 'stream'
);

CREATE TABLE pg_unpaid_rides (
    ride_id STRING,
    city_id INT,
    terminal_state STRING,
    ended_ts TIMESTAMP(3),
    fare_cents BIGINT,
    PRIMARY KEY (ride_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/stream',
    'table-name' = 'flink.unpaid_rides',
    'username' = 'stream',
    'password' = 'stream'
);
