-- Driver utilisation: session windows over the GPS ping stream, 5-minute gap.
--
-- Syntax note (spec review round 3): Flink 1.20's SESSION table-valued
-- function is only partially supported (it requires PARTITION BY and still
-- carries limitations), so this job was validated live; if the TVF form ever
-- regresses, the legacy GROUP BY SESSION(ts, INTERVAL '5' MINUTE) group-window
-- syntax is the documented fallback.
--
-- Distance: the exact per-step haversine needs LAG over event time BEFORE
-- windowing, which the streaming planner rejects inside this pipeline shape
-- (time attribute is consumed by the OVER window). The distance here is the
-- documented approximation distance = mean reported speed x active time,
-- which for smooth simulated tracks lands within a few percent of the
-- step-sum. The batch-exact version lives in dbt (int_drivers__shifts), where
-- LAG is free; comparing the two is a deliberate stream-vs-batch teaching
-- point.

SET 'pipeline.name' = 'driver-utilisation';

INSERT INTO pg_driver_utilisation
SELECT
    driver_id,
    window_start AS session_start,
    window_end AS session_end,
    MIN(city_id) AS city_id,
    COUNT(*) AS ping_count,
    CAST(TIMESTAMPDIFF(SECOND, MIN(ts), MAX(ts)) AS DOUBLE) / 60.0 AS active_minutes,
    AVG(speed_kmh) * (CAST(TIMESTAMPDIFF(SECOND, MIN(ts), MAX(ts)) AS DOUBLE) / 3600.0)
        AS distance_km
FROM TABLE(
    SESSION(TABLE drivers_locations PARTITION BY driver_id, DESCRIPTOR(ts), INTERVAL '5' MINUTES)
)
GROUP BY driver_id, window_start, window_end;
