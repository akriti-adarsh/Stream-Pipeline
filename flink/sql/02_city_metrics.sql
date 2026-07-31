-- City metrics: 1-minute tumbling windows per city, plus the late side output.
--
-- EMIT semantics: a TUMBLE window emits exactly once, when the watermark
-- passes window_end. The watermark trails max(event_ts) by 30 seconds (the
-- generator's documented out-of-order skew), so results appear about 30
-- seconds of EVENT time after each minute closes. Rows arriving after the
-- watermark has passed their window are DROPPED by the aggregate: that is the
-- price of emitting final results exactly once instead of retracting. They do
-- not vanish, though: the third insert below catches every row older than the
-- current watermark and diverts it to the late.events topic, where the late
-- data test and the dbt lookback pick it up.
--
-- p50/p95 time-to-match: Flink 1.20 SQL has no percentile aggregate, so the
-- second insert emits 5-second histogram buckets per window (61 buckets, the
-- last one open-ended) and the Postgres view flink.city_tt_match_percentiles
-- finishes the quantile math. Conservative by at most one bucket width (5 s).

SET 'pipeline.name' = 'city-metrics-and-late-output';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO pg_city_metrics
SELECT
    window_start,
    window_end,
    city_id,
    COUNT(*) FILTER (WHERE event_type = 'requested') AS requests,
    COUNT(*) FILTER (WHERE event_type = 'completed') AS completes,
    COUNT(*) FILTER (WHERE event_type = 'cancelled') AS cancels,
    CAST(COUNT(*) FILTER (WHERE event_type = 'completed') AS DOUBLE)
        / CAST(NULLIF(COUNT(*) FILTER (WHERE event_type IN ('completed', 'cancelled')), 0) AS DOUBLE)
        AS completion_rate,
    CAST(COUNT(*) FILTER (WHERE event_type = 'cancelled') AS DOUBLE)
        / CAST(NULLIF(COUNT(*) FILTER (WHERE event_type IN ('completed', 'cancelled')), 0) AS DOUBLE)
        AS cancellation_rate,
    AVG(surge_multiplier) FILTER (WHERE event_type = 'requested') AS mean_surge
FROM TABLE(TUMBLE(TABLE rides_events, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, city_id;

INSERT INTO pg_city_tt_match_hist
SELECT
    window_start,
    window_end,
    city_id,
    LEAST(CAST(FLOOR(time_to_match_sec / 5) AS INT), 60) AS bucket,
    COUNT(*) AS cnt
FROM TABLE(TUMBLE(TABLE rides_sessions, DESCRIPTOR(session_ts), INTERVAL '1' MINUTE))
WHERE time_to_match_sec IS NOT NULL
GROUP BY window_start, window_end, city_id, LEAST(CAST(FLOOR(time_to_match_sec / 5) AS INT), 60);

-- The late side output: every rides.events row already older than the current
-- watermark, i.e. exactly the rows the tumbling aggregate above will drop.
INSERT INTO late_events
SELECT
    event_id,
    ride_id,
    event_type,
    event_ts,
    city_id,
    TIMESTAMPDIFF(SECOND, event_ts, CURRENT_WATERMARK(event_ts)) AS late_by_sec
FROM rides_events
WHERE CURRENT_WATERMARK(event_ts) IS NOT NULL
  AND event_ts < CURRENT_WATERMARK(event_ts);

END;
