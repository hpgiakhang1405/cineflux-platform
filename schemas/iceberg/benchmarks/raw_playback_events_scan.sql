EXPLAIN ANALYZE
SELECT
    count(*) AS event_rows,
    sum(watch_seconds) AS total_watch_seconds
FROM iceberg.bronze.raw_playback_events
WHERE event_timestamp >= TIMESTAMP '2025-12-01 00:00:00'
  AND event_timestamp < TIMESTAMP '2026-01-01 00:00:00';
