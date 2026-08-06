SELECT
    count(*) AS data_files_scanned,
    round(avg(file_size_in_bytes)) AS average_file_size_bytes,
    sum(file_size_in_bytes) AS total_file_size_bytes,
    sum(record_count) AS record_count
FROM iceberg.bronze."raw_playback_events$files"
WHERE partition.event_timestamp_day >= DATE '2025-12-01'
  AND partition.event_timestamp_day < DATE '2026-01-01';
