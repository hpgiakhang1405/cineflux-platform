{{ config(
    alias='mart_content_trending',
    unique_key=['window_start', 'content_id'],
    incremental_strategy='delete+insert',
    views_enabled=false,
    on_schema_change='sync_all_columns'
) }}

select
    window_start,
    window_end,
    content_id,
    playback_count,
    unique_viewer_count,
    completed_session_count,
    total_watch_seconds,
    popularity_score,
    popularity_rank,
    created_timestamp
from {{ ref('mart_content_trending') }}
