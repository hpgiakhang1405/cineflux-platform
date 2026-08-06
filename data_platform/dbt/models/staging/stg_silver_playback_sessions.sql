select
    session_id,
    user_id,
    content_id,
    session_started_timestamp,
    session_ended_timestamp,
    watch_seconds,
    content_runtime_seconds,
    completion_rate,
    is_completed,
    event_count,
    created_timestamp,
    pipeline_run_id
from {{ source('silver', 'int_playback_sessions') }}
