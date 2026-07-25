{{ config(
    properties={
        "format": "'PARQUET'",
        "partitioning": "ARRAY['month(activity_date)']"
    }
) }}

with sessions as (
    select *
    from {{ ref('stg_silver_playback_sessions') }}
)

select
    sessions.session_id,
    users.user_sk,
    content.content_sk,
    sessions.session_started_timestamp,
    sessions.session_ended_timestamp,
    cast(sessions.session_started_timestamp as date) as activity_date,
    sessions.watch_seconds,
    sessions.content_runtime_seconds,
    sessions.completion_rate,
    sessions.is_completed,
    sessions.event_count,
    sessions.created_timestamp,
    sessions.pipeline_run_id
from sessions
left join {{ ref('dim_user') }} as users
    on sessions.user_id = users.user_id
   and sessions.session_started_timestamp >= users.valid_from_ts
   and sessions.session_started_timestamp < users.valid_to_ts
left join {{ ref('dim_content') }} as content
    on sessions.content_id = content.content_id
   and sessions.session_started_timestamp >= content.valid_from_ts
   and sessions.session_started_timestamp < content.valid_to_ts
