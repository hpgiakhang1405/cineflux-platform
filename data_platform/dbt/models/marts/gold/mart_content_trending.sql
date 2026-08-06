{{ config(
    properties={
        "format": "'PARQUET'",
        "partitioning": "ARRAY['day(window_start)']"
    }
) }}

with hourly_engagement as (
    select
        date_trunc('hour', fact.session_started_timestamp) as window_start,
        date_add('hour', 1, date_trunc('hour', fact.session_started_timestamp)) as window_end,
        content.content_id,
        count(*) as playback_count,
        count(distinct users.user_id) as unique_viewer_count,
        count_if(fact.is_completed) as completed_session_count,
        sum(fact.watch_seconds) as total_watch_seconds,
        max(fact.created_timestamp) as created_timestamp
    from {{ ref('fact_playback_session') }} as fact
    inner join {{ ref('dim_user') }} as users
        on fact.user_sk = users.user_sk
    inner join {{ ref('dim_content') }} as content
        on fact.content_sk = content.content_sk
    group by
        date_trunc('hour', fact.session_started_timestamp),
        content.content_id
),

scored as (
    select
        *,
        cast(
            playback_count
            + completed_session_count * 2
            + total_watch_seconds / 3600.0
            as double
        ) as popularity_score
    from hourly_engagement
)

select
    window_start,
    window_end,
    content_id,
    playback_count,
    unique_viewer_count,
    completed_session_count,
    total_watch_seconds,
    popularity_score,
    dense_rank() over (
        partition by window_start
        order by popularity_score desc, content_id
    ) as popularity_rank,
    created_timestamp
from scored
