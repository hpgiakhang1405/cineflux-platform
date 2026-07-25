with daily_engagement as (
    select
        users.user_id,
        content.content_id,
        fact.activity_date,
        count(*) as playback_count,
        count_if(fact.is_completed) as completed_session_count,
        sum(fact.watch_seconds) as total_watch_seconds,
        avg(fact.completion_rate) as average_completion_rate,
        max(fact.created_timestamp) as created_timestamp
    from {{ ref('fact_playback_session') }} as fact
    inner join {{ ref('dim_user') }} as users
        on fact.user_sk = users.user_sk
    inner join {{ ref('dim_content') }} as content
        on fact.content_sk = content.content_sk
    group by
        users.user_id,
        content.content_id,
        fact.activity_date
)

select
    user_id,
    content_id,
    activity_date,
    playback_count,
    completed_session_count,
    total_watch_seconds,
    average_completion_rate,
    coalesce(
        date_diff(
            'day',
            lag(activity_date) over (
                partition by user_id, content_id
                order by activity_date
            ),
            activity_date
        ),
        0
    ) as days_since_last_playback,
    created_timestamp
from daily_engagement
