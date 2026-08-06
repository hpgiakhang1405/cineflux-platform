{{ config(
    properties={
        "format": "'PARQUET'",
        "partitioning": "ARRAY['day(event_timestamp)']"
    }
) }}

with anchors as (
    select distinct
        user_id,
        activity_date
    from {{ ref('obt_user_content_engagement') }}
),

rolling as (
    select
        anchors.user_id,
        anchors.activity_date,
        round(sum(
            case
                when engagement.activity_date = anchors.activity_date
                then engagement.total_watch_seconds
                else 0
            end
        ) / 60.0, 6) as watch_minutes_24h,
        round(sum(
            case
                when engagement.activity_date >= date_add('day', -6, anchors.activity_date)
                then engagement.total_watch_seconds
                else 0
            end
        ) / 60.0, 6) as watch_minutes_7d,
        sum(
            case
                when engagement.activity_date >= date_add('day', -6, anchors.activity_date)
                then engagement.playback_count
                else 0
            end
        ) as playback_sessions_7d,
        sum(engagement.completed_session_count) as completed_sessions_30d,
        cast(
            sum(
                cast(engagement.average_completion_rate as decimal(18, 12))
                    * engagement.playback_count
            ) / nullif(sum(engagement.playback_count), 0)
            as decimal(18, 6)
        ) as average_completion_rate_30d,
        count(distinct engagement.content_id) as distinct_content_count_30d,
        date_diff('day', max(engagement.activity_date), anchors.activity_date)
            as days_since_last_playback,
        max(engagement.created_timestamp) as created
    from anchors
    inner join {{ ref('obt_user_content_engagement') }} as engagement
        on anchors.user_id = engagement.user_id
       and engagement.activity_date between date_add('day', -29, anchors.activity_date)
                                        and anchors.activity_date
    group by anchors.user_id, anchors.activity_date
)

select
    user_id,
    watch_minutes_24h,
    watch_minutes_7d,
    playback_sessions_7d,
    completed_sessions_30d,
    average_completion_rate_30d,
    distinct_content_count_30d,
    days_since_last_playback,
    with_timezone(
        cast(date_add('day', 1, activity_date) as timestamp(6)),
        'UTC'
    ) as event_timestamp,
    created
from rolling
