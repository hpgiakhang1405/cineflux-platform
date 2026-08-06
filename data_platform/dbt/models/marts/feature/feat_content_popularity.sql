{{ config(
    properties={
        "format": "'PARQUET'",
        "partitioning": "ARRAY['day(event_timestamp)']"
    }
) }}

with anchors as (
    select
        content_id,
        window_start,
        window_end
    from {{ ref('mart_content_trending') }}
),

mart_rollups as (
    select
        anchors.content_id,
        anchors.window_start,
        anchors.window_end,
        sum(
            case
                when trending.window_start = anchors.window_start
                then trending.playback_count
                else 0
            end
        ) as playback_count_1h,
        sum(
            case
                when trending.window_start >= date_add('hour', -23, anchors.window_start)
                then trending.playback_count
                else 0
            end
        ) as playback_count_24h,
        round(
            sum(trending.completed_session_count) * 1.0
                / nullif(sum(trending.playback_count), 0),
            6
        ) as completion_rate_7d,
        round(sum(
            case
                when trending.window_start >= date_add('hour', -23, anchors.window_start)
                then trending.popularity_score
                else 0.0
            end
        ), 6) as popularity_score_24h,
        max(trending.created_timestamp) as mart_created
    from anchors
    inner join {{ ref('mart_content_trending') }} as trending
        on anchors.content_id = trending.content_id
       and trending.window_start between date_add('hour', -167, anchors.window_start)
                                         and anchors.window_start
    group by
        anchors.content_id,
        anchors.window_start,
        anchors.window_end
),

session_entities as (
    select
        content.content_id,
        users.user_id,
        fact.session_started_timestamp,
        fact.created_timestamp
    from {{ ref('fact_playback_session') }} as fact
    inner join {{ ref('dim_user') }} as users
        on fact.user_sk = users.user_sk
    inner join {{ ref('dim_content') }} as content
        on fact.content_sk = content.content_sk
),

exact_viewers as (
    select
        anchors.content_id,
        anchors.window_start,
        count(distinct sessions.user_id) as unique_viewer_count_24h,
        max(sessions.created_timestamp) as session_created
    from anchors
    inner join session_entities as sessions
        on anchors.content_id = sessions.content_id
       and sessions.session_started_timestamp >= date_add('hour', -24, anchors.window_end)
       and sessions.session_started_timestamp < anchors.window_end
    group by anchors.content_id, anchors.window_start
)

select
    rollups.content_id,
    rollups.playback_count_1h,
    rollups.playback_count_24h,
    viewers.unique_viewer_count_24h,
    rollups.completion_rate_7d,
    rollups.popularity_score_24h,
    rollups.window_end as event_timestamp,
    greatest(rollups.mart_created, viewers.session_created) as created
from mart_rollups as rollups
inner join exact_viewers as viewers
    on rollups.content_id = viewers.content_id
   and rollups.window_start = viewers.window_start
