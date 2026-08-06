with fact_totals as (
    select
        count(*) as playback_count,
        sum(watch_seconds) as total_watch_seconds
    from {{ ref('fact_playback_session') }}
),

obt_totals as (
    select
        sum(playback_count) as playback_count,
        sum(total_watch_seconds) as total_watch_seconds
    from {{ ref('obt_user_content_engagement') }}
),

mart_totals as (
    select
        sum(playback_count) as playback_count,
        sum(total_watch_seconds) as total_watch_seconds
    from {{ ref('mart_content_trending') }}
)

select 'obt_user_content_engagement' as model_name
from fact_totals
cross join obt_totals
where fact_totals.playback_count <> obt_totals.playback_count
   or fact_totals.total_watch_seconds <> obt_totals.total_watch_seconds

union all

select 'mart_content_trending' as model_name
from fact_totals
cross join mart_totals
where fact_totals.playback_count <> mart_totals.playback_count
   or fact_totals.total_watch_seconds <> mart_totals.total_watch_seconds
