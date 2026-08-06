with history as (
    select *
    from {{ ref('int_dim_content_history') }}
),

first_playback as (
    select
        content_id,
        min(session_started_timestamp) as first_playback_timestamp
    from {{ ref('stg_silver_playback_sessions') }}
    group by content_id
),

ranked as (
    select
        history.*,
        first_playback.first_playback_timestamp,
        row_number() over (
            partition by history.content_id
            order by history.valid_from_ts
        ) as version_number
    from history
    left join first_playback
        on history.content_id = first_playback.content_id
),

bootstrap_adjusted as (
    select
        content_id,
        content_type,
        title,
        genres,
        release_year,
        runtime_minutes,
        maturity_rating,
        original_language,
        availability_status,
        critic_score,
        case
            when version_number = 1 and first_playback_timestamp < valid_from_ts
                then first_playback_timestamp
            else valid_from_ts
        end as valid_from_ts,
        record_hash,
        created_timestamp
    from ranked
),

effective_ranges as (
    select
        *,
        lead(valid_from_ts) over (
            partition by content_id
            order by valid_from_ts
        ) as next_valid_from_ts
    from bootstrap_adjusted
)

select
    {{ cineflux_deterministic_hash(['content_id', 'to_iso8601(valid_from_ts)']) }} as content_sk,
    content_id,
    content_type,
    title,
    genres,
    release_year,
    runtime_minutes,
    maturity_rating,
    original_language,
    availability_status,
    critic_score,
    valid_from_ts,
    coalesce(
        next_valid_from_ts,
        cast('9999-12-31 23:59:59.999999 UTC' as timestamp(6) with time zone)
    ) as valid_to_ts,
    next_valid_from_ts is null as is_current,
    record_hash,
    created_timestamp
from effective_ranges
