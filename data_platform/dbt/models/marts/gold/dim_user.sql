with history as (
    select *
    from {{ ref('int_dim_user_history') }}
),

first_playback as (
    select
        user_id,
        min(session_started_timestamp) as first_playback_timestamp
    from {{ ref('stg_silver_playback_sessions') }}
    group by user_id
),

ranked as (
    select
        history.*,
        first_playback.first_playback_timestamp,
        row_number() over (
            partition by history.user_id
            order by history.valid_from_ts
        ) as version_number
    from history
    left join first_playback
        on history.user_id = first_playback.user_id
),

bootstrap_adjusted as (
    select
        user_id,
        country_code,
        city,
        birth_year,
        preferred_language,
        is_marketing_opt_in,
        subscription_tier,
        subscription_status,
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
            partition by user_id
            order by valid_from_ts
        ) as next_valid_from_ts
    from bootstrap_adjusted
)

select
    {{ cineflux_deterministic_hash(['user_id', 'to_iso8601(valid_from_ts)']) }} as user_sk,
    user_id,
    country_code,
    city,
    birth_year,
    preferred_language,
    is_marketing_opt_in,
    subscription_tier,
    subscription_status,
    valid_from_ts,
    coalesce(
        next_valid_from_ts,
        cast('9999-12-31 23:59:59.999999 UTC' as timestamp(6) with time zone)
    ) as valid_to_ts,
    next_valid_from_ts is null as is_current,
    record_hash,
    created_timestamp
from effective_ranges
