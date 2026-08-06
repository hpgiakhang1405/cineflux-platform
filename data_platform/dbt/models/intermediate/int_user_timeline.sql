with user_versions as (
    select *
    from {{ ref('stg_silver_users') }}
),

subscription_versions as (
    select *
    from {{ ref('stg_silver_subscriptions') }}
),

change_timeline as (
    select user_id, source_updated_timestamp as valid_from_ts
    from user_versions

    union

    select user_id, source_updated_timestamp as valid_from_ts
    from subscription_versions
),

resolved_candidates as (
    select
        timeline.user_id,
        users.country_code,
        users.city,
        users.birth_year,
        users.preferred_language,
        users.is_marketing_opt_in,
        subscriptions.subscription_tier,
        subscriptions.subscription_status,
        timeline.valid_from_ts,
        greatest(
            users.ingested_timestamp,
            coalesce(subscriptions.ingested_timestamp, users.ingested_timestamp)
        ) as created_timestamp,
        row_number() over (
            partition by timeline.user_id, timeline.valid_from_ts
            order by
                users.source_updated_timestamp desc,
                users.ingested_timestamp desc,
                subscriptions.source_updated_timestamp desc nulls last,
                subscriptions.ingested_timestamp desc nulls last
        ) as candidate_rank
    from change_timeline as timeline
    left join user_versions as users
        on timeline.user_id = users.user_id
       and users.source_updated_timestamp <= timeline.valid_from_ts
    left join subscription_versions as subscriptions
        on timeline.user_id = subscriptions.user_id
       and subscriptions.source_updated_timestamp <= timeline.valid_from_ts
    where users.user_id is not null
),

resolved as (
    select
        user_id,
        country_code,
        city,
        birth_year,
        preferred_language,
        is_marketing_opt_in,
        subscription_tier,
        subscription_status,
        valid_from_ts,
        created_timestamp,
        {{ cineflux_deterministic_hash([
            'country_code',
            'city',
            'birth_year',
            'preferred_language',
            'is_marketing_opt_in',
            'subscription_tier',
            'subscription_status'
        ]) }} as record_hash
    from resolved_candidates
    where candidate_rank = 1
)

select
    user_id,
    country_code,
    city,
    birth_year,
    preferred_language,
    is_marketing_opt_in,
    subscription_tier,
    subscription_status,
    valid_from_ts,
    created_timestamp,
    record_hash
from resolved
