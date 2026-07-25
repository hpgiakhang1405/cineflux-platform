with ordered as (
    select
        *,
        lag(record_hash) over (
            partition by user_id
            order by valid_from_ts
        ) as previous_record_hash
    from {{ ref('int_user_timeline') }}
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
from ordered
where previous_record_hash is null
   or previous_record_hash <> record_hash
