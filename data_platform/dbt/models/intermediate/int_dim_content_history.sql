with hashed as (
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
        source_updated_timestamp as valid_from_ts,
        ingested_timestamp as created_timestamp,
        {{ cineflux_deterministic_hash([
            'content_type',
            'title',
            'json_format(cast(genres as json))',
            'release_year',
            'runtime_minutes',
            'maturity_rating',
            'original_language',
            'availability_status',
            'critic_score'
        ]) }} as record_hash
    from {{ ref('stg_silver_content') }}
),

ordered as (
    select
        *,
        lag(record_hash) over (
            partition by content_id
            order by valid_from_ts
        ) as previous_record_hash
    from hashed
)

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
    valid_from_ts,
    created_timestamp,
    record_hash
from ordered
where previous_record_hash is null
   or previous_record_hash <> record_hash
