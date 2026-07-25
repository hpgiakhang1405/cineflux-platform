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
    source_updated_timestamp,
    ingested_timestamp,
    record_hash as source_record_hash,
    pipeline_run_id
from {{ source('silver', 'stg_content') }}
