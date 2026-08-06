select
    user_id,
    country_code,
    city,
    birth_year,
    preferred_language,
    is_marketing_opt_in,
    source_updated_timestamp,
    ingested_timestamp,
    record_hash as source_record_hash,
    pipeline_run_id
from {{ source('silver', 'stg_users') }}
