select
    user_id,
    subscription_tier,
    subscription_status,
    billing_country_code,
    started_date,
    ended_date,
    source_updated_timestamp,
    ingested_timestamp,
    record_hash as source_record_hash,
    pipeline_run_id
from {{ source('silver', 'stg_subscriptions') }}
