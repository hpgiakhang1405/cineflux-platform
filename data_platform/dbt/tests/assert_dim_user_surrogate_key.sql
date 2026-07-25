select *
from {{ ref('dim_user') }}
where user_sk <> {{ cineflux_deterministic_hash(['user_id', 'to_iso8601(valid_from_ts)']) }}
