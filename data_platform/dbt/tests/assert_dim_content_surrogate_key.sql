select *
from {{ ref('dim_content') }}
where content_sk <> {{ cineflux_deterministic_hash(['content_id', 'to_iso8601(valid_from_ts)']) }}
