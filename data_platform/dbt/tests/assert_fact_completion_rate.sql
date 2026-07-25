select *
from {{ ref('fact_playback_session') }}
where completion_rate < 0.0
   or completion_rate > 1.0
