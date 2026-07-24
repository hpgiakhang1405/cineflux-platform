SELECT format('SET search_path TO %I', :'streaming_schema') \gexec

CREATE TABLE IF NOT EXISTS playback_metrics_5m (
    window_start timestamp(3) NOT NULL,
    window_end timestamp(3) NOT NULL,
    user_id text NOT NULL,
    event_count bigint NOT NULL,
    unique_session_count bigint NOT NULL,
    unique_content_count bigint NOT NULL,
    playback_started_count bigint NOT NULL,
    playback_completed_count bigint NOT NULL,
    completed_watch_seconds bigint NOT NULL,
    created_timestamp timestamp(3) NOT NULL,
    PRIMARY KEY (window_start, user_id)
);

CREATE TABLE IF NOT EXISTS content_popularity_5m (
    window_start timestamp(3) NOT NULL,
    window_end timestamp(3) NOT NULL,
    content_id text NOT NULL,
    event_count bigint NOT NULL,
    unique_user_count bigint NOT NULL,
    unique_session_count bigint NOT NULL,
    playback_started_count bigint NOT NULL,
    playback_completed_count bigint NOT NULL,
    completed_watch_seconds bigint NOT NULL,
    created_timestamp timestamp(3) NOT NULL,
    PRIMARY KEY (window_start, content_id)
);
