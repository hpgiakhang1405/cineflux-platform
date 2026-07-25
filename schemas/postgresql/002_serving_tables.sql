SELECT format('SET search_path TO %I', :'serving_schema') \gexec

CREATE TABLE IF NOT EXISTS mart_content_trending (
    window_start timestamp(6) with time zone NOT NULL,
    window_end timestamp(6) with time zone,
    content_id varchar NOT NULL,
    playback_count bigint,
    unique_viewer_count bigint,
    completed_session_count bigint,
    total_watch_seconds bigint,
    popularity_score double precision,
    popularity_rank bigint,
    created_timestamp timestamp(6) with time zone,
    PRIMARY KEY (window_start, content_id)
);

ALTER TABLE mart_content_trending
    ALTER COLUMN window_start SET NOT NULL,
    ALTER COLUMN content_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'mart_content_trending'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE mart_content_trending
            ADD PRIMARY KEY (window_start, content_id);
    END IF;
END
$$;
