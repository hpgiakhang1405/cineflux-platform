SELECT format('SET search_path TO %I', :'serving_schema') \gexec

CREATE INDEX IF NOT EXISTS idx_mart_content_trending_score
    ON mart_content_trending (
        popularity_score DESC,
        window_start DESC,
        content_id ASC
    );

ANALYZE mart_content_trending;
