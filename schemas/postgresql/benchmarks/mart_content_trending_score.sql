EXPLAIN (ANALYZE, BUFFERS)
SELECT
    window_start,
    content_id,
    popularity_score,
    popularity_rank
FROM serving.mart_content_trending
ORDER BY
    popularity_score DESC,
    window_start DESC,
    content_id ASC
LIMIT 100;

WITH top_trends AS (
    SELECT
        window_start,
        content_id,
        popularity_score,
        popularity_rank
    FROM serving.mart_content_trending
    ORDER BY
        popularity_score DESC,
        window_start DESC,
        content_id ASC
    LIMIT 100
)
SELECT
    count(*) AS result_rows,
    md5(
        string_agg(
            concat_ws(
                '|',
                window_start::text,
                content_id,
                popularity_score::text,
                popularity_rank::text
            ),
            '||'
            ORDER BY popularity_score DESC, window_start DESC, content_id ASC
        )
    ) AS result_checksum
FROM top_trends;

SELECT
    pg_size_pretty(
        pg_relation_size('serving.mart_content_trending')
    ) AS table_size,
    pg_size_pretty(
        pg_indexes_size('serving.mart_content_trending')
    ) AS all_indexes_size,
    coalesce(
        pg_size_pretty(
            pg_relation_size(
                to_regclass('serving.idx_mart_content_trending_score')
            )
        ),
        'not created'
    ) AS benchmark_index_size;
