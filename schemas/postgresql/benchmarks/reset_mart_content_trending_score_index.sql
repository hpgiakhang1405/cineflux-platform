SELECT format('SET search_path TO %I', :'serving_schema') \gexec

DROP INDEX IF EXISTS idx_mart_content_trending_score;
