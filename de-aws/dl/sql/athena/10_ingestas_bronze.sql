-- Cada consulta a un feed: estado HTTP, hash del contenido y archivo original en S3.
SELECT run_id, source_id, status, http_status, item_count, bytes, content_sha256, bronze_key, origin, error
FROM news_lake.bronze_ingestions
ORDER BY ingested_at_utc DESC, source_id;
