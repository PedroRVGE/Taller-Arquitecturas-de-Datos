-- Campos obligatorios presentes.
SELECT COUNT(*) AS failures
FROM news_lake.news
WHERE news_id IS NULL OR title IS NULL OR link IS NULL OR content_hash IS NULL OR source_id IS NULL;
