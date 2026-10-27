-- Cada noticia vigente tiene un estado de enriquecimiento conocido.
SELECT COUNT(*) AS failures
FROM news_lake.news
WHERE enrichment_status IS NULL
   OR enrichment_status NOT IN ('analyzed', 'truncated', 'empty_text', 'unsupported_language', 'error');
