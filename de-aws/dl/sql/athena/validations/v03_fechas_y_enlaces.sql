-- Validez de fechas y enlaces: la partición coincide con la fecha UTC y el enlace es http(s).
SELECT COUNT(*) AS failures
FROM news_lake.news
WHERE (published_date_valid AND (published_at_utc IS NULL
                                 OR published_date <> CAST(CAST(published_at_utc AS DATE) AS VARCHAR)))
   OR (NOT published_date_valid AND published_date <> 'unknown')
   OR NOT regexp_like(canonical_link, '^https?://[^/]+');
