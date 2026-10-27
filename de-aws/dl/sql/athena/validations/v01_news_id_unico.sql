-- Unicidad del identificador de noticia en el conjunto vigente.
SELECT COUNT(*) AS failures
FROM (
    SELECT news_id FROM news_lake.news GROUP BY news_id HAVING COUNT(*) > 1
);
