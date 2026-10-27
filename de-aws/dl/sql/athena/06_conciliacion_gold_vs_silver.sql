-- Conciliación: noticias por día y sección en Gold contra el conteo directo en Silver.
-- Debe devolver cero filas.
WITH silver AS (
    SELECT published_date, media, section, COUNT(DISTINCT news_id) AS news_count
    FROM news_lake.news
    WHERE published_date <> 'unknown'
    GROUP BY published_date, media, section
)
SELECT
    COALESCE(s.published_date, g.published_date) AS published_date,
    COALESCE(s.section, g.section) AS section,
    s.news_count AS silver,
    g.news_count AS gold
FROM silver AS s
FULL OUTER JOIN news_lake.gold_news_daily AS g
    ON g.published_date = s.published_date AND g.media = s.media AND g.section = s.section
WHERE s.news_count IS NULL OR g.news_count IS NULL OR s.news_count <> g.news_count;
