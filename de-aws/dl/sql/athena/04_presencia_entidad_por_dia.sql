-- Presencia de una entidad entre días: noticias distintas que la mencionan y variación diaria.
WITH daily AS (
    SELECT published_date, SUM(news_count) AS noticias
    FROM news_lake.gold_entity_daily
    WHERE entity = 'colombia'          -- entidad normalizada (minúsculas)
    GROUP BY published_date
)
SELECT
    published_date,
    noticias,
    noticias - LAG(noticias) OVER (ORDER BY published_date) AS variacion_vs_dia_anterior
FROM daily
ORDER BY published_date;
