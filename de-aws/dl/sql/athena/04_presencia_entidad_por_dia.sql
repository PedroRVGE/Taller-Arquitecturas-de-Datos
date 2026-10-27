-- Presencia de una entidad entre días: noticias distintas que la mencionan y variación diaria.
-- Se filtra también el tipo: gold_entity_daily cuenta por (fecha, tipo, entidad), y "Colombia"
-- puede detectarse como LOCATION y como ORGANIZATION en la misma noticia; sumar ambos la contaría dos veces.
WITH daily AS (
    SELECT published_date, news_count AS noticias
    FROM news_lake.gold_entity_daily
    WHERE entity = 'colombia'          -- entidad normalizada (minúsculas)
      AND entity_type = 'LOCATION'
)
SELECT
    published_date,
    noticias,
    noticias - LAG(noticias) OVER (ORDER BY published_date) AS variacion_vs_dia_anterior
FROM daily
ORDER BY published_date;
