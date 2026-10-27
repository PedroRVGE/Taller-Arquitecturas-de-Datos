-- Las diez organizaciones presentes en más noticias distintas.
-- gold_entity_daily ya cuenta noticias distintas por día; cada noticia tiene una sola
-- fecha de publicación, así que sumar los días no cuenta dos veces la misma noticia.
SELECT entity AS organizacion, SUM(news_count) AS noticias
FROM news_lake.gold_entity_daily
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity
ORDER BY noticias DESC, organizacion
LIMIT 10;
