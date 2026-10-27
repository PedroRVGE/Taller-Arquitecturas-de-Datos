-- Las diez personas y los diez lugares presentes en más noticias distintas (pregunta 2 de Gold,
-- junto con 03_top10_organizaciones.sql). Igual que en 03, sumar los días no cuenta dos veces
-- una noticia: cada noticia tiene una sola fecha de publicación.
WITH ranked AS (
    SELECT entity_type, entity, SUM(news_count) AS noticias,
           ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY SUM(news_count) DESC, entity) AS puesto
    FROM news_lake.gold_entity_daily
    WHERE entity_type IN ('PERSON', 'LOCATION')
    GROUP BY entity_type, entity
)
SELECT entity_type AS tipo, puesto, entity AS entidad, noticias
FROM ranked
WHERE puesto <= 10
ORDER BY tipo, puesto;
