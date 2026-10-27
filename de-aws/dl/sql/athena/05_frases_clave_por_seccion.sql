-- Frases clave más frecuentes por sección (top 5 por sección).
WITH ranked AS (
    SELECT
        section,
        key_phrase,
        SUM(news_count) AS noticias,
        ROW_NUMBER() OVER (PARTITION BY section ORDER BY SUM(news_count) DESC, key_phrase) AS puesto
    FROM news_lake.gold_key_phrases_daily
    GROUP BY section, key_phrase
)
SELECT section, puesto, key_phrase, noticias
FROM ranked
WHERE puesto <= 5
ORDER BY section, puesto;
