-- Noticias por día, medio y sección (Gold), filtrando por un rango de fechas.
-- El filtro sobre published_date (columna de partición) limita los archivos que Athena lee.
SELECT published_date, media, section, news_count
FROM news_lake.gold_news_daily
WHERE published_date BETWEEN '2026-10-01' AND '2026-10-31'
ORDER BY published_date DESC, news_count DESC;
