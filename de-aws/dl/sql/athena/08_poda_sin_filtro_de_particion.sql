-- Sin filtro de partición: el filtro sobre una columna normal obliga a leer todas las particiones.
SELECT COUNT(*) AS noticias, COUNT(DISTINCT section) AS secciones
FROM news_lake.news
WHERE date(published_at_utc) = DATE '2026-10-21';
