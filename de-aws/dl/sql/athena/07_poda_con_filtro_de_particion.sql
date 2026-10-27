-- Con filtro de partición: Athena solo lee la carpeta published_date=2026-10-21.
-- Compare "Data scanned" con la consulta 08.
SELECT COUNT(*) AS noticias, COUNT(DISTINCT section) AS secciones
FROM news_lake.news
WHERE published_date = '2026-10-21';
