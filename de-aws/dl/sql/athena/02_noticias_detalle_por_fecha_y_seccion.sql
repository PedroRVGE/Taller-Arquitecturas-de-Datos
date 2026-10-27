-- Noticias vigentes (Silver) de una fecha y una sección, con su estado de enriquecimiento.
SELECT published_date, section, title, link, published_at_utc, enrichment_status, version
FROM news_lake.news
WHERE published_date = '2026-10-21'
  AND section = 'Economía'
ORDER BY published_at_utc DESC;
