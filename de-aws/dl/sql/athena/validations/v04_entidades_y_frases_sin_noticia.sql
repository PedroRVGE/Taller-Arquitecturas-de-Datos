-- Ninguna entidad ni frase clave sin una noticia vigente asociada.
SELECT
    (SELECT COUNT(*) FROM news_lake.news_entities e
      LEFT JOIN news_lake.news n ON n.news_id = e.news_id AND n.published_date = e.published_date
      WHERE n.news_id IS NULL)
  + (SELECT COUNT(*) FROM news_lake.news_key_phrases k
      LEFT JOIN news_lake.news n ON n.news_id = k.news_id AND n.published_date = k.published_date
      WHERE n.news_id IS NULL) AS failures;
