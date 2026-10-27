-- Conciliación de indicadores: Gold contra Silver por día y sección, y presencia de entidades.
WITH silver_news AS (
    SELECT published_date, media, section, COUNT(DISTINCT news_id) AS news_count
    FROM news_lake.news WHERE published_date <> 'unknown'
    GROUP BY 1, 2, 3
),
news_diff AS (
    SELECT COUNT(*) AS n
    FROM silver_news s
    FULL OUTER JOIN news_lake.gold_news_daily g
      ON g.published_date = s.published_date AND g.media = s.media AND g.section = s.section
    WHERE s.news_count IS NULL OR g.news_count IS NULL OR s.news_count <> g.news_count
),
silver_entities AS (
    SELECT published_date, entity_type, entity_text_normalized AS entity, COUNT(DISTINCT news_id) AS news_count
    FROM news_lake.news_entities WHERE published_date <> 'unknown'
    GROUP BY 1, 2, 3
),
entity_diff AS (
    SELECT COUNT(*) AS n
    FROM silver_entities s
    FULL OUTER JOIN news_lake.gold_entity_daily g
      ON g.published_date = s.published_date AND g.entity_type = s.entity_type AND g.entity = s.entity
    WHERE s.news_count IS NULL OR g.news_count IS NULL OR s.news_count <> g.news_count
)
SELECT (SELECT n FROM news_diff) + (SELECT n FROM entity_diff) AS failures;
