-- Referencia de estudio: DDL de Athena equivalente a lo que Terraform crea en glue.tf.
-- No hace falta ejecutarlo; sirve para comparar Terraform con SQL.
CREATE EXTERNAL TABLE IF NOT EXISTS news_lake.gold_news_daily (
    media      string COMMENT 'Medio',
    section    string COMMENT 'Sección principal',
    news_count bigint COMMENT 'Noticias distintas publicadas'
)
PARTITIONED BY (published_date string)
STORED AS PARQUET
LOCATION 's3://<bucket>/gold/news_daily/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Tres formas de registrar una partición nueva:
-- 1) Explícita (lo que hace el DAG con la API de Glue):
ALTER TABLE news_lake.gold_news_daily ADD IF NOT EXISTS
    PARTITION (published_date = '2026-10-21')
    LOCATION 's3://<bucket>/gold/news_daily/published_date=2026-10-21/version=r20261021T150000Z/';
-- 2) Descubrimiento (solo si la carpeta sigue el patrón clave=valor, sin carpetas de versión):
MSCK REPAIR TABLE news_lake.gold_news_daily;
-- 3) Partition projection: Athena calcula las particiones a partir de TBLPROPERTIES
--    ('projection.enabled'='true', ...), sin registrarlas en Glue.
