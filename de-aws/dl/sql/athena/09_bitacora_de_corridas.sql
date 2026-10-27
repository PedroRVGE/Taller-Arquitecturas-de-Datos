-- Estado y conteos de cada procesamiento: recibidos vs. noticias únicas.
-- received incluye repetidos dentro de la corrida (la misma noticia en dos feeds), ya
-- conocidos de corridas anteriores y rechazados; por eso supera a new + updated.
-- Una corrida repetida o recuperada aparece en varias filas; mode distingue la extracción
-- desde los RSS (new) del reproceso desde Bronze (reprocess) y de la reconstrucción (rebuild).
SELECT run_id, mode, status, finished_at, received, rejected, duplicates_in_batch, new, updated, repeated,
       unique_news_current, news_without_valid_date, comprehend_calls, comprehend_cached, comprehend_errors,
       sources_failed, sources_reused, bytes_scanned_with_filter, bytes_scanned_without_filter
FROM news_lake.pipeline_runs
ORDER BY finished_at DESC;
