-- Estado y conteos por corrida: recibidos vs. noticias únicas.
-- received incluye repetidos dentro de la corrida (la misma noticia en dos feeds), ya
-- conocidos de corridas anteriores y rechazados; por eso supera a new + updated.
SELECT run_id, mode, status, received, rejected, duplicates_in_batch, new, updated, repeated,
       unique_news_current, comprehend_calls, comprehend_cached, sources_failed,
       bytes_scanned_with_filter, bytes_scanned_without_filter
FROM news_lake.pipeline_runs
ORDER BY started_at DESC;
