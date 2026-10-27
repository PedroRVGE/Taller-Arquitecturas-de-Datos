-- Ítems rechazados con su motivo y su archivo de Bronze (trazabilidad).
SELECT ingestion_date, run_id, source_id, rejection_reason, item_index, title, link, bronze_key
FROM news_lake.news_quarantine
ORDER BY ingestion_date DESC, run_id DESC;
