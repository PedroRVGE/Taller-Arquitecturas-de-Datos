-- Identificación de registros: en cada procesamiento registrado, cada ítem recibido queda
-- clasificado exactamente una vez como rechazado, repetido dentro de la corrida, nuevo,
-- actualizado o repetido de corridas anteriores.
SELECT COUNT(*) AS failures
FROM news_lake.pipeline_runs
WHERE received IS NOT NULL
  AND received <> rejected + duplicates_in_batch + new + updated + repeated;
