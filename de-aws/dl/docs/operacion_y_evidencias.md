# Incrementalidad, recuperación y evidencias

## Cómo se escriben y publican los archivos

Athena lee una partición en la ubicación que tiene registrada en Glue, no todo lo que haya bajo el prefijo. El pipeline lo aprovecha:

1. `publish_silver` y `build_gold` calculan el contenido completo de cada partición y su hash. Si coincide con el `content_sha256` guardado en los parámetros de la partición, no escriben nada.
2. Si cambió, escriben `.../published_date=<fecha>/version=<run_id>/part-00000.parquet`, una carpeta que Athena todavía no ve.
3. `update_glue_catalog` cambia en lote la ubicación de las particiones (`BatchCreatePartition`, `BatchUpdatePartition`, `BatchDeletePartition`). Ese es el momento de publicación.
4. Después borra las versiones viejas, conservando la inmediatamente anterior por si una consulta en curso la estaba leyendo.

Consecuencias:

- Un fallo antes del paso 3 deja a Athena viendo la versión anterior completa: nunca resultados parciales.
- Repetir una corrida escribe el mismo contenido en la misma carpeta y deja el mismo puntero: nunca duplicados.
- Una partición sin cambios no se toca: una corrida sin noticias nuevas publica 0 particiones.
- Antes de publicar, `update_glue_catalog` comprueba que cada partición sigue apuntando a donde apuntaba cuando se calculó el plan. Si otra corrida publicó entre medias (por ejemplo, la programada de los 30 minutos mientras una corrida fallida esperaba su *Clear*), el plan se rechaza completo en lugar de pisar esas noticias, y el error indica recuperar con `{"reprocess_run_id": "<id>"}`, que recalcula desde Bronze sobre el estado actual.

## Metadatos de particiones nuevas

Las tablas se definen en Terraform (sin crawlers). Las particiones se registran con la API de Glue en `update_glue_catalog`. Alternativas de estudio en `sql/athena/reference/ddl_equivalente.sql`: `ALTER TABLE ADD PARTITION`, `MSCK REPAIR TABLE` (descubre carpetas `clave=valor`, pero no entiende las carpetas `version=`) y *partition projection*. Un crawler solo infiere esquemas y particiones: no reemplaza las validaciones ni las transformaciones.

## Parámetros del DAG

| Parámetro | Efecto |
| --- | --- |
| `run_id` | Identificador de la corrida; vacío = `r` + fecha lógica en UTC |
| `reprocess_run_id` | Reprocesa esa corrida solo desde Bronze (sin consultar RSS) |
| `rebuild_from_bronze` | Reconstruye Silver y Gold desde todas las corridas de Bronze, en orden cronológico, sin RSS ni Comprehend (usa la caché) |
| `simulate_change_from_run` | Copia controlada: toma el Bronze de esa corrida y modifica el título de la primera noticia de cada feed |
| `unavailable_source` | Simula la caída de una fuente (URL inexistente, HTTP 404) |
| `simulate_failure` | Falla una vez entre la escritura de archivos y la publicación en Glue |

## Escenarios y evidencia

Las consultas están en `sql/athena/` y también como *saved queries* del workgroup.

| Escenario | Cómo se ejecuta | Evidencia |
| --- | --- | --- |
| Carga inicial | Primera corrida programada o manual | `09_bitacora_de_corridas.sql`: `status=complete`, `new = unique_news_current`; `01` a `05` devuelven datos |
| Registros defectuosos | Ítems sin título o enlace en el RSS (o un fixture editado) | `11_cuarentena.sql` con `rejection_reason` y `bronze_key` |
| Repetición de un lote exitoso | Trigger con `{"run_id": "<id ya exitoso>"}` (o `{"reprocess_run_id": "<id>"}`) | El log de `ingest_bronze` dice "se reprocesa sin consultar el RSS"; en `pipeline_runs` aparece una fila nueva con `mode = reprocess`, `sources_reused` con las 3 fuentes, `new = 0`, `partitions_published = 0` y `comprehend_calls = 0`, y la fila de la primera ejecución conserva sus conteos; validación `v01` sin duplicados |
| Nuevo lote incremental | La siguiente corrida programada | `new` solo con noticias nuevas; `comprehend_cached` crece y `comprehend_calls = new` |
| Noticia modificada | `{"simulate_change_from_run": "<run_id anterior>"}` | `updated = 3`, `comprehend_calls = 3`; en `news`, `version = 2` y el título termina en "(actualizada)". La siguiente corrida real vuelve a traer el título original y lo registra como otro cambio (`version = 3`): tome las capturas antes o pause el DAG durante la demostración |
| Fallo controlado | `{"simulate_failure": true}` y luego *Clear* de `failure_injection` con *Downstream* | Antes del Clear, Athena muestra la versión anterior; después, la nueva, sin duplicados. Si entre el fallo y el *Clear* corrió otra corrida, `update_glue_catalog` rechaza el plan viejo; se recupera con `{"reprocess_run_id": "<id>"}` |
| Reproceso desde Bronze | `{"rebuild_from_bronze": true}` | Mismos `news_id`, `content_hash` y `version`; `comprehend_calls = 0`; sin solicitudes HTTP |
| Nueva partición | Una noticia con fecha nueva | `SHOW PARTITIONS news_lake.news` antes y después de `update_glue_catalog` |
| Fuente caída | `{"unavailable_source": "eltiempo_deportes"}` | `status = partial`, `sources_failed = [eltiempo_deportes]`; las noticias de deportes siguen en Silver |

Comparación de bytes escaneados: `07_poda_con_filtro_de_particion.sql` contra `08_poda_sin_filtro_de_particion.sql`. La tarea `athena_validations` guarda ambos valores en `pipeline_runs` (`bytes_scanned_with_filter`, `bytes_scanned_without_filter`).

## Validaciones automáticas (athena_validations)

| Archivo | Comprueba |
| --- | --- |
| `v01_news_id_unico.sql` | Unicidad de `news_id` en el conjunto vigente |
| `v02_campos_obligatorios.sql` | `news_id`, `title`, `link`, `content_hash`, `source_id` presentes |
| `v03_fechas_y_enlaces.sql` | La partición coincide con la fecha UTC; enlaces `http(s)` |
| `v04_entidades_y_frases_sin_noticia.sql` | Ninguna entidad ni frase sin noticia vigente |
| `v05_conciliacion_silver_gold.sql` | Noticias por día y sección y presencia de entidades: Gold = Silver |
| `v06_estados_de_enriquecimiento.sql` | Todo `enrichment_status` es un valor conocido |
| `v07_conteos_por_corrida.sql` | En cada procesamiento, `received = rejected + duplicates_in_batch + new + updated + repeated` |

Cada archivo devuelve una columna `failures`; cualquier valor mayor que cero hace fallar la tarea.

## Pruebas locales

```bash
cd dl/tests
pip install pytest "moto[s3,glue]" pyarrow defusedxml requests pyyaml boto3
python -m pytest -q
```

Recorren el DAG completo con S3 y Glue simulados (moto), Comprehend y HTTP falsos, y verifican los escenarios: carga inicial, repetición sin cambios (registrada como `reprocess` sin borrar la bitácora anterior), noticia modificada con solo 3 llamadas nuevas, fuente caída, fallo controlado sin cambios visibles, rechazo de un plan desactualizado tras otra corrida, validación fallida que detiene el pipeline y reconstrucción idéntica desde Bronze.
