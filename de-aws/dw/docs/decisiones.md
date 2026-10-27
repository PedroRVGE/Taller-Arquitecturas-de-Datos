# Decisiones técnicas y diferencias frente a la versión local

| Tema | Local (DE-DW) | AWS (este repositorio) | Motivo |
| --- | --- | --- | --- |
| Infraestructura | `docker compose` con MySQL, MongoDB y DuckDB | Terraform: RDS, DocumentDB, Redshift, S3, EC2 | Reproducible y destruible; nada se crea a mano |
| Warehouse | Archivo DuckDB, un solo escritor | Redshift Serverless | Columnar MPP, concurrencia; se quitan los bloqueos de archivo y dbt usa 4 hilos |
| Aterrizaje | CSV temporales por corrida en disco | `s3://.../raw/<fuente>/<entidad>/batch_id=<id>/` inmutable | Reprocesar sin consultar las fuentes |
| Raw | Se reemplazaba completo en cada corrida | Acumula lotes; `stg_*` reconstruye el estado vigente | Extracción incremental y trazabilidad por lote |
| Carga | `INSERT ... FROM read_csv` | `COPY ... IAM_ROLE` en una transacción | Carga paralela; sin llaves de acceso |
| Orquestación | 3 DAGs (maestro + 2 hijos) | 1 DAG `dw_pipeline` con grupos de tareas + `dw_source_ops` | El lote viaja por XCom entre todas las tareas |
| Credenciales | `.env` local | Secrets Manager + rol IAM de la EC2 | Sin secretos en el repositorio ni en `user_data` |

## Formato de los archivos: CSV comprimido con gzip

- `COPY ... FORMAT AS CSV GZIP` es nativo y lee en paralelo.
- Es el formato de la versión local y se inspecciona con cualquier herramienta.
- Los tipos se fijan en las tablas `raw` y en `stg_*`. Con Parquet, `COPY` exige que el esquema del archivo coincida exactamente con la tabla, lo que vuelve frágil un cambio de tipos.
- Los documentos de DocumentDB viajan completos en `document_json` (hasta 65 535 bytes), igual que la versión local guardaba el JSON.
- Opciones: `IGNOREHEADER 1` salta el encabezado, `EMPTYASNULL` vuelve NULL los textos vacíos y `TIMEFORMAT 'auto'` interpreta las fechas. Las fechas de DocumentDB se escriben en UTC sin zona y van a columnas `timestamptz`, que Redshift interpreta como UTC.

## Extracción incremental por watermark

| Entidad | Columna de avance | Captura |
| --- | --- | --- |
| customers, products | `updated_at` | Altas y cambios (necesario para SCD tipo 2) |
| orders | `order_id` | Pedidos nuevos (los pedidos no se modifican) |
| order_items | `order_id` | Líneas de los pedidos nuevos |
| reviews | `review_id` | Reseñas nuevas |
| navigation_events | `event_id` | Eventos nuevos |

El primer lote es `full`. Los siguientes son `incremental` y leen la última marca confirmada de `ops.batch_entities`. Limitaciones documentadas:

- Un borrado en la fuente no aparece en un lote incremental; se refleja en el siguiente lote `full` (el `post_hook` `delete_orphaned_facts` lo propaga a los hechos).
- Un cambio con `updated_at` anterior a la marca no se detecta. `simulate_changes` escribe la fecha que se le indica; use una posterior a la marca de `customers`/`products` (con los datos por defecto, 2025-01-01). El valor por defecto de DE-DW, 2025-07-01, funciona.
- Los documentos de DocumentDB no tienen fecha de modificación: un documento editado conserva su identificador y no se vuelve a extraer en modo incremental.
- Los lotes de `append_batch` deben agregarse en orden creciente. En DE-DW cualquier número libre servía porque raw se recargaba completo; aquí los ids del lote N (`1 000 000 + N × 10`) tienen que superar la marca. `append_batch` rechaza un número menor al último agregado y sigue permitiendo repetir uno existente.
- Regenerar las fuentes con `generate` equivale a `run_pipeline.sh` sin `--existing-sources`: borra los lotes agregados. `dw_source_ops` deja la marca `control/source_generation.json` en S3 y el siguiente `dw_pipeline` en modo `auto` se ejecuta como `full`, así `stg_*` deja de ver filas que ya no existen en la fuente.

## Estado vigente en `stg_*`

`raw` guarda cada extracción. La macro `latest_batch_rows` toma las filas desde la última extracción `full` de la entidad y se queda con la más reciente por llave (`row_number()` sobre `_extracted_at`). Cada fila conserva su `_batch_id`, que llega hasta `quarantine` como lote de origen del rechazo.

## Adaptaciones de SQL a Redshift

| DuckDB | Redshift | Dónde |
| --- | --- | --- |
| `select * exclude (rejection_reason)` | `dbt_utils.star(from=..., except=[...])` | `int_*__valid` |
| `a is distinct from b` | macro `is_distinct_from` | `incremental.sql`, pruebas |
| `timezone('America/Bogota', ts)` | `ts at time zone 'America/Bogota'` (macro `to_business_time`); `convert_timezone` no acepta `timestamptz` | hechos de MongoDB, pruebas |
| `not in (...) or exists (...)` correlacionado | `left join` contra el hecho ya cargado: Redshift no admite subconsultas correlacionadas dentro de un `OR` | `incremental.sql` |
| `extract(isodow from d)` | `dow` con domingo = 7 (macro `iso_day_of_week`) | `dim_date` |
| `json_extract_string(doc, '$.a.b')` | `json_extract_path_text(doc, 'a', 'b', true)` | `stg_mongodb__reviews` |
| `max(date) - 7` | `dateadd(day, -7, max(date))` | `incremental.sql` |
| `datediff('day', a, b)` | `datediff(day, a, b)` | prueba de `dim_date` |
| alias `date` | alias `calendar` | hechos |
| esquema `raw` | `"raw"` entre comillas | Python y fuentes de dbt |
| `threads: 1` y bloqueo de archivo | `threads: 4` | `profiles.yml` |

## Ajustes por DocumentDB

| Diferencia | Ajuste |
| --- | --- |
| TLS obligatorio | `tls=true&tlsCAFile=/opt/certs/global-bundle.pem`; la imagen descarga el bundle de AWS |
| No soporta *retryable writes* | `retryWrites=false` en el URI |
| Es un replica set | `replicaSet=rs0&readPreference=secondaryPreferred` |
| Soporte parcial de `$jsonSchema` y `bypassDocumentValidation` | `generate_data.py` intenta el validador; si DocumentDB lo rechaza, crea la colección sin él. Para insertar defectos intenta `bypass_document_validation` y, si no está disponible, desactiva la validación durante la inserción |
| Sin acceso desde internet | Las operaciones sobre la fuente corren en la EC2 mediante el DAG `dw_source_ops` |
| `tz_aware` | Las fechas se leen con zona (UTC) y se escriben en UTC |

## Qué se conservó

Las reglas de calidad de `int_*__qa`, el orden de prioridad de los motivos, el modelo de constelación (3 hechos y 6 dimensiones), las llaves subrogadas, los snapshots SCD tipo 2 con estrategia `timestamp`, la ventana incremental de 7 días, `delete+insert` y las pruebas genéricas y singulares.
