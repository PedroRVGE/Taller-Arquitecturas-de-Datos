# Arquitectura del data lake de noticias

Un solo bucket S3 contiene las zonas Bronze, Silver y Gold. Airflow en una EC2 consulta tres feeds RSS de El Tiempo, guarda el XML original en Bronze, limpia y deduplica en Silver, enriquece con Comprehend, agrega en Gold, publica las particiones en el Glue Data Catalog y valida con Athena.

```mermaid
flowchart TD
    RSS["3 feeds RSS de El Tiempo<br/>economía, política, deportes"]
    subgraph EC2["EC2 t3.medium · Docker Compose · Airflow (LocalExecutor)"]
        DAG["newslake_pipeline"]
    end
    subgraph S3["S3: un solo bucket"]
        B[("bronze/<br/>XML original + metadatos")]
        S[("silver/<br/>news, news_entities, news_key_phrases,<br/>comprehend_results, news_quarantine")]
        G[("gold/<br/>news_daily, entity_daily, key_phrases_daily")]
        C[("control/<br/>bronze_log, run_log, staging")]
    end
    CMP["Amazon Comprehend<br/>DetectEntities, DetectKeyPhrases"]
    GLUE["Glue Data Catalog<br/>base news_lake, 10 tablas, particiones"]
    ATH["Athena<br/>workgroup con tope de bytes"]

    RSS -- "HTTPS" --> DAG
    DAG -- "ingesta" --> B
    DAG -- "limpieza, validación, deduplicación" --> S
    DAG -- "texto nuevo o cambiado" --> CMP
    CMP -- "entidades y frases" --> DAG
    DAG -- "agregaciones" --> G
    DAG -- "bitácoras" --> C
    DAG -- "BatchCreate/UpdatePartition" --> GLUE
    GLUE -- "metadatos" --> ATH
    S --> ATH
    G --> ATH
    C --> ATH
```

## Zonas del bucket

| Prefijo | Formato | Contenido | Inmutable |
| --- | --- | --- | --- |
| `bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/` | XML + JSON | Respuesta original válida y `_metadata.json` (corrida, URL, UTC, HTTP, SHA-256, ubicación) | Sí (escritura condicional) |
| `silver/<tabla>/published_date=<fecha>/version=<run_id>/` | Parquet + Snappy | Noticias vigentes, entidades, frases clave | Cada versión es una carpeta nueva |
| `silver/news_quarantine/ingestion_date=<fecha>/run_id=<id>/` | Parquet | Rechazos con motivo y archivo de Bronze | Una partición por corrida |
| `silver/comprehend_results/analyzed_date=<fecha>/run_id=<id>/` | Parquet | Caché de análisis por hash de texto | Una partición por corrida |
| `gold/<tabla>/published_date=<fecha>/version=<run_id>/` | Parquet | Indicadores agregados | Cada versión es una carpeta nueva |
| `control/bronze_log/` | JSON (una línea) | Bitácora de ingesta por fuente y corrida, consultable en Athena | Se reescribe por corrida |
| `control/run_log/<run_id>__<fecha>.json` | JSON (una línea) | Bitácora de cada procesamiento, consultable en Athena | Un archivo por procesamiento: repetir no borra el anterior |
| `control/staging/run_id=<id>/` | JSON | Archivos de trabajo entre tareas | Expiran a los 3 días |
| `athena-results/` | CSV | Resultados de Athena | Expiran a los 7 días |
| `artifacts/` | — | Código y configuración publicados por Terraform | — |

## Tareas del DAG

```mermaid
flowchart LR
    V[validate_access] --> I1[ingest_bronze.economia]
    V --> I2[ingest_bronze.politica]
    V --> I3[ingest_bronze.deportes]
    I1 --> T[transform_silver]
    I2 --> T
    I3 --> T
    T --> E[enrich_comprehend] --> P[publish_silver] --> G[build_gold] --> F[failure_injection] --> U[update_glue_catalog] --> A[athena_validations] --> R[record_run_status]
```

| # | Requisito | Tarea |
| --- | --- | --- |
| 1 | Validación de configuración y acceso | `validate_access`: feeds.yml, S3, 10 tablas de Glue, workgroup de Athena, Comprehend; crea el pool `newslake_writer` |
| 2 | Extracción y escritura en Bronze | `ingest_bronze.<fuente>`: una tarea por feed, 3 reintentos solo para errores transitorios |
| 3 | Limpieza, validación y deduplicación | `transform_silver` (corre aunque falle una fuente: `trigger_rule=all_done`) |
| 4 | Enriquecimiento | `enrich_comprehend` con caché por hash |
| 5 | Publicación de Silver | `publish_silver`: escribe solo las particiones que cambiaron |
| 6 | Construcción de Gold | `build_gold` |
| 7 | Glue Data Catalog | `update_glue_catalog`: cambia los punteros de partición |
| 8 | Validaciones con Athena | `athena_validations`: `sql/athena/validations/*.sql` y comparación de bytes escaneados |
| 9 | Estado del procesamiento | `record_run_status`: `complete`, `partial` o `failed` en `control/run_log/` |

Control de concurrencia: `max_active_runs=1` y el pool `newslake_writer` de un cupo para las tareas que escriben en el lago.
