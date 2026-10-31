# Estado de la entrega

Relación entre los requisitos del enunciado (`Actividad Arquitecturas de Datos.pdf`), la implementación en `de-aws/` y las evidencias en `de-aws/dw/docs/evidencias/`.

| Parte | Implementación | Evidencias |
| --- | --- | --- |
| 1. Data warehouse (`de-aws/dw`) | Completa | Escenarios de la sección 3.5 ejecutados en AWS el 2026-10-26; fallo controlado, dashboard de QuickSight y eliminación el 2026-10-30. Pendiente: correr en AWS las pruebas singulares de dbt (ver pendiente 1) |
| 2. Data lake (`de-aws/dl`) | Completa, con pruebas locales | Escenarios de la sección 4.5 ejecutados en AWS el 2026-10-30 y eliminación verificada. Sin partición de fecha nueva (ver limitaciones) |

---

## Parte 1. Data warehouse

### Ejecución en AWS

La infraestructura se creó con Terraform en `us-east-1` (131 recursos) y se eliminó con `terraform destroy`. La eliminación se verificó con la AWS CLI: no quedaron instancias EC2, bases RDS o DocumentDB, snapshots, Redshift Serverless, bucket, direcciones IP elásticas, VPC, secretos ni roles IAM del proyecto.

| Lote | Escenario | Resultado |
| --- | --- | --- |
| `b20261025T110000Z` | Carga inicial (`full`) | `success`. La primera ejecución de `dbt.run_marts` reveló dos diferencias de Redshift frente a DuckDB (`convert_timezone` no acepta `timestamptz` y no se admiten subconsultas correlacionadas dentro de un `OR`). Se adaptaron las macros `to_business_time` e `incremental_candidate_filter` y el lote se recuperó con *Clear* (capturas `ERROR-*`) |
| `b20261025T110000Z` | Repetición de un lote exitoso (`reprocess`) | `attempts = 2`, conteos sin cambios |
| `b20261027T032447Z` | Nuevo lote incremental (lote 2) | +3 filas en cada hecho |
| (paso 5) | Cambio en atributo historizado | Nueva versión SCD2 del cliente 1 y cierre de la anterior |
| `b20261027T033805Z` | Fallo controlado y reejecución | Recuperado con *Clear*. El lote no traía filas nuevas; se repitió el 2026-10-30 (fila siguiente) |
| (lote 3) | Lote nuevo hasta el indicador | Ventas totales 7 357 260.42 → 7 420 730.58 (256 → 259 líneas) |

Segunda ejecución (2026-10-30, 132 recursos con la fuente de datos de QuickSight), para los escenarios que quedaron pendientes. Los pasos 1 a 5 se repitieron con los mismos resultados (253 / 150 / 500 → 256 / 153 / 503 y SCD2 del cliente 1):

| Lote | Escenario | Resultado |
| --- | --- | --- |
| `b20261030T232516Z` | Fallo controlado con lote 3 (`append_batch` 3) | `failed` con las 3 filas nuevas ya en `raw`; recuperado con *Clear* de `failure_injection`: `success`, `attempts = 1`, 3 filas = 3 distintas en `raw`, `fct_sales` 256 → 259 = 259 llaves, conciliación `true` (capturas `60` a `64`) |
| — | Dashboard de QuickSight (Direct Query) | KPI 7 420 730.58 sin filtro y 541 641.20 con `category = Electrónica` (22 líneas), igual a la consulta de validación y con la misma fecha de actualización (capturas `70` a `72`) |
| `b20261031T021602Z` | Lote 4 hasta el dashboard | 7 420 730.58 → 7 484 200.74 (259 → 262 líneas) en el dashboard y en `ops.reconciliation` (capturas `80-*`) |

Las capturas `62`, `64-*`, `72` y `80-demo-sql-despues` muestran consultas ejecutadas en Redshift (workgroup del despliegue, usuario `dw_admin`) con su resultado, renderizadas como imagen en lugar de capturas del Query Editor v2.

Los conteos coinciden con los de la versión local (DE-DW):

| Medida | Obtenido | Esperado |
| --- | --- | --- |
| `raw` pedidos / reseñas (lote inicial) | 106 / 158 | 106 / 158 |
| `fct_sales` / `fct_reviews` / `fct_navigation_events` | 253 / 150 / 500 | 253 / 150 / 500 |
| `dbt test` | PASS=93, ERROR=0 | Sin errores |
| Conciliación (`ops.reconciliation`) | `passed = true` en todas | `true` |
| Cuarentena: clientes / productos / pedidos / líneas / reseñas / eventos | 4 / 2 / 4 / 6 / 8 / 8 | 4 / 2 / 4 / 6 / 8 / 8 |
| `stg = valid + quarantine` (líneas de pedido) | 259 = 253 + 6 | Igualdad |
| Repetición de un lote | Sin cambios | Sin cambios |
| Hechos después del lote 2 | 256 / 153 / 503 | 256 / 153 / 503 |

### Requisitos funcionales

| Requisito | Estado | Implementación | Evidencia |
| --- | --- | --- | --- |
| 3.1 MySQL en RDS y MongoDB en DocumentDB | Cumplido | `dw/terraform/rds.tf`, `docdb.tf` | `02-fuentes-rds-docdb-a/b` |
| 3.1 Scripts de generación y conexión adaptados, mismos datos que la versión local | Cumplido | `dw/extract/sources/`, `dw/extract/connections.py` | `10-source-ops-generate`; conteos iguales a DE-DW |
| 3.1 Ajustes por diferencias entre MongoDB y DocumentDB | Cumplido | `dw/docs/decisiones.md` | — |
| 3.2 Extracción inicial e incremental por lote | Cumplido | `dw/extract/extractors.py` | `15-*` (full), `42-batch-entities-incremental` |
| 3.2 Bucket privado y ruta con fuente, entidad y lote | Cumplido | `dw/terraform/s3.tf`, `dw/extract/s3_batches.py` | `01-s3-bucket-a/b`, `12-s3-lote` |
| 3.2 Fecha de extracción, lote y cantidad de registros | Cumplido | `_manifest.json`, `ops.batch_entities` | `13-s3-manifiesto`, `42-*` |
| 3.2 Reproceso sin consultar las fuentes y sin sobrescribir otros lotes | Cumplido | Manifiesto y escritura condicional en S3 | `31-reproceso-log` |
| 3.2 Justificación del formato (CSV comprimido) | Cumplido | `dw/docs/decisiones.md` | — |
| 3.3 Redshift con COPY desde S3 autorizado por un rol IAM | Cumplido | `dw/extract/redshift.py`, `dw/terraform/redshift.tf` | `03-redshift-workgroup-a/b`, `15-*` |
| 3.3 Capas raw, intermediate, quarantine y marts | Cumplido | `dw/dbt/models/` | `15-*`, `20-*`, `21-*` |
| 3.3 Modelo dimensional e historia SCD tipo 2 | Cumplido | `dw/dbt/models/marts/`, `dw/dbt/snapshots/` | `51-scd2-dim-customer` |
| 3.3 Tipos, funciones y configuración de dbt adaptados a Redshift | Cumplido | `dw/dbt/macros/`, `dw/docs/decisiones.md` | `ERROR-*` y su corrección |
| 3.3 Rechazos con motivo y lote de origen | Cumplido | `quarantine_rejections_log` | `20-cuarentena-motivos-a/b` |
| 3.4 DAG con las siete etapas, dependencias, reintentos acotados y logs por lote y tarea | Cumplido | `dw/airflow/dags/dw_pipeline.py` | `11-pipeline-carga-inicial-a/b`, `41-*` |
| 3.4 Airflow y dbt en la misma EC2; SQL ejecutado en Redshift | Cumplido | `dw/terraform/airflow.tf`, `dw/extract/dbt_runner.py` | `04-ec2-airflow`, `14-dbt-test-a/b` |
| 3.4 Sin intervención manual entre etapas | Cumplido | Ejecución programada diaria | `11-*` (corrida programada) |
| 3.5 Carga inicial | Cumplido | — | `10` a `16` |
| 3.5 Registros defectuosos | Cumplido | — | `20-*`, `21` |
| 3.5 Repetición de un lote exitoso | Cumplido | — | `30`, `31`, `32` |
| 3.5 Nuevo lote incremental | Cumplido | — | `40` a `43` |
| 3.5 Cambio en un atributo historizado | Cumplido | — | `50`, `51` |
| 3.5 Fallo controlado y reejecución | Cumplido | — | `60` a `64` |
| 3.5 Pruebas de unicidad, obligatorios, relaciones y reglas de negocio | Cumplido en los modelos; pruebas singulares sin ejecutar en AWS (pendiente 1) | `dw/dbt/models/**/*.yml`, `dw/dbt/tests/` | `14-dbt-test-a/b` |
| 3.5 Conciliación de un indicador agregado | Cumplido | Tarea `reconcile`, `ops.reconciliation` | `16-conciliacion` |
| 3.5 Identificación del lote y momento en que avanza su marca | Cumplido | `dw/docs/operacion_y_evidencias.md` | — |
| 3.6 Dashboard de QuickSight con gráfica, indicador y filtro sobre marts | Cumplido | Vista `marts.rpt_sales_dashboard`, `dw/quicksight/` (dataset y dashboard versionados), `dw/docs/quicksight.md` | `70`, `71` |
| 3.6 Consulta SQL que valida el indicador | Cumplido | `dw/docs/quicksight.md` | `72-sql-validacion-kpi` |
| 3.6 Actualización de SPICE | Documentado | `dw/docs/quicksight.md` | No aplica con Direct Query |

### Entregables

| Entregable | Estado | Ubicación |
| --- | --- | --- |
| DAGs y scripts de extracción y carga | Cumplido | `dw/airflow/dags/`, `dw/extract/` |
| Proyecto dbt adaptado a Redshift | Cumplido | `dw/dbt/` |
| Despliegue sin secretos y archivo de variables de ejemplo | Cumplido | `dw/terraform/` (Secrets Manager), `terraform.tfvars.example`, `.env.example` |
| Diagrama de arquitectura | Cumplido | `dw/docs/arquitectura.md` |
| Despliegue, ejecución y eliminación | Cumplido | `dw/docs/despliegue.md` |
| Decisiones técnicas y diferencias frente a la versión local | Cumplido | `dw/docs/decisiones.md` |
| Costos y controles de seguridad | Cumplido | `dw/docs/costos_y_seguridad.md` |
| Evidencias de ejecución, pruebas y escenarios | Cumplido | Capturas `00` a `90` |
| Dashboard de QuickSight y consulta del indicador | Cumplido | `70` a `72`, `80-*` |

### Pendientes

1. **Pruebas singulares de dbt en AWS.** El filtro de `dw/terraform/airflow.tf` que elegía los archivos a subir a la EC2 excluía toda carpeta llamada `tests`, incluida `dw/dbt/tests/`, así que las cinco pruebas singulares (fechas, festivos y conciliación de ventas por fecha de negocio) no se ejecutaron en ninguno de los dos despliegues: `PASS=93` corresponde a las pruebas declaradas en los `.yml`. El filtro ya se corrigió; en el próximo despliegue `dbt.test` las incluirá por primera vez sobre Redshift y hay que retomar las capturas `14-dbt-test-a/b`.

Resueltos el 2026-10-30: dashboard de QuickSight, nueva evidencia del fallo controlado con un lote que trae filas nuevas y captura de la eliminación (`90-terraform-destroy`). El mismo día se corrigió `airflow.tf`: el `fileset` recorría `terraform/`, donde aparecen el plan y el state entre `plan` y `apply`, y el primer `terraform apply dw.tfplan` fallaba con *inconsistent result*.

---

## Parte 2. Data lake

### Requisitos e implementación

| Requisito | Implementación |
| --- | --- |
| 4.1 Tres feeds RSS de El Tiempo con medio, sección y URL; manejo de respuestas vacías, errores HTTP y XML malformado | `dl/src/feeds.yml`, `dl/src/newslake/rss.py`, `dl/docs/datos.md` |
| 4.2 Bronze con metadatos por lote, reproceso sin consultar los RSS y distinción entre extracción nueva y reproceso | `dl/src/newslake/bronze.py`; `mode` y `sources_reused` en `pipeline_runs` |
| 4.3 Silver: limpieza, fechas UTC, obligatorios, duplicados, cambios, cuarentena con motivo | `dl/src/newslake/silver.py`, `dl/docs/datos.md` |
| 4.3 Enriquecimiento con Comprehend, caché por hash del texto y estados explícitos | `dl/src/newslake/enrich.py` |
| 4.4 Gold y Glue Data Catalog (definiciones explícitas en Terraform) | `dl/src/newslake/gold.py`, `dl/terraform/glue.tf` |
| 4.4 Consultas Athena: fecha y sección, top de organizaciones, personas y lugares, presencia por día, frases por sección, conciliación, bytes con y sin filtro de partición | `dl/sql/athena/01` a `12` |
| 4.5 Validaciones: unicidad, obligatorios, fechas y enlaces, huérfanos, conciliación, clasificación de registros | `dl/sql/athena/validations/v01` a `v07` |
| 4.5 Conteos por lote y diferencia entre recibidos y noticias únicas | `pipeline_runs` (una fila por procesamiento), `dl/docs/datos.md` |
| 4.5 Publicación sin resultados parciales ni duplicados visibles; recuperación sin pérdida | `dl/src/newslake/storage.py`, `dl/docs/operacion_y_evidencias.md` |
| 4.6 DAG en EC2 con las nueve etapas, programación, reintentos, logs por fuente, lote y tarea, control de concurrencia y parámetros de reproceso | `dl/airflow/dags/newslake_pipeline.py`, `dl/terraform/airflow.tf` |
| Documentación (arquitectura, fuentes, diccionario, reglas, incrementalidad, despliegue, costos) | `dl/docs/` |
| Pruebas locales de los escenarios (S3 y Glue simulados) | `dl/tests/`: 25 pruebas |

### Ejecución en AWS (2026-10-30, hora de Bogotá; `run_id` en UTC)

La infraestructura se creó con Terraform en `us-east-1` (94 recursos; EC2 `t3.medium`, DAG cada 30 minutos) y se eliminó con `terraform destroy` (94 recursos, con el DAG pausado antes). La AWS CLI no encontró instancias, volúmenes, bucket, base de Glue, workgroup, secretos, roles ni VPC del proyecto (captura `90`). Evidencias en `dl/docs/evidencias/`.

| Corrida | Escenario | Resultado | Evidencia |
| --- | --- | --- | --- |
| `r20261031T033000Z` (programada) | Carga inicial | `complete`: 30 recibidas, 30 nuevas, 30 llamadas a Comprehend, 13 particiones publicadas | `00`, `10`, `30` |
| `r20261031T033000Z` (repetida con `{"run_id": ...}`) | Repetición de un lote exitoso | Fila nueva con `mode = reprocess`, las 3 fuentes en `sources_reused`, `new = 0`, `repeated = 30`, 0 llamadas, 0 particiones; la fila original conserva sus conteos; `news` 30 filas = 30 `news_id` | `30`, `31` |
| `r20261031T040720Z` | Registros defectuosos (`simulate_defects_from_run`) | 39 recibidas, 9 rechazadas (3 por feed: `missing_title`, `missing_link`, `invalid_link`) con `bronze_key`; 0 nuevas, 0 llamadas | `20` |
| `r20261031T040836Z` | Noticia modificada (`simulate_change_from_run`) | `updated = 3`, 3 llamadas, 27 desde la caché, 6 particiones | `40` |
| `r20261031T041003Z` | Fuente caída (`unavailable_source`) | `partial`, `sources_failed = [eltiempo_deportes]`; Deportes sigue con sus 10 noticias. La consulta real de Economía y Política devolvió los títulos originales (`updated = 2`, 0 llamadas: estaban en caché) | `80`, `81`, `82` |
| `r20261031T041133Z` | Fallo controlado (`simulate_failure`) y *Clear* de `failure_injection` con *Downstream* | Primero `failed` en `failure_injection` sin publicar (Athena seguía mostrando la versión anterior, verificado con consulta; sin captura de ese momento); después del *Clear*, `complete`, 6 particiones, 30 filas = 30 `news_id` | `60`, `61`, `63`, `64` |
| `r20261031T040000Z` (programada) | Corrida programada sin noticias nuevas | Los RSS eran idénticos a los de las 23:00: `new = 0`, `updated = 3` (vuelven los títulos reales), 0 llamadas (caché), 5 particiones. La partición `2026-10-31` pasó a `version=r20261031T040000Z` | `41`, `56` |
| `rebuild20261031T043303Z` | Reproceso desde Bronze (`rebuild_from_bronze`) | Recorre las 179 noticias recibidas en todas las corridas, 0 llamadas a Comprehend; huella de (`news_id`, `content_hash`, `version`) igual antes y después (`8CCE6F…EA2E`) | `45` |
| `r20261031T043000Z` (programada, 00:00) | Nuevo lote incremental | 1 noticia nueva de Política: `new = 1`, `comprehend_calls = 1 = new`, 30 desde la caché, `unique_news_current` 30 → 31; `news` 31 filas = 31 `news_id` | `41` |

Consultas y validaciones sobre el estado de las 23:35, con 30 noticias (consultas 02, 07 y 08 con la fecha `2026-10-31` en lugar del ejemplo `2026-10-21`, que no tiene datos):

| Medida | Resultado | Evidencia |
| --- | --- | --- |
| Consultas 01 a 05 y 12 | Devuelven datos | `70` a `73` |
| Bytes escaneados con y sin filtro de partición (07 vs. 08) | 109 vs. 302 bytes, mismo resultado (15 noticias, 3 secciones); los mismos valores en `pipeline_runs` | `74` |
| Validaciones `v01` a `v07` | `failures = 0` en todas | `75`, `76` |
| Comprehend | 36 textos analizados (`es`, 0 truncados), 30 noticias `analyzed`; entidades con puntaje (p. ej. PERSON 0.999) | `77` |
| Bronze, Silver, Gold y Glue | Estructura de carpetas, versiones publicadas y ubicación de cada partición | `50`, `55`, `56` |

Las imágenes de Athena (`20`, `30`, `40`, `41`, `45`, `50`, `64`, `70` a `77`, `82`) son consultas ejecutadas en el workgroup `de-aws-dev-dl` con su resultado y bytes escaneados, renderizadas como imagen. `55` y `56` son salidas de la AWS CLI (`aws s3 ls`, `aws glue get-tables` y `get-partitions`), porque la extensión de Chrome no tuvo permiso para tomar capturas de la consola. Las de Airflow (`10`, `31`, `60`, `61`, `63`, `80`, `81`) son capturas de la interfaz.

Limitaciones:

- **Nueva partición.** Todas las noticias de los feeds eran del 30 y 31 de octubre (UTC), así que no apareció una fecha nueva después de la carga inicial, que fue la que creó las dos particiones (`CreationTime` en `56`). Lo que se ve en vivo es el cambio de ubicación de una partición existente al publicar una versión nueva. Una partición nueva aparece con la primera noticia del 1 de noviembre UTC (19:00 en Bogotá).
- La organización `1` en la consulta 03 es una entidad que Comprehend detectó en un texto; se deja como dato de origen.
