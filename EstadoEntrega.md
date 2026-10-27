# Estado de la entrega

Relación entre los requisitos del enunciado (`Actividad Arquitecturas de Datos.pdf`), la implementación en `de-aws/` y las evidencias en `de-aws/dw/docs/evidencias/`.

| Parte | Implementación | Evidencias |
| --- | --- | --- |
| 1. Data warehouse (`de-aws/dw`) | Completa | Escenarios de la sección 3.5 ejecutados en AWS el 2026-10-26. Pendientes: dashboard de QuickSight, nueva captura del fallo controlado y captura de la eliminación |
| 2. Data lake (`de-aws/dl`) | Completa, con pruebas locales | Pendientes: requiere despliegue en AWS |

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
| `b20261027T033805Z` | Fallo controlado y reejecución | Recuperado con *Clear*. El lote no traía filas nuevas; ver pendiente 2 |
| (lote 3) | Lote nuevo hasta el indicador | Ventas totales 7 357 260.42 → 7 420 730.58 (256 → 259 líneas) |

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
| 3.5 Fallo controlado y reejecución | Cumplido; evidencia por repetir (pendiente 2) | — | `60` a `64` |
| 3.5 Pruebas de unicidad, obligatorios, relaciones y reglas de negocio | Cumplido | `dw/dbt/models/**/*.yml`, `dw/dbt/tests/` | `14-dbt-test-a/b` |
| 3.5 Conciliación de un indicador agregado | Cumplido | Tarea `reconcile`, `ops.reconciliation` | `16-conciliacion` |
| 3.5 Identificación del lote y momento en que avanza su marca | Cumplido | `dw/docs/operacion_y_evidencias.md` | — |
| 3.6 Dashboard de QuickSight con gráfica, indicador y filtro sobre marts | Pendiente (pendiente 1) | Vista `marts.rpt_sales_dashboard`, guía en `dw/docs/quicksight.md` | — |
| 3.6 Consulta SQL que valida el indicador | Consulta lista; falta compararla con el dashboard | `dw/docs/quicksight.md` | `72-sql-validacion-kpi` |
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
| Evidencias de ejecución, pruebas y escenarios | Cumplido | Capturas `00` a `64` |
| Dashboard de QuickSight y consulta del indicador | Pendiente | Pendiente 1 |

### Pendientes

1. **Dashboard de QuickSight** (capturas `70`, `71`, `72` con el mismo filtro y fecha del dashboard, `73` si se usa SPICE y `80-demo-dashboard-despues`).
   1. Desplegar con `terraform apply` en `dw/terraform` y ejecutar los pasos 1 a 5 de `dw/docs/operacion_y_evidencias.md`.
   2. Suscribirse a QuickSight en `us-east-1` y seguir `dw/docs/quicksight.md` con Direct Query sobre `marts.rpt_sales_dashboard`.
   3. Tomar las capturas del paso 7 y la demostración del paso 8.
   4. Eliminar la cuenta de QuickSight (*Manage QuickSight → Account settings → Delete account*); `terraform destroy` no la elimina.
2. **Nueva captura del fallo controlado** (`60` a `64`). En la ejecución del 2026-10-26 el lote fallido no traía filas nuevas, de modo que la ausencia de pérdidas y duplicados no quedó demostrada, y el log anterior no incluía el prefijo `[batch=… task=…]`, que ya se agregó. El paso 6 de la guía ahora agrega un lote con `append_batch` antes del fallo; se espera que el lote recupere sus 3 filas sin duplicados y que `fct_sales` pase de 256 a 259 líneas. Se hace en el mismo despliegue del pendiente 1.
3. **Captura de la eliminación** (`90-terraform-destroy.png`): salida de `terraform destroy` al cerrar el despliegue de los pendientes 1 y 2.

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
| Pruebas locales de los escenarios (S3 y Glue simulados) | `dl/tests/`: 24 pruebas |

### Pendiente: evidencias en AWS

1. Desplegar con Terraform desde `dl/terraform` según `dl/docs/despliegue_y_costos.md`.
2. Ejecutar los escenarios de la sección 4.5 siguiendo `dl/docs/operacion_y_evidencias.md`: carga inicial, registros defectuosos, repetición, lote incremental, noticia modificada, fallo controlado, reproceso desde Bronze y nueva partición.
3. Guardar en `dl/docs/evidencias/` las capturas del DAG, los archivos de Bronze, Silver y Gold, los resultados de Comprehend, las tablas y particiones de Glue, las consultas de Athena (incluida la comparación de bytes escaneados) y cada escenario.
4. Pausar el DAG, ejecutar `terraform destroy` y verificar que no queden recursos.
