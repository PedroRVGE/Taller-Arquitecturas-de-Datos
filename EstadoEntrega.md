# Estado de la entrega

Comparación del enunciado (`Actividad Arquitecturas de Datos.pdf`) contra el repositorio (`de-aws/`) y las capturas de `de-aws/dw/docs/evidencias/`. Corte: 2026-10-27, después de la corrida en AWS y del `terraform destroy`.

Resumen:

- **Parte 1, Data Warehouse (`de-aws/dw`)**: código, documentación y evidencias de los 6 escenarios completos. Falta todo QuickSight (omitido por el tope de 5 USD) y la captura del `terraform destroy`.
- **Parte 2, Data Lake (`de-aws/dl`)**: hay código, SQL y documentación, pero no se ha desplegado ni hay evidencias. No se revisó contra el enunciado en esta sesión.

---

## Parte 1. Data Warehouse

### Corrida en AWS (noche del 2026-10-26)

Infraestructura creada con Terraform en `us-east-1` (131 recursos) y eliminada con `terraform destroy` (131 destruidos). Verificado con la AWS CLI: no quedan EC2, RDS, DocumentDB, snapshots, Redshift Serverless, bucket, NAT/EIP, VPC, secretos ni roles IAM del proyecto. QuickSight nunca se suscribió.

| Lote | Escenario | Resultado |
| --- | --- | --- |
| `b20261025T110000Z` | Carga inicial (`full`) | `dbt.run_marts` falló 3 veces: `convert_timezone` no acepta `timestamptz` (`fct_reviews`, `fct_navigation_events`) y Redshift no admite la subconsulta correlacionada dentro de `OR` del filtro incremental (`fct_sales`). Se corrigieron las macros `to_business_time` e `incremental_candidate_filter`, se redesplegó el código y con *Clear* el lote terminó en `success` |
| `b20261025T110000Z` | Repetición (paso 3, `reprocess`) | `attempts = 2`, conteos sin cambios |
| `b20261027T032447Z` | Nuevo lote incremental (lote 2) | +3 filas por hecho |
| (lote del paso 5) | `simulate_changes` | Nueva versión SCD2 del cliente 1 y cierre de la anterior |
| `b20261027T033805Z` | Fallo controlado | Recuperado con *Clear*; `attempts` quedó en 1 (el *Clear* no vuelve a ejecutar `resolve_batch`); en `raw.mysql_order_items` no hubo filas nuevas (`count = count distinct = 0`) |
| (lote 3) | Demostración | KPI sin filtro 7 357 260.42 → 7 420 730.58 (256 → 259 líneas) |

Conteos obtenidos, todos iguales a los de DE-DW:

| Medida | Obtenido | Esperado |
| --- | --- | --- |
| `raw` orders / reviews (lote inicial) | 106 / 158 | 106 / 158 |
| `fct_sales` / `fct_reviews` / `fct_navigation_events` | 253 / 150 / 500 | 253 / 150 / 500 |
| `dbt test` | PASS=93, ERROR=0 | sin errores |
| Conciliación (`ops.reconciliation`) | `passed = true` en todas | `true` |
| Cuarentena clientes / productos / pedidos / líneas / reseñas / eventos | 4 / 2 / 4 / 6 / 8 / 8 | 4 / 2 / 4 / 6 / 8 / 8 |
| `stg = valid + quarantine` (líneas de pedido) | 259 = 253 + 6 | igualdad |
| Reproceso | sin cambios | sin cambios |
| Hechos tras el lote 2 | 256 / 153 / 503 | 256 / 153 / 503 |

### Requisitos funcionales

| Requisito (sección del PDF) | Estado | Dónde está | Evidencia |
| --- | --- | --- | --- |
| 3.1 MySQL → RDS, MongoDB → DocumentDB | Hecho | `dw/terraform/rds.tf`, `docdb.tf` | `02-fuentes-rds-docdb-a/b` |
| 3.1 Scripts de generación y conexión adaptados, mismos datos que local | Hecho | `dw/extract/sources/`, `dw/extract/connections.py` | `10-source-ops-generate`; conteos iguales a DE-DW |
| 3.1 Ajustes MongoDB vs DocumentDB documentados | Hecho | `dw/docs/decisiones.md` ("Ajustes por DocumentDB") | — |
| 3.2 Extracción inicial e incremental por lote | Hecho | `dw/extract/extractors.py` (watermarks) | `15-*` (full), `42-batch-entities-incremental` |
| 3.2 Bucket privado, ruta con fuente/entidad/lote | Hecho | `dw/terraform/s3.tf`, `dw/extract/s3_batches.py` | `01-s3-bucket-a/b`, `12-s3-lote` |
| 3.2 Registrar fecha, lote y cantidad | Hecho | `_manifest.json` y `ops.batch_entities` | `13-s3-manifiesto`, `42-*` |
| 3.2 Reprocesar sin consultar fuentes; no sobrescribir otros lotes | Hecho | manifiesto + escritura condicional `If-None-Match` | `31-reproceso-log` |
| 3.2 Justificar formato (CSV.gz) | Hecho | `dw/docs/decisiones.md` | — |
| 3.3 Redshift en lugar de DuckDB, COPY desde S3 con rol IAM | Hecho | `dw/extract/redshift.py`, `dw/terraform/redshift.tf` | `03-redshift-workgroup-a/b`, `15-*` |
| 3.3 Capas raw, intermediate, quarantine, marts | Hecho | `dw/dbt/models/` | `15-*`, `20-*`, `21-*` |
| 3.3 Modelo dimensional y SCD2 | Hecho | `dw/dbt/models/marts/`, `dw/dbt/snapshots/` | `51-scd2-dim-customer` |
| 3.3 Adaptación de tipos, funciones y dbt a Redshift | Hecho | `dw/dbt/macros/`, tabla en `dw/docs/decisiones.md` | errores `ERROR-*` y su corrección |
| 3.3 Rechazos con motivo y lote de origen | Hecho | `quarantine_rejections_log` | `20-cuarentena-motivos-a/b` |
| 3.4 DAG con las 7 etapas, dependencias, reintentos y logs por lote | Hecho | `dw/airflow/dags/dw_pipeline.py` | `11-pipeline-carga-inicial-a/b`, `41-*` |
| 3.4 Airflow y dbt en la misma EC2; SQL ejecutado en Redshift | Hecho | `dw/terraform/airflow.tf`, `dw/extract/dbt_runner.py` | `04-ec2-airflow`, `14-dbt-test-a/b` |
| 3.4 Sin intervención manual entre etapas | Hecho | DAG programado diario 06:00 | `11-*` (corrida programada) |
| 3.5 Carga inicial | Hecho | — | `10`, `11`, `12`, `13`, `15` |
| 3.5 Registros defectuosos | Hecho | — | `20-*`, `21` |
| 3.5 Repetición de un lote exitoso | Hecho | — | `30`, `31`, `32` |
| 3.5 Nuevo lote incremental | Hecho | — | `40`, `41-*`, `42`, `43` |
| 3.5 Cambio en atributo historizado | Hecho | — | `50`, `51` |
| 3.5 Fallo controlado y reejecución | Hecho, con una observación (ver pendientes) | — | `60`, `61-*`, `62`, `63`, `64-*` |
| 3.5 Pruebas de unicidad, obligatorios, relaciones y negocio | Hecho | `dw/dbt/models/**/*.yml`, `dw/dbt/tests/` | `14-dbt-test-a/b` (PASS=93) |
| 3.5 Conciliación de un indicador agregado | Hecho | tarea `reconcile`, `ops.reconciliation` | `16-conciliacion` |
| 3.5 Cómo se identifica un lote y cuándo avanza la marca | Hecho | `dw/docs/operacion_y_evidencias.md` | — |
| 3.6 Dashboard QuickSight (gráfica, KPI, filtro) sobre marts | **Falta** | Vista `marts.rpt_sales_dashboard` y guía en `dw/docs/quicksight.md` listas | ninguna |
| 3.6 Consulta SQL que valida el KPI con mismos filtros y fecha | Parcial: consulta ejecutada, sin dashboard con qué compararla | `dw/docs/quicksight.md` | `72-sql-validacion-kpi` |
| 3.6 Actualización de SPICE | Documentado | `dw/docs/quicksight.md` | no aplica si se usa Direct Query |

### Entregables

| Entregable | Estado | Dónde |
| --- | --- | --- |
| DAGs y scripts de extracción y carga | Hecho | `dw/airflow/dags/`, `dw/extract/` |
| Proyecto dbt adaptado a Redshift | Hecho | `dw/dbt/` |
| Despliegue sin secretos y archivo de variables de ejemplo | Hecho | `dw/terraform/` (Secrets Manager), `terraform.tfvars.example`, `.env.example` |
| Diagrama de arquitectura | Hecho | `dw/docs/arquitectura.md` |
| Instrucciones de despliegue, ejecución y eliminación | Hecho | `dw/docs/despliegue.md` |
| Decisiones técnicas y diferencias con la versión local | Hecho | `dw/docs/decisiones.md` |
| Costos y seguridad | Hecho | `dw/docs/costos_y_seguridad.md` |
| Evidencia: ejecución exitosa en Airflow | Hecho | `11-*`, `41-*`, `63` |
| Evidencia: resultados de dbt test | Hecho | `14-*` |
| Evidencia: carga, calidad, incrementalidad, idempotencia, SCD2, fallos | Hecho | capturas 10 a 64 |
| Evidencia: dashboard y consulta del indicador | **Falta el dashboard** | solo `72`, `80-demo-sql-despues` |
| Demostración: lote nuevo hasta el dashboard | Parcial: lote 3 verificado por SQL, sin dashboard | `80-demo-sql-despues` |
| Demostración: identificar y recuperar una ejecución fallida | Listo para explicar | `60` a `64`, `dw/docs/operacion_y_evidencias.md` |

Las capturas `ERROR-dbt-run-marts-log.png` y `ERROR-dbt-run-marts-intento2.png` son evidencia extra del fallo real de dbt en la carga inicial y sirven para explicar la recuperación y las adaptaciones a Redshift.

### Pendientes y cómo completarlos

1. **QuickSight (capturas 70, 71, 73 y el dashboard del paso 8).** Requiere volver a desplegar, porque Redshift ya no existe.
   1. `cd de-aws/dw/terraform && AWS_PROFILE=taller ~/.local/bin/terraform apply` (20-30 min).
   2. En Airflow: `dw_source_ops` `{"action": "generate", "seed": 42, "bad_records": 2}`, despausar `dw_pipeline` y esperar la carga inicial en verde. Si se quiere repetir la demo del lote 3, `append_batch` con 2 y luego con 3, cada uno seguido de `dw_pipeline`.
   3. Suscribirse a QuickSight en `us-east-1` (revisar si ofrece prueba gratuita de 30 días) y seguir `dw/docs/quicksight.md` con **Direct Query** sobre `marts.rpt_sales_dashboard`.
   4. Capturas: `70-quicksight-dashboard.png`, `71-quicksight-filtrado.png`, `72-sql-validacion-kpi.png` (repetirla con el mismo filtro y fecha del dashboard), `80-demo-dashboard-despues.png`. `73-spice-refresh.png` solo si se usa SPICE.
   5. Borrar la cuenta de QuickSight (*Manage QuickSight → Account settings → Delete account*) y ejecutar `terraform destroy`.
   6. Costo estimado de esta repetición: 2-3 USD de infraestructura por unas 3 horas; QuickSight gratis si aplica la prueba.
2. **`90-terraform-destroy.png`.** El destroy ya se ejecutó (`Destroy complete! Resources: 131 destroyed.`). Opciones: capturar la salida del próximo destroy (si se hace el punto 1) o escribir en el informe el resultado y la verificación con la AWS CLI descritos arriba.
3. **Log del paso 6 sin prefijo `[batch=… task=…]`.** `failure_injection` lanza la excepción sin escribir con `batch_log`, así que la línea de error dice el lote ("Fallo controlado del lote …") pero no trae el prefijo que promete la guía. Dos caminos: ajustar el texto de la captura 61 en `dw/docs/operacion_y_evidencias.md`, o en `dw/extract/airflow_tasks.py` escribir un `batch_log(batch["batch_id"], "failure_injection").error(...)` antes del `raise` (requiere volver a correr el paso 6 para recapturar).
4. **Nombres de capturas distintos a los enlaces de la guía.** Varias capturas quedaron divididas con sufijo (`11-…-a/b`, `14-…-a/b`, `15-…-a/b/c`, `20-…-a/b`, `41-…-a/b`, `61-…-a/b`, `64-…-a/b`, `01`, `02`, `03` con `-a/b`) y la del paso 8 se llama `80-demo-sql-despues.png` en vez de `80-demo-dashboard-despues.png`. Hay que actualizar las imágenes enlazadas en `dw/docs/operacion_y_evidencias.md` para que apunten a los archivos reales.
5. **`attempts = 1` en el lote del fallo controlado.** Es correcto (el *Clear* no reejecuta `resolve_batch`), pero la guía dice `attempts = 2` en la captura 64. Ajustar ese texto o recuperar con la opción `reprocess`, que sí incrementa `attempts`.
6. **Opcional: alerta de presupuesto.** Crear un presupuesto mensual en AWS Budgets (por ejemplo 15 USD) desde la consola, fuera de Terraform, antes de volver a desplegar.

---

## Parte 2. Data Lake (`de-aws/dl`)

No se desplegó ni se revisó contra el enunciado en esta sesión. Lo que existe en el repositorio:

| Requisito (sección 4 del PDF) | En el repositorio |
| --- | --- |
| 4.1 Tres feeds RSS de El Tiempo con medio, sección y URL; manejo de vacíos, HTTP y XML malformado | `dl/src/feeds.yml`, `dl/src/newslake/rss.py`, `dl/docs/datos.md` |
| 4.2 Bronze con metadatos por lote y reproceso | `dl/src/newslake/bronze.py` |
| 4.3 Silver: limpieza, validación, deduplicación, cuarentena | `dl/src/newslake/silver.py` |
| 4.3 Comprehend con caché por hash y estados | `dl/src/newslake/enrich.py`, `dl/docs/datos.md` |
| 4.4 Gold y Glue Data Catalog | `dl/src/newslake/gold.py`, `dl/terraform/glue.tf` |
| 4.4 Consultas Athena (fecha/sección, top 10 organizaciones, presencia por día, frases por sección, conciliación, poda de particiones) | `dl/sql/athena/01` a `11` |
| 4.5 Validaciones | `dl/sql/athena/validations/v01` a `v06` |
| 4.6 DAG en EC2 | `dl/airflow/dags/newslake_pipeline.py`, `dl/terraform/airflow.tf` |
| Documentación (arquitectura, fuentes, diccionario, reglas, incrementalidad, despliegue, costos) | `dl/docs/` |
| Pruebas locales | `dl/tests/` |
| **Evidencias** | **Faltan todas**: no existe `dl/docs/evidencias/` |

### Pasos para completar la Parte 2

1. Revisar `dl/` contra la sección 4 del PDF (igual que se hizo con `dw/`) y correr `pytest` en `dl/tests`.
2. Desplegar: `cd de-aws/dl/terraform`, crear `terraform.tfvars` desde el ejemplo, `terraform init`, `plan` y `apply` con el perfil `taller` (ver `dl/docs/despliegue_y_costos.md`).
3. Ejecutar los 8 escenarios de la sección 4.5 siguiendo `dl/docs/operacion_y_evidencias.md`: carga inicial, defectuosos, repetición, incremental, noticia modificada (con copia controlada de un RSS en Bronze), fallo controlado, reproceso desde Bronze y nueva partición.
4. Capturas de: DAG en Airflow, archivos Bronze/Silver/Gold en S3, resultados de Comprehend, tablas y particiones en Glue, consultas Athena (incluida la comparación de bytes escaneados con y sin filtro de partición) y cada escenario. Guardarlas en `dl/docs/evidencias/`.
5. `terraform destroy` y verificar con la AWS CLI que no quede nada.
