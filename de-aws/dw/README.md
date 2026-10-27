# Parte 1: data warehouse en AWS

RDS for MySQL y DocumentDB → S3 (`raw/<fuente>/<entidad>/batch_id=<id>/`) → Redshift Serverless con `COPY` → dbt (intermediate, quarantine, snapshots, marts) → QuickSight. Airflow y dbt corren en una EC2.

| Carpeta | Contenido |
| --- | --- |
| `terraform/` | Infraestructura completa (usa `../../modules`) |
| `airflow/` | Imagen de Airflow 2.11 + dbt-redshift, `docker-compose.yml`, DAGs `dw_pipeline` y `dw_source_ops` |
| `extract/` | Extracción por lotes, manifiestos en S3, carga a Redshift, tablas de control `ops.*`, generadores de datos |
| `dbt/` | Proyecto DE-DW adaptado a Redshift |
| `tests/` | Pruebas sin AWS de formato, manifiestos y SQL de carga |
| `docs/` | [Arquitectura](docs/arquitectura.md) · [Despliegue](docs/despliegue.md) · [Decisiones](docs/decisiones.md) · [Operación y evidencias](docs/operacion_y_evidencias.md) · [QuickSight](docs/quicksight.md) · [Costos y seguridad](docs/costos_y_seguridad.md) |
