# Parte 2: data lake de noticias RSS

Tres feeds RSS de El Tiempo → Bronze (XML original) → Silver (limpieza, deduplicación, Comprehend) → Gold (agregados) en un solo bucket S3, catalogado en Glue y consultado con Athena. Airflow corre en una EC2.

| Carpeta | Contenido |
| --- | --- |
| `terraform/` | Bucket, Glue (base + 10 tablas con diccionario de datos), Athena, EC2 e IAM |
| `airflow/` | Imagen de Airflow 2.11, `docker-compose.yml`, DAG `newslake_pipeline` |
| `src/` | Paquete `newslake` y `feeds.yml` (medio, sección y URL de cada fuente) |
| `sql/athena/` | Consultas de negocio, validaciones del DAG y DDL de referencia |
| `tests/` | Recorrido completo del DAG con S3 y Glue simulados |
| `docs/` | [Arquitectura](docs/arquitectura.md) · [Datos y reglas](docs/datos.md) · [Operación y evidencias](docs/operacion_y_evidencias.md) · [Despliegue y costos](docs/despliegue_y_costos.md) |
