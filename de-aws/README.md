# de-aws: data warehouse y data lake en AWS con Terraform

Solución del taller *Arquitecturas de Datos* (Ingeniería de Datos, Universidad EIA, 2026-2). Toda la infraestructura se declara con Terraform; Airflow corre en EC2 con Docker Compose.

| Parte | Qué construye | Servicios |
| --- | --- | --- |
| [Parte 1 · Data warehouse](dw/) | Migración del proyecto DE-DW: RDS for MySQL y DocumentDB → S3 por lotes → Redshift con `COPY` → dbt → QuickSight | VPC, RDS, DocumentDB, S3, Redshift Serverless, EC2, Secrets Manager, QuickSight |
| [Parte 2 · Data lake](dl/) | Noticias de 3 feeds RSS de El Tiempo en una arquitectura medallion Bronze/Silver/Gold con Comprehend | S3 (un bucket), Glue Data Catalog, Athena, Comprehend, EC2, Secrets Manager |

## Estructura

```text
de-aws/
├── README.md
├── Makefile                 # atajos: make dw-plan, make dl-tunnel, make test...
├── modules/
│   ├── network/             # VPC, subredes públicas y privadas, IGW, endpoint de S3
│   └── ec2-airflow/         # EC2 + rol IAM + IMDSv2 + user_data con Docker Compose
├── dw/                      # Parte 1: Data Warehouse
│   ├── terraform/           # VPC, RDS, DocumentDB, S3, Redshift, EC2, secreto, QuickSight
│   ├── airflow/             # Dockerfile, docker-compose.yml y dags/
│   ├── extract/             # RDS / DocumentDB → S3 → Redshift, control de lotes, generadores
│   ├── dbt/                 # proyecto DE-DW adaptado a Redshift
│   ├── tests/
│   ├── docs/
│   └── .env.example
└── dl/                      # Parte 2: Data Lake
    ├── terraform/           # S3, Glue DB + 10 tablas, Athena, EC2, IAM (Comprehend/Athena/Glue)
    ├── airflow/             # Dockerfile, docker-compose.yml y dags/
    ├── src/                 # newslake: rss → bronze → silver → gold, feeds.yml
    ├── sql/athena/          # consultas de negocio, validaciones y DDL de referencia
    ├── tests/
    ├── docs/
    └── .env.example
```

## Requisitos

- Terraform ≥ 1.6, AWS CLI v2 con credenciales y el plugin de Session Manager.
- Python 3.11+ solo para las pruebas locales (`make test`).
- En AWS Academy / Learner Lab: usar `LabInstanceProfile` y `LabRole` (variables `existing_*` en `terraform.tfvars`), porque esas cuentas no permiten crear roles IAM.

## Inicio rápido

```bash
# Parte 1
cp dw/terraform/terraform.tfvars.example dw/terraform/terraform.tfvars   # editar
make dw-init dw-plan dw-apply
make dw-tunnel        # Airflow en http://localhost:8080

# Parte 2
cp dl/terraform/terraform.tfvars.example dl/terraform/terraform.tfvars   # editar
make dl-init dl-plan dl-apply
make dl-tunnel        # Airflow en http://localhost:8081

# Al terminar el laboratorio
make dw-destroy dl-destroy
```

En la Parte 1 active los DAGs `dw_source_ops` y `dw_pipeline` en la interfaz (se crean pausados). En la Parte 2 `newslake_pipeline` se crea activo y corre cada 30 minutos.

## Documentación

| Parte 1 | Parte 2 |
| --- | --- |
| [Arquitectura](dw/docs/arquitectura.md) | [Arquitectura](dl/docs/arquitectura.md) |
| [Despliegue, ejecución y eliminación](dw/docs/despliegue.md) | [Despliegue, costos y eliminación](dl/docs/despliegue_y_costos.md) |
| [Decisiones y diferencias con la versión local](dw/docs/decisiones.md) | [Fuentes, reglas y diccionario de datos](dl/docs/datos.md) |
| [Lotes, recuperación y evidencias](dw/docs/operacion_y_evidencias.md) | [Incrementalidad, recuperación y evidencias](dl/docs/operacion_y_evidencias.md) |
| [QuickSight](dw/docs/quicksight.md) | |
| [Costos y seguridad](dw/docs/costos_y_seguridad.md) | |

## Principios comunes

- **Sin secretos en el repositorio.** Terraform genera las contraseñas y las guarda en Secrets Manager; la EC2 arma su `.env` al arrancar. Los `.env.example` documentan las variables.
- **Sin llaves de acceso.** La EC2 usa un rol IAM; Redshift lee S3 con su propio rol.
- **Idempotencia.** Lotes inmutables en S3, escrituras condicionales, `DELETE` + `COPY` transaccional, `delete+insert` en dbt, particiones publicadas por puntero en Glue.
- **Recuperación.** Cada corrida tiene un identificador estable; un lote o una corrida se reprocesa desde S3 sin volver a consultar las fuentes.
- **Calidad del código.** `make validate` (Terraform) y `make test` (pytest con S3 y Glue simulados).
