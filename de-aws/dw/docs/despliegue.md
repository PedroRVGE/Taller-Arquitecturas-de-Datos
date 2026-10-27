# Despliegue, ejecución y eliminación

Todo se crea con Terraform desde `dw/terraform`. Ningún secreto vive en el repositorio: Terraform genera las contraseñas, las guarda en Secrets Manager y la EC2 las lee al arrancar.

## Requisitos

- Terraform 1.6 o superior y AWS CLI v2 con credenciales (`aws sts get-caller-identity` debe responder).
- Plugin de Session Manager para el túnel a Airflow: [instalación](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
- Cuota para Redshift Serverless y DocumentDB en la región (por defecto `us-east-1`).

## 1. Configurar

```bash
cd dw/terraform
cp terraform.tfvars.example terraform.tfvars
curl -s https://checkip.amazonaws.com   # tu IP pública para allowed_cidrs
```

Edita `terraform.tfvars`. En AWS Academy (Learner Lab) descomenta `existing_instance_profile_name = "LabInstanceProfile"` y `existing_redshift_role_arn` con el ARN de `LabRole`, porque esas cuentas no permiten crear roles IAM.

## 2. Crear la infraestructura

```bash
terraform init          # descarga los providers aws y random y los módulos locales
terraform fmt -check    # formato
terraform validate      # sintaxis y tipos, sin llamar a AWS
terraform plan -out dw.tfplan
terraform apply dw.tfplan
```

El `apply` tarda de 15 a 25 minutos: DocumentDB y Redshift son los recursos más lentos. La EC2 arranca al final, cuando el secreto ya tiene los endpoints, y en unos 5 minutos más construye la imagen de Airflow y levanta los contenedores (`/var/log/de-aws-bootstrap.log` en la instancia).

```bash
terraform output                                  # bucket, endpoints, comando del túnel
aws secretsmanager get-secret-value --secret-id "$(terraform output -raw secret_arn)" \
  --query SecretString --output text | jq -r .AIRFLOW_ADMIN_PASSWORD
```

## 3. Abrir Airflow

- Con tu IP en `allowed_cidrs`: `terraform output -raw airflow_url`.
- Sin puertos abiertos (recomendado): `make dw-tunnel` desde la raíz y abrir <http://localhost:8080>.

Usuario `airflow` y la contraseña del paso anterior.

## 4. Ejecutar

| Paso | DAG y configuración |
| --- | --- |
| Crear fuentes con datos y defectos | `dw_source_ops` con `{"action": "generate", "bad_records": 2}` (el siguiente `dw_pipeline` en `auto` será `full`) |
| Carga inicial | `dw_pipeline` con `{"mode": "auto"}`; el primer lote queda `full` |
| Nuevo lote incremental | `dw_source_ops` `{"action": "append_batch", "batch_number": 2}` y luego `dw_pipeline` |
| Cambio SCD tipo 2 | `dw_source_ops` `{"action": "simulate_changes"}` y luego `dw_pipeline` |
| Repetir un lote exitoso | `dw_pipeline` con `{"batch_id": "<id>", "mode": "reprocess"}` |
| Fallo controlado | `dw_pipeline` con `{"simulate_failure": true}`; luego *Clear* de `failure_injection` |
| Dashboard | Suscripción a QuickSight, fuente de datos y dataset sobre `marts.rpt_sales_dashboard`: ver [quicksight.md](quicksight.md) |

`dw_pipeline` también corre todos los días a las 06:00 (America/Bogota) en modo `auto`. El detalle de cada escenario y las consultas de evidencia están en [operacion_y_evidencias.md](operacion_y_evidencias.md).

## 5. Actualizar código

Después de cambiar DAGs, scripts o dbt:

```bash
make dw-deploy-code     # sube el código a S3 y ejecuta de-aws-deploy en la EC2 por SSM
```

## 6. Eliminar todo

```bash
cd dw/terraform
terraform destroy
```

Borra la EC2, RDS (sin snapshot final), DocumentDB, Redshift, el bucket con su contenido (`force_destroy`), el secreto (sin ventana de recuperación) y la red. Comprueba en la consola de Billing que no queden recursos. Si creaste un dataset de QuickSight a mano, bórralo antes; la suscripción de QuickSight se cancela aparte.
