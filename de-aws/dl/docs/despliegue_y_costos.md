# Despliegue, ejecución, eliminación y costos

## Desplegar

```bash
cd dl/terraform
cp terraform.tfvars.example terraform.tfvars   # región, allowed_cidrs; en AWS Academy, LabInstanceProfile
terraform init
terraform plan -out dl.tfplan
terraform apply dl.tfplan
terraform output
```

Crea en unos 3 minutos: VPC de 2 zonas, bucket único, base `news_lake` con 10 tablas, workgroup de Athena con las consultas guardadas, secreto de Airflow y la EC2. Airflow queda disponible unos 5 minutos después (imagen en construcción; log en `/var/log/de-aws-bootstrap.log`).

## Ejecutar

1. `make dl-tunnel` y abrir <http://localhost:8081> (o `terraform output airflow_url` si su IP está en `allowed_cidrs`). Usuario `airflow`; contraseña en el secreto (`terraform output secret_arn`).
2. `newslake_pipeline` se crea activo y corre cada 30 minutos (`pipeline_schedule`). Para no esperar: *Trigger DAG*.
3. Consultar en Athena con el workgroup `de-aws-dev-dl` (pestaña *Saved queries*).

Actualizar código: `make dl-deploy-code`.

## Eliminar

```bash
cd dl/terraform
terraform destroy
```

Borra la EC2, el bucket con todas las zonas (`force_destroy`), la base y las tablas de Glue, el workgroup (`force_destroy` borra también su historial) y el secreto. Las particiones registradas por el DAG se eliminan con sus tablas.

## Costos aproximados (us-east-1)

Precios de referencia bajo demanda; confírmelos en la [calculadora de AWS](https://calculator.aws/).

| Recurso | Precio de referencia | Uso del taller | Costo aproximado |
| --- | --- | --- | --- |
| EC2 t3.medium + 30 GB gp3 | ~US$0,042/h | 8 h de laboratorio | ~US$0,35 por día |
| IPv4 pública | ~US$0,005/h | 8 h | ~US$0,04 por día |
| S3 | US$0,023 por GB-mes + peticiones | Pocos MB | < US$0,01 |
| Glue Data Catalog | Gratis hasta 1 millón de objetos y solicitudes al mes | 10 tablas, cientos de particiones | US$0 |
| Athena | US$5 por TB escaneado, mínimo 10 MB por consulta | ~20 consultas por corrida | ~US$0,001 por corrida |
| Comprehend | ~US$0,0001 por unidad de 100 caracteres, mínimo 3 unidades por solicitud; 2 APIs por texto | ~30 textos en la carga inicial, solo los nuevos después | ~US$0,02 la carga inicial; centavos por día |
| Secrets Manager | US$0,40 por secreto-mes | 1 secreto | ~US$0,01 por día |

Un día de laboratorio cuesta menos de US$1; lo que más pesa es la EC2. La caché de Comprehend es el control de costo principal: una noticia ya analizada no vuelve a pagar. Otros controles: tope de 1 GB escaneado por consulta en el workgroup, particiones por fecha para escanear menos, Parquet comprimido (columnar: Athena lee solo las columnas de la consulta) y expiración de resultados de Athena y archivos de trabajo.

## Seguridad

El mismo esquema de la Parte 1: rol IAM de la instancia (Comprehend sin ARN de recurso, Athena limitado al workgroup y Glue limitado a la base `news_lake`), bucket privado cifrado y versionado con TLS obligatorio, IMDSv2, UI de Airflow por túnel de SSM o solo desde `allowed_cidrs`, y secretos en Secrets Manager.
