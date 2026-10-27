# Estimación de costos y controles de seguridad

## Costos aproximados (us-east-1, precios bajo demanda)

Valores aproximados para orientarse; confírmelos en la [calculadora de AWS](https://calculator.aws/) antes de desplegar.

| Recurso | Precio de referencia | Día de laboratorio (8 h) | Mes encendido 24/7 |
| --- | --- | --- | --- |
| EC2 t3.large + 30 GB gp3 | ~US$0,083/h + ~US$2,4/mes | ~US$0,7 | ~US$63 |
| RDS db.t4g.micro + 20 GB gp3 | ~US$0,016/h + ~US$2,3/mes | ~US$0,15 | ~US$14 |
| DocumentDB db.t3.medium | ~US$0,078/h + almacenamiento e I/O | ~US$0,65 | ~US$58 |
| Redshift Serverless 8 RPU | ~US$0,375 por RPU-hora, cobro por segundo (mínimo 60 s) | 1 h de cómputo activo ≈ US$3 | Según uso; el límite diario corta en 16 RPU-h |
| IPv4 públicas (EC2 y Redshift) | ~US$0,005/h cada una | ~US$0,08 | ~US$7 |
| Secrets Manager | US$0,40 por secreto-mes | — | US$0,40 |
| S3 (MB de CSV) | US$0,023 por GB-mes | ~0 | ~0 |
| QuickSight | Por usuario-mes (revise prueba gratuita) | — | ~US$9 a 24 por autor |

Un día de laboratorio cuesta alrededor de US$5. Lo más caro es dejar los recursos encendidos: **ejecute `terraform destroy` al terminar**. Controles de costo incluidos:

- `aws_redshiftserverless_usage_limit`: desactiva el workgroup al pasar `redshift_daily_rpu_hours_limit` RPU-hora en el día.
- Sin NAT Gateway (~US$33/mes cada uno): la EC2 usa subred pública y el endpoint gateway de S3, que es gratuito.
- Instancias de una sola zona y clases mínimas; RDS sin Multi-AZ.
- Ciclo de vida de S3: borra versiones no vigentes a los 30 días.
- Etiquetas `Project`, `Environment`, `Owner` en todo (`default_tags`) para filtrar en Cost Explorer.

## Controles de seguridad

| Control | Implementación |
| --- | --- |
| Red privada para las fuentes | RDS y DocumentDB en subredes privadas, `publicly_accessible = false` |
| Mínima exposición | Security groups que referencian el SG de la EC2; la UI de Airflow solo desde `allowed_cidrs` (nunca `0.0.0.0/0`, validado en Terraform) o por túnel de SSM sin puertos abiertos |
| Sin llaves de acceso | La EC2 usa un rol (instance profile); Redshift lee S3 con su propio rol en `COPY ... IAM_ROLE` |
| Mínimo privilegio | El rol de Redshift solo hace `GetObject` y `ListBucket` sobre `raw/`; el de la EC2 solo accede a su bucket y a su secreto |
| Secretos | Generados con `random_password`, guardados en Secrets Manager, leídos al arrancar; `.env` solo en la instancia |
| Cifrado en reposo | SSE-S3 en el bucket; `storage_encrypted` en RDS y DocumentDB; Redshift cifrado por defecto; EBS cifrado |
| Cifrado en tránsito | TLS con RDS y DocumentDB (bundle de AWS), `sslmode: require` en dbt, `ssl=True` en redshift_connector, la política del bucket niega peticiones sin TLS |
| S3 privado | Bloqueo de acceso público (las 4 opciones), `BucketOwnerEnforced` (sin ACL), versionado |
| Metadatos de la instancia | IMDSv2 obligatorio (`http_tokens = "required"`), que mitiga ataques SSRF |
| Datos de QuickSight | Redshift público solo para el rango IP de QuickSight de la región, con usuario y contraseña |
| Repositorio | `.gitignore` excluye `*.tfstate`, `terraform.tfvars` y `.env` |

Riesgos aceptados para el curso: el state local contiene las contraseñas (use el backend S3 de `backend.tf.example` en equipo); Airflow usa HTTP en el puerto 8080 (use el túnel de SSM); el usuario maestro de RDS extrae los datos (en producción se crea un usuario de solo lectura).
