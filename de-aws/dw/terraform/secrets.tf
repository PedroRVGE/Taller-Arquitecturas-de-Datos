# Contraseñas generadas por Terraform. Sin caracteres especiales para que viajen
# sin escapes en URIs (mongodb://) y en archivos .env. Quedan en el state:
# por eso el state nunca se versiona y en equipo va a un backend S3 cifrado.
resource "random_password" "mysql" {
  length  = 32
  special = false
}

resource "random_password" "docdb" {
  length  = 32
  special = false
}

resource "random_password" "redshift" {
  length      = 32
  special     = false
  min_upper   = 1
  min_lower   = 1
  min_numeric = 1 # Redshift exige mayúscula, minúscula y número
}

resource "random_password" "airflow_admin" {
  length  = 20
  special = false
}

resource "random_password" "airflow_postgres" {
  length  = 32
  special = false
}

resource "random_password" "airflow_secret_key" {
  length  = 48
  special = false
}

# Fernet necesita 32 bytes en base64 URL-safe con relleno (44 caracteres).
resource "random_id" "fernet" {
  byte_length = 32
}

locals {
  docdb_uri = format(
    "mongodb://%s:%s@%s:27017/?tls=true&tlsCAFile=/opt/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false",
    var.docdb_username,
    random_password.docdb.result,
    aws_docdb_cluster.this.endpoint,
  )

  # Cada clave se convierte en una línea KEY=valor del .env que la EC2 genera al arrancar.
  app_secret = {
    MYSQL_HOST             = aws_db_instance.mysql.address
    MYSQL_PORT             = tostring(aws_db_instance.mysql.port)
    MYSQL_DATABASE         = var.mysql_database
    MYSQL_USER             = var.mysql_username
    MYSQL_PASSWORD         = random_password.mysql.result
    MONGODB_URI            = local.docdb_uri
    MONGO_DATABASE         = var.mongo_database
    REDSHIFT_HOST          = aws_redshiftserverless_workgroup.this.endpoint[0].address
    REDSHIFT_PORT          = tostring(aws_redshiftserverless_workgroup.this.port)
    REDSHIFT_DATABASE      = var.redshift_database
    REDSHIFT_USER          = var.redshift_admin_username
    REDSHIFT_PASSWORD      = random_password.redshift.result
    AIRFLOW_FERNET_KEY     = "${random_id.fernet.b64_url}="
    AIRFLOW_SECRET_KEY     = random_password.airflow_secret_key.result
    AIRFLOW_ADMIN_USERNAME = var.airflow_admin_username
    AIRFLOW_ADMIN_PASSWORD = random_password.airflow_admin.result
    POSTGRES_USER          = "airflow"
    POSTGRES_PASSWORD      = random_password.airflow_postgres.result
  }
}

resource "aws_secretsmanager_secret" "app" {
  name                    = "${local.name}/app"
  description             = "Credenciales del pipeline DW (RDS, DocumentDB, Redshift, Airflow)"
  recovery_window_in_days = 0 # borrado inmediato en destroy: permite recrear con el mismo nombre
}

resource "aws_secretsmanager_secret_version" "app" {
  secret_id     = aws_secretsmanager_secret.app.id
  secret_string = jsonencode(local.app_secret)
}
