# El data lake no tiene bases de datos propias: el secreto solo guarda las credenciales
# internas de Airflow. Los permisos sobre AWS vienen del rol de la instancia.
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

resource "random_id" "fernet" {
  byte_length = 32
}

resource "aws_secretsmanager_secret" "app" {
  name                    = "${local.name}/app"
  description             = "Credenciales internas de Airflow del data lake"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "app" {
  secret_id = aws_secretsmanager_secret.app.id
  secret_string = jsonencode({
    AIRFLOW_FERNET_KEY     = "${random_id.fernet.b64_url}="
    AIRFLOW_SECRET_KEY     = random_password.airflow_secret_key.result
    AIRFLOW_ADMIN_USERNAME = var.airflow_admin_username
    AIRFLOW_ADMIN_PASSWORD = random_password.airflow_admin.result
    POSTGRES_USER          = "airflow"
    POSTGRES_PASSWORD      = random_password.airflow_postgres.result
  })
}
