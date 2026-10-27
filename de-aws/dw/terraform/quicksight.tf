# Fuente de datos de QuickSight hacia Redshift (opcional).
# Requisito previo manual: suscribirse a QuickSight en la consola (una vez por cuenta).
# El dataset y el dashboard se crean en la interfaz (docs/quicksight.md).
resource "aws_quicksight_data_source" "redshift" {
  count = var.quicksight_principal_arn == null ? 0 : 1

  data_source_id = "${local.name}-redshift"
  name           = "${local.name} Redshift marts"
  type           = "REDSHIFT"

  parameters {
    redshift {
      host     = aws_redshiftserverless_workgroup.this.endpoint[0].address
      port     = aws_redshiftserverless_workgroup.this.port
      database = var.redshift_database
    }
  }

  credentials {
    credential_pair {
      username = var.redshift_admin_username
      password = random_password.redshift.result
    }
  }

  ssl_properties {
    disable_ssl = false
  }

  permission {
    principal = var.quicksight_principal_arn
    actions = [
      "quicksight:DescribeDataSource",
      "quicksight:DescribeDataSourcePermissions",
      "quicksight:PassDataSource",
      "quicksight:UpdateDataSource",
      "quicksight:DeleteDataSource",
      "quicksight:UpdateDataSourcePermissions",
    ]
  }
}
