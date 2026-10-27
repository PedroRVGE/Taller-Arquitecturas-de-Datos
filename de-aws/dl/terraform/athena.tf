# Workgroup de Athena: fija la ubicación de resultados, el cifrado y un tope de bytes
# escaneados por consulta (Athena cobra por TB escaneado).
resource "aws_athena_workgroup" "lake" {
  name          = local.name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Cada archivo de sql/athena (excepto la carpeta de validaciones del DAG) queda guardado
# como consulta con nombre en la consola de Athena.
locals {
  sql_root  = abspath("${path.module}/../sql/athena")
  sql_files = toset([for file in fileset(local.sql_root, "*.sql") : file])
}

resource "aws_athena_named_query" "lake" {
  for_each = local.sql_files

  name        = trimsuffix(each.value, ".sql")
  workgroup   = aws_athena_workgroup.lake.id
  database    = aws_glue_catalog_database.lake.name
  query       = file("${local.sql_root}/${each.value}")
  description = "Consulta del taller: ${each.value}"
}
