locals {
  code_root = abspath("${path.module}/..")

  # Solo se recorren las carpetas que corren en la EC2. Recorrer "**" incluiría terraform/,
  # donde aparecen el plan y el state entre plan y apply, y fileset fallaría con
  # "inconsistent result" en el apply.
  code_files = toset([
    for file in flatten([for dir in ["airflow", "sql", "src"] : fileset(local.code_root, "${dir}/**")]) : file
    if !can(regex("(^|/)(\\.env|__pycache__|\\.pytest_cache|\\.ruff_cache)(/|$)", file))
  ])

  code_version = substr(sha1(join("", [for file in sort(tolist(local.code_files)) : filemd5("${local.code_root}/${file}")])), 0, 12)
  code_prefix  = "artifacts/code/dl"

  glue_database_arn = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lake.name}"
  glue_tables_arn   = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lake.name}/*"
  glue_catalog_arn  = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog"
}

resource "aws_s3_object" "code" {
  for_each = local.code_files

  bucket = aws_s3_bucket.lake.id
  key    = "${local.code_prefix}/${each.value}"
  source = "${local.code_root}/${each.value}"
  etag   = filemd5("${local.code_root}/${each.value}")
}

# Permisos de la carga de trabajo, además de S3 y del secreto (que da el módulo).
data "aws_iam_policy_document" "workload" {
  statement {
    sid = "Comprehend"
    actions = [
      "comprehend:DetectEntities",
      "comprehend:DetectKeyPhrases",
      "comprehend:DetectDominantLanguage",
    ]
    resources = ["*"] # Comprehend no admite ARN de recurso para la detección en tiempo real
  }

  statement {
    sid = "Athena"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.lake.arn]
  }

  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:BatchUpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
    ]
    resources = [local.glue_catalog_arn, local.glue_database_arn, local.glue_tables_arn]
  }
}

module "airflow" {
  source = "../../modules/ec2-airflow"

  name          = local.name
  region        = var.aws_region
  vpc_id        = module.network.vpc_id
  subnet_id     = module.network.public_subnet_ids[0]
  instance_type = var.airflow_instance_type
  allowed_cidrs = var.allowed_cidrs
  key_name      = var.key_name

  bucket_name    = aws_s3_bucket.lake.id
  bucket_arn     = aws_s3_bucket.lake.arn
  code_s3_prefix = local.code_prefix
  code_version   = local.code_version
  app_dir        = "/opt/de-aws/dl"
  compose_file   = "airflow/docker-compose.yml"
  secret_arn     = aws_secretsmanager_secret_version.app.secret_id

  environment = {
    AWS_DEFAULT_REGION = var.aws_region
    LAKE_BUCKET        = aws_s3_bucket.lake.id
    GLUE_DATABASE      = aws_glue_catalog_database.lake.name
    ATHENA_WORKGROUP   = aws_athena_workgroup.lake.name
    PIPELINE_SCHEDULE  = var.pipeline_schedule
    AIRFLOW_UID        = "50000"
    AIRFLOW_UI_PORT    = "8080"
  }

  extra_policy_json              = data.aws_iam_policy_document.workload.json
  existing_instance_profile_name = var.existing_instance_profile_name
  tags                           = local.tags
}
