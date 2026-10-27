# Publica el código de dw/ (sin terraform/ ni artefactos locales) en S3 y crea la EC2
# que lo descarga y levanta Airflow con Docker Compose.
locals {
  code_root = abspath("${path.module}/..")

  code_files = toset([
    for file in fileset(local.code_root, "**") : file
    if !startswith(file, "terraform/")
    && !startswith(file, "docs/")
    && !can(regex("(^|/)(\\.env|__pycache__|target|dbt_packages|logs|tests|\\.pytest_cache|\\.ruff_cache)(/|$)", file))
  ])

  code_version = substr(sha1(join("", [for file in sort(tolist(local.code_files)) : filemd5("${local.code_root}/${file}")])), 0, 12)
  code_prefix  = "artifacts/code/dw"
}

resource "aws_s3_object" "code" {
  for_each = local.code_files

  bucket = aws_s3_bucket.data.id
  key    = "${local.code_prefix}/${each.value}"
  source = "${local.code_root}/${each.value}"
  etag   = filemd5("${local.code_root}/${each.value}")
}

data "aws_iam_policy_document" "airflow_workload" {
  statement {
    sid       = "DescribeRedshiftServerless"
    actions   = ["redshift-serverless:GetWorkgroup", "redshift-serverless:GetNamespace"]
    resources = ["*"]
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

  bucket_name    = aws_s3_bucket.data.id
  bucket_arn     = aws_s3_bucket.data.arn
  code_s3_prefix = local.code_prefix
  code_version   = local.code_version
  app_dir        = "/opt/de-aws/dw"
  compose_file   = "airflow/docker-compose.yml"

  # El ARN de la versión crea una dependencia implícita: la EC2 arranca cuando
  # las bases ya existen y el secreto tiene sus endpoints.
  secret_arn = aws_secretsmanager_secret_version.app.secret_id

  environment = {
    AWS_DEFAULT_REGION    = var.aws_region
    DW_BUCKET             = aws_s3_bucket.data.id
    REDSHIFT_IAM_ROLE_ARN = local.redshift_role_arn
    AIRFLOW_UID           = "50000"
    AIRFLOW_UI_PORT       = "8080"
    PIPELINE_TIMEZONE     = "America/Bogota"
  }

  extra_policy_json              = data.aws_iam_policy_document.airflow_workload.json
  existing_instance_profile_name = var.existing_instance_profile_name
  tags                           = local.tags
}
