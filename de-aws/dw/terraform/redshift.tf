# Data warehouse: Redshift Serverless.
#   namespace = datos (base, usuarios, roles IAM asociados)
#   workgroup = cómputo (RPU, red, acceso público)
# Redshift lee S3 con un rol IAM propio: el COPY usa IAM_ROLE, nunca llaves de acceso.

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

# Mínimo privilegio: solo lectura y solo del prefijo raw/.
data "aws_iam_policy_document" "redshift_s3_read" {
  statement {
    sid       = "ListRawPrefix"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }

  statement {
    sid       = "ReadRawObjects"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/raw/*"]
  }
}

resource "aws_iam_role" "redshift" {
  count = var.existing_redshift_role_arn == null ? 1 : 0

  name               = "${local.name}-redshift-s3"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

resource "aws_iam_role_policy" "redshift_s3_read" {
  count = var.existing_redshift_role_arn == null ? 1 : 0

  name   = "read-raw-prefix"
  role   = aws_iam_role.redshift[0].id
  policy = data.aws_iam_policy_document.redshift_s3_read.json
}

locals {
  redshift_role_arn = coalesce(var.existing_redshift_role_arn, try(aws_iam_role.redshift[0].arn, null))
  quicksight_cidr   = lookup(var.quicksight_cidrs, var.aws_region, null)
}

resource "aws_security_group" "redshift" {
  name        = "${local.name}-redshift"
  description = "Redshift desde Airflow y desde QuickSight"
  vpc_id      = module.network.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "redshift_from_airflow" {
  security_group_id            = aws_security_group.redshift.id
  referenced_security_group_id = module.airflow.security_group_id
  from_port                    = 5439
  to_port                      = 5439
  ip_protocol                  = "tcp"
  description                  = "Redshift desde Airflow y dbt"
}

resource "aws_vpc_security_group_ingress_rule" "redshift_from_quicksight" {
  count = var.redshift_publicly_accessible && local.quicksight_cidr != null ? 1 : 0

  security_group_id = aws_security_group.redshift.id
  cidr_ipv4         = local.quicksight_cidr
  from_port         = 5439
  to_port           = 5439
  ip_protocol       = "tcp"
  description       = "Redshift desde QuickSight"
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = "${local.name}-ns"
  db_name             = var.redshift_database
  admin_username      = var.redshift_admin_username
  admin_user_password = random_password.redshift.result

  iam_roles            = [local.redshift_role_arn]
  default_iam_role_arn = local.redshift_role_arn # permite `COPY ... IAM_ROLE default`

  lifecycle {
    # La API devuelve los roles en otro formato; evita diferencias falsas en cada plan.
    ignore_changes = [iam_roles]
  }
}

resource "aws_redshiftserverless_workgroup" "this" {
  workgroup_name = "${local.name}-wg"
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  base_capacity  = var.redshift_base_capacity

  # Público: subredes con Internet Gateway, filtrado por el security group.
  # Privado: subredes privadas, solo alcanzable desde la VPC.
  subnet_ids          = var.redshift_publicly_accessible ? module.network.public_subnet_ids : module.network.private_subnet_ids
  security_group_ids  = [aws_security_group.redshift.id]
  publicly_accessible = var.redshift_publicly_accessible
}

# Control de costos: si el consumo diario supera el límite, el workgroup se desactiva.
resource "aws_redshiftserverless_usage_limit" "daily" {
  resource_arn  = aws_redshiftserverless_workgroup.this.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_daily_rpu_hours_limit
  period        = "daily"
  breach_action = "deactivate"
}
