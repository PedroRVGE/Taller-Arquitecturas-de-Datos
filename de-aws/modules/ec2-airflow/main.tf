# EC2 que ejecuta Airflow (y dbt en la parte DW) con Docker Compose.
# - Rol IAM propio (o el LabRole de AWS Academy mediante existing_instance_profile_name).
# - IMDSv2 obligatorio con hop limit 2: los contenedores Docker necesitan un salto extra
#   para llegar al servicio de metadatos y obtener las credenciales del rol.
# - El código llega desde S3 y los secretos desde Secrets Manager; nada sensible
#   viaja en user_data.

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

locals {
  create_role  = var.existing_instance_profile_name == null
  profile_name = local.create_role ? aws_iam_instance_profile.this[0].name : var.existing_instance_profile_name

  env_lines = join("\n", [for key in sort(keys(var.environment)) : "${key}=${var.environment[key]}"])
}

# ---------------------------------------------------------------- red
resource "aws_security_group" "this" {
  name        = "${var.name}-airflow"
  description = "Airflow EC2: UI 8080 y SSH solo desde las IP permitidas"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = "${var.name}-airflow" })
}

resource "aws_vpc_security_group_ingress_rule" "airflow_ui" {
  for_each = toset(var.allowed_cidrs)

  security_group_id = aws_security_group.this.id
  description       = "Airflow UI"
  cidr_ipv4         = each.value
  from_port         = 8080
  to_port           = 8080
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  for_each = var.key_name == null ? toset([]) : toset(var.allowed_cidrs)

  security_group_id = aws_security_group.this.id
  description       = "SSH"
  cidr_ipv4         = each.value
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.this.id
  description       = "Salida a internet (imagenes Docker, paquetes, RSS) y a la VPC"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# ---------------------------------------------------------------- IAM
data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "base" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.bucket_arn]
  }

  statement {
    sid       = "ReadWriteObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${var.bucket_arn}/*"]
  }

  statement {
    sid       = "ReadAppSecret"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [var.secret_arn]
  }
}

resource "aws_iam_role" "this" {
  count = local.create_role ? 1 : 0

  name               = "${var.name}-airflow-ec2"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
  tags               = var.tags
}

# Una sola política: permisos base + los de la carga de trabajo. Combinar documentos evita
# un `count` que dependa de extra_policy_json, cuyo valor solo se conoce tras el apply
# (por ejemplo, el ARN del workgroup de Athena) y que Terraform no podría planear.
data "aws_iam_policy_document" "combined" {
  source_policy_documents = compact([data.aws_iam_policy_document.base.json, var.extra_policy_json])
}

resource "aws_iam_role_policy" "workload" {
  count = local.create_role ? 1 : 0

  name   = "s3-secret-and-workload"
  role   = aws_iam_role.this[0].id
  policy = data.aws_iam_policy_document.combined.json
}

# Session Manager: entrar a la instancia sin abrir el puerto 22 ni manejar llaves SSH.
resource "aws_iam_role_policy_attachment" "ssm" {
  count = local.create_role ? 1 : 0

  role       = aws_iam_role.this[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "this" {
  count = local.create_role ? 1 : 0

  name = "${var.name}-airflow-ec2"
  role = aws_iam_role.this[0].name
}

# ---------------------------------------------------------------- configuración
# Variables no secretas en S3: cambiarlas no altera user_data (no recrea la instancia);
# basta con `terraform apply` y volver a ejecutar de-aws-deploy en la instancia.
resource "aws_s3_object" "config" {
  bucket       = var.bucket_name
  key          = "artifacts/config/${var.name}.env"
  content      = "${local.env_lines}\n"
  content_type = "text/plain"
}

# ---------------------------------------------------------------- instancia
resource "aws_instance" "this" {
  ami                         = coalesce(var.ami_id, data.aws_ami.al2023.id)
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.this.id]
  iam_instance_profile        = local.profile_name
  associate_public_ip_address = true
  key_name                    = var.key_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    app_dir         = var.app_dir
    bucket          = var.bucket_name
    code_prefix     = var.code_s3_prefix
    compose_file    = var.compose_file
    secret_arn      = var.secret_arn
    region          = var.region
    config_key      = aws_s3_object.config.key
    compose_version = var.docker_compose_version
    buildx_version  = var.docker_buildx_version
  })
  # Cambiar el script de arranque recrea la instancia: user_data solo corre en el primer arranque.
  user_data_replace_on_change = true

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2
    http_put_response_hop_limit = 2          # contenedores -> host -> IMDS
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    encrypted             = true
    delete_on_termination = true
  }

  tags = merge(var.tags, {
    Name          = "${var.name}-airflow"
    CodeVersion   = var.code_version
    ConfigVersion = substr(md5(local.env_lines), 0, 12)
  })

  lifecycle {
    # Una AMI nueva publicada por Amazon no debe recrear la instancia en cada plan.
    ignore_changes = [ami]
  }
}
