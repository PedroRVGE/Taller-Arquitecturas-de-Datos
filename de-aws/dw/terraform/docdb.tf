# Fuente documental: Amazon DocumentDB (compatible con la API de MongoDB).
# Un clúster separa almacenamiento (compartido, replicado en 3 AZ) de instancias de cómputo.
resource "aws_docdb_subnet_group" "this" {
  name       = "${local.name}-docdb"
  subnet_ids = module.network.private_subnet_ids
}

resource "aws_security_group" "docdb" {
  name        = "${local.name}-docdb"
  description = "DocumentDB solo desde la EC2 de Airflow"
  vpc_id      = module.network.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "docdb_from_airflow" {
  security_group_id            = aws_security_group.docdb.id
  referenced_security_group_id = module.airflow.security_group_id
  from_port                    = 27017
  to_port                      = 27017
  ip_protocol                  = "tcp"
  description                  = "DocumentDB desde Airflow"
}

# TLS obligatorio (valor por defecto, explícito para documentarlo).
resource "aws_docdb_cluster_parameter_group" "this" {
  name   = "${local.name}-docdb50"
  family = "docdb5.0"

  parameter {
    name  = "tls"
    value = "enabled"
  }
}

resource "aws_docdb_cluster" "this" {
  cluster_identifier              = "${local.name}-docdb"
  engine                          = "docdb"
  engine_version                  = var.docdb_engine_version
  master_username                 = var.docdb_username
  master_password                 = random_password.docdb.result
  db_subnet_group_name            = aws_docdb_subnet_group.this.name
  vpc_security_group_ids          = [aws_security_group.docdb.id]
  db_cluster_parameter_group_name = aws_docdb_cluster_parameter_group.this.name
  storage_encrypted               = true

  backup_retention_period = 1
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true
}

resource "aws_docdb_cluster_instance" "this" {
  identifier         = "${local.name}-docdb-1"
  cluster_identifier = aws_docdb_cluster.this.id
  instance_class     = var.docdb_instance_class
  apply_immediately  = true
}
