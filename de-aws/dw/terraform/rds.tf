# Fuente relacional: RDS for MySQL en subredes privadas, solo accesible desde la EC2.
resource "aws_db_subnet_group" "mysql" {
  name       = "${local.name}-mysql"
  subnet_ids = module.network.private_subnet_ids
}

resource "aws_security_group" "mysql" {
  name        = "${local.name}-mysql"
  description = "MySQL solo desde la EC2 de Airflow"
  vpc_id      = module.network.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "mysql_from_airflow" {
  security_group_id            = aws_security_group.mysql.id
  referenced_security_group_id = module.airflow.security_group_id
  from_port                    = 3306
  to_port                      = 3306
  ip_protocol                  = "tcp"
  description                  = "MySQL desde Airflow"
}

resource "aws_db_instance" "mysql" {
  identifier     = "${local.name}-mysql"
  engine         = "mysql"
  engine_version = var.mysql_engine_version
  instance_class = var.rds_instance_class

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = var.mysql_database
  username = var.mysql_username
  password = random_password.mysql.result

  db_subnet_group_name   = aws_db_subnet_group.mysql.name
  vpc_security_group_ids = [aws_security_group.mysql.id]
  publicly_accessible    = false
  multi_az               = false # una sola AZ: suficiente para el curso y más barato

  backup_retention_period    = 1
  auto_minor_version_upgrade = true
  apply_immediately          = true

  # Entorno de curso: destroy sin snapshot final ni protección.
  skip_final_snapshot = true
  deletion_protection = false
}
