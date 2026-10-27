variable "project_name" {
  description = "Prefijo de todos los nombres."
  type        = string
  default     = "de-aws"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "owner" {
  description = "Etiqueta Owner (tu usuario o correo)."
  type        = string
  default     = "estudiante"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "allowed_cidrs" {
  description = "Tu IP pública con /32 para abrir la UI de Airflow. Vacío = acceso solo por SSM (port forwarding)."
  type        = list(string)
  default     = []
}

variable "key_name" {
  description = "Par de llaves EC2 opcional para SSH."
  type        = string
  default     = null
}

# ------------------------------------------------------------- EC2 / Airflow
variable "airflow_instance_type" {
  type    = string
  default = "t3.large"
}

variable "airflow_admin_username" {
  type    = string
  default = "airflow"
}

# ------------------------------------------------------------- RDS MySQL
variable "rds_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "mysql_engine_version" {
  description = "Versión mayor de MySQL en RDS (la versión local usaba 8.4)."
  type        = string
  default     = "8.4"
}

variable "mysql_database" {
  type    = string
  default = "ecommerce"
}

variable "mysql_username" {
  type    = string
  default = "etl_admin"
}

# ------------------------------------------------------------- DocumentDB
variable "docdb_instance_class" {
  description = "db.t3.medium es la clase más pequeña de DocumentDB."
  type        = string
  default     = "db.t3.medium"
}

variable "docdb_engine_version" {
  type    = string
  default = "5.0.0"
}

variable "docdb_username" {
  type    = string
  default = "docdb_admin"
}

variable "mongo_database" {
  type    = string
  default = "ecommerce"
}

# ------------------------------------------------------------- Redshift Serverless
variable "redshift_database" {
  type    = string
  default = "ecommerce_dw"
}

variable "redshift_admin_username" {
  type    = string
  default = "dw_admin"
}

variable "redshift_base_capacity" {
  description = "RPU base del workgroup. 8 es el mínimo y el más barato."
  type        = number
  default     = 8

  validation {
    condition     = var.redshift_base_capacity >= 8 && var.redshift_base_capacity % 8 == 0
    error_message = "La capacidad base debe ser múltiplo de 8 y al menos 8 RPU."
  }
}

variable "redshift_daily_rpu_hours_limit" {
  description = "Límite diario de RPU-hora; al superarlo el workgroup se desactiva (control de costos)."
  type        = number
  default     = 16
}

variable "redshift_publicly_accessible" {
  description = "QuickSight Standard llega a Redshift por su endpoint público, limitado al rango IP de QuickSight. Con false, Redshift queda privado (requiere VPC connection de QuickSight Enterprise)."
  type        = bool
  default     = true
}

variable "quicksight_cidrs" {
  description = "Rangos IP de QuickSight por región (documentación de AWS: 'IP address ranges for Amazon QuickSight')."
  type        = map(string)
  default = {
    "us-east-1" = "52.23.63.224/27"
    "us-east-2" = "52.15.247.160/27"
    "us-west-2" = "54.70.204.128/27"
    "eu-west-1" = "52.210.255.224/27"
  }
}

variable "quicksight_principal_arn" {
  description = "ARN del usuario de QuickSight (arn:aws:quicksight:REGION:CUENTA:user/default/USUARIO). Si es null no se crea la fuente de datos de QuickSight."
  type        = string
  default     = null
}

# ------------------------------------------------------------- AWS Academy
variable "existing_instance_profile_name" {
  description = "AWS Academy no permite crear roles: usa \"LabInstanceProfile\"."
  type        = string
  default     = null
}

variable "existing_redshift_role_arn" {
  description = "AWS Academy: ARN de LabRole para que Redshift lea S3."
  type        = string
  default     = null
}
