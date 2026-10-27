variable "project_name" {
  type    = string
  default = "de-aws"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "owner" {
  type    = string
  default = "estudiante"
}

variable "aws_region" {
  description = "Región con Comprehend disponible (us-east-1, us-east-2, us-west-2, eu-west-1...)."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  type    = string
  default = "10.30.0.0/16"
}

variable "allowed_cidrs" {
  description = "Tu IP pública con /32 para la UI de Airflow. Vacío = solo por túnel de SSM."
  type        = list(string)
  default     = []
}

variable "key_name" {
  type    = string
  default = null
}

variable "airflow_instance_type" {
  description = "Sin dbt ni bases de datos, 4 GB de RAM alcanzan."
  type        = string
  default     = "t3.medium"
}

variable "airflow_admin_username" {
  type    = string
  default = "airflow"
}

variable "glue_database_name" {
  description = "Base del Glue Data Catalog. Las consultas de sql/athena usan este nombre."
  type        = string
  default     = "news_lake"

  validation {
    condition     = can(regex("^[a-z0-9_]+$", var.glue_database_name))
    error_message = "Use solo minúsculas, números y guiones bajos."
  }
}

variable "athena_bytes_scanned_cutoff" {
  description = "Máximo de bytes que puede escanear una consulta (control de costos). 1 GB por defecto."
  type        = number
  default     = 1073741824
}

variable "pipeline_schedule" {
  description = "Cron del DAG durante el laboratorio (zona America/Bogota)."
  type        = string
  default     = "*/30 * * * *"
}

variable "existing_instance_profile_name" {
  description = "AWS Academy: \"LabInstanceProfile\"."
  type        = string
  default     = null
}
