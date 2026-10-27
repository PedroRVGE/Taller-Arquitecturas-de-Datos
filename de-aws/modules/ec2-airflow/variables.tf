variable "name" {
  description = "Prefijo de nombres (por ejemplo de-aws-dev-dw)."
  type        = string
}

variable "region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_id" {
  description = "Subred pública donde vive la instancia."
  type        = string
}

variable "instance_type" {
  description = "Airflow + PostgreSQL + dbt necesitan unos 6 GB de RAM; t3.large tiene 8 GB."
  type        = string
  default     = "t3.large"
}

variable "ami_id" {
  description = "AMI fija opcional. Por defecto, la Amazon Linux 2023 más reciente."
  type        = string
  default     = null
}

variable "root_volume_gb" {
  type    = number
  default = 30
}

variable "allowed_cidrs" {
  description = "CIDR que pueden abrir la UI de Airflow (8080) y SSH. Usa tu IP pública con /32. Vacío = solo por SSM."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.allowed_cidrs : can(cidrnetmask(cidr)) && cidr != "0.0.0.0/0"])
    error_message = "allowed_cidrs debe contener CIDR válidos y nunca 0.0.0.0/0."
  }
}

variable "key_name" {
  description = "Par de llaves EC2 opcional. Si es null no se abre el puerto 22 (se entra por SSM)."
  type        = string
  default     = null
}

variable "bucket_name" {
  type = string
}

variable "bucket_arn" {
  type = string
}

variable "code_s3_prefix" {
  description = "Prefijo del bucket donde Terraform publica el código (sin barra final)."
  type        = string
}

variable "app_dir" {
  description = "Directorio de la aplicación en la instancia."
  type        = string
}

variable "compose_file" {
  description = "Ruta del docker-compose.yml relativa a app_dir."
  type        = string
  default     = "airflow/docker-compose.yml"
}

variable "secret_arn" {
  description = "Secreto JSON de Secrets Manager; cada clave se vuelve una variable del .env."
  type        = string
}

variable "environment" {
  description = "Variables no secretas que se agregan al .env de la aplicación."
  type        = map(string)
  default     = {}
}

variable "extra_policy_json" {
  description = "Política IAM adicional para la carga de trabajo (Comprehend, Athena, Glue...)."
  type        = string
  default     = null
}

variable "existing_instance_profile_name" {
  description = "Instance profile existente (por ejemplo LabInstanceProfile en AWS Academy). Si se define, no se crean roles."
  type        = string
  default     = null
}

variable "code_version" {
  description = "Huella del código publicado; se guarda como etiqueta y crea la dependencia con los objetos de S3."
  type        = string
  default     = "none"
}

variable "docker_compose_version" {
  type    = string
  default = "v2.29.7"
}

variable "docker_buildx_version" {
  type    = string
  default = "v0.19.3"
}

variable "tags" {
  type    = map(string)
  default = {}
}
