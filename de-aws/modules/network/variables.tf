variable "name" {
  description = "Prefijo de nombres de los recursos de red."
  type        = string
}

variable "region" {
  description = "Región de AWS; se usa para el nombre del servicio del endpoint de S3."
  type        = string
}

variable "cidr_block" {
  description = "Rango CIDR de la VPC."
  type        = string
  default     = "10.20.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.cidr_block))
    error_message = "cidr_block debe ser un CIDR IPv4 válido, por ejemplo 10.20.0.0/16."
  }
}

variable "az_count" {
  description = "Zonas de disponibilidad. Redshift Serverless exige al menos 3; RDS y DocumentDB, 2."
  type        = number
  default     = 3

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 4
    error_message = "az_count debe estar entre 2 y 4."
  }
}

variable "tags" {
  description = "Etiquetas adicionales."
  type        = map(string)
  default     = {}
}
