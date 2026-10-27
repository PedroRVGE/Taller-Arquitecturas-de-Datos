terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.80, < 7.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State local por defecto (un estudiante). Para trabajar en equipo copia
  # backend.tf.example a backend.tf, crea el bucket del state y ejecuta
  # `terraform init -migrate-state`.
}

provider "aws" {
  region = var.aws_region

  # Todas las etiquetas comunes se aplican a cada recurso que las soporte.
  default_tags {
    tags = local.tags
  }
}
