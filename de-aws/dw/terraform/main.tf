locals {
  name = "${var.project_name}-${var.environment}-dw"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Part        = "data-warehouse"
    Owner       = var.owner
    ManagedBy   = "terraform"
  }
}

data "aws_caller_identity" "current" {}

# Sufijo aleatorio estable: los nombres de bucket son globales en todo AWS.
resource "random_id" "suffix" {
  byte_length = 3
}

module "network" {
  source = "../../modules/network"

  name       = local.name
  region     = var.aws_region
  cidr_block = var.vpc_cidr
  az_count   = 3 # Redshift Serverless exige subredes en 3 zonas
  tags       = local.tags
}
