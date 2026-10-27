locals {
  name = "${var.project_name}-${var.environment}-dl"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Part        = "data-lake"
    Owner       = var.owner
    ManagedBy   = "terraform"
  }
}

data "aws_caller_identity" "current" {}

resource "random_id" "suffix" {
  byte_length = 3
}

# El data lake no tiene bases privadas: basta con 2 zonas y la subred pública para la EC2.
module "network" {
  source = "../../modules/network"

  name       = local.name
  region     = var.aws_region
  cidr_block = var.vpc_cidr
  az_count   = 2
  tags       = local.tags
}
