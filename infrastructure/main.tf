terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Configure per-environment via -backend-config or environments/*.backend.hcl
    # bucket = "sas-migration-tfstate"
    # key    = "dev/terraform.tfstate"
    # region = "eu-west-1"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "sas-migration"
      Environment = var.env
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id     = data.aws_caller_identity.current.account_id
  region         = data.aws_region.current.name
  project_prefix = "sas-migration-${var.env}"
  name_prefix    = replace(local.project_prefix, "-", "_")
}
