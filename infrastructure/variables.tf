variable "env" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "eu-west-1"
}

variable "glue_worker_type" {
  description = "Worker type for Glue jobs"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers for Glue jobs"
  type        = number
  default     = 2
}

variable "glue_job_timeout" {
  description = "Timeout in minutes for Glue jobs"
  type        = number
  default     = 120
}

variable "sfn_timeout_hours" {
  description = "Timeout in hours for Step Functions pipelines"
  type        = number
  default     = 4
}

variable "lambda_memory_size" {
  description = "Memory size in MB for Lambda SAS jobs (max 10240)"
  type        = number
  default     = 2048
}

# -----------------------------------------------------------------------------
# Database connectivity
# -----------------------------------------------------------------------------

variable "enable_db2" {
  description = "Enable DB2 data source (creates Secrets Manager secret + Glue Connection)"
  type        = bool
  default     = false
}

variable "db2_host" {
  description = "DB2 server hostname"
  type        = string
  default     = ""
  sensitive   = true
}

variable "db2_port" {
  description = "DB2 server port"
  type        = number
  default     = 50000
}

variable "db2_database" {
  description = "DB2 database name"
  type        = string
  default     = ""
}

variable "db2_username" {
  description = "DB2 username"
  type        = string
  default     = ""
  sensitive   = true
}

variable "db2_password" {
  description = "DB2 password"
  type        = string
  default     = ""
  sensitive   = true
}

variable "enable_mssql" {
  description = "Enable MSSQL Server data source (creates Secrets Manager secret + Glue Connection)"
  type        = bool
  default     = false
}

variable "mssql_host" {
  description = "MSSQL Server hostname"
  type        = string
  default     = ""
  sensitive   = true
}

variable "mssql_port" {
  description = "MSSQL Server port"
  type        = number
  default     = 1433
}

variable "mssql_database" {
  description = "MSSQL database name"
  type        = string
  default     = ""
}

variable "mssql_username" {
  description = "MSSQL username"
  type        = string
  default     = ""
  sensitive   = true
}

variable "mssql_password" {
  description = "MSSQL password"
  type        = string
  default     = ""
  sensitive   = true
}

# -----------------------------------------------------------------------------
# VPC / Networking
# -----------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC (needed for database connectivity)"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (at least 2 for HA)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "availability_zones" {
  description = "Availability zones for subnets"
  type        = list(string)
  default     = ["eu-west-1a", "eu-west-1b"]
}

variable "enable_vpc" {
  description = "Create VPC resources (required for DB2/MSSQL connectivity)"
  type        = bool
  default     = false
}
