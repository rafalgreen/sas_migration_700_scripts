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
