# -----------------------------------------------------------------------------
# Glue Job IAM Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue" {
  name = "${local.project_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "s3-access"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetBucketLocation",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetBucketLocation",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_logs" {
  name = "cloudwatch-logs"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:*:*:/aws-glue/*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_catalog" {
  name = "glue-catalog"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
        ]
        Resource = "*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Bedrock access — used by the SAS-to-PySpark conversion tooling.
# Attach to CI/CD roles or developer roles that run `sas-convert`.
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "glue_bedrock" {
  name = "bedrock-invoke"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
        ]
        Resource = "arn:aws:bedrock:${data.aws_region.current.name}::foundation-model/*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Glue Job
# -----------------------------------------------------------------------------

resource "aws_glue_job" "etl_sample" {
  name     = "${local.project_prefix}-etl-sample"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.scripts.id}/jobs/etl-sample.py"
  }

  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = 1
  timeout           = var.glue_job_timeout

  default_arguments = {
    "--job-language"                        = "python"
    "--enable-metrics"                      = "true"
    "--enable-continuous-cloudwatch-log"     = "true"
    "--enable-spark-ui"                     = "true"
    "--spark-event-logs-path"               = "s3://${aws_s3_bucket.scripts.id}/spark-logs/"
    "--extra-py-files"                      = "s3://${aws_s3_bucket.scripts.id}/common/common_utils.zip"
    "--TempDir"                             = "s3://${aws_s3_bucket.scripts.id}/temp/"
  }
}

# -----------------------------------------------------------------------------
# Glue Crawlers
# -----------------------------------------------------------------------------

resource "aws_glue_crawler" "raw_zone" {
  name          = "${local.project_prefix}-crawler-raw-zone"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.data.id}/raw/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "processed_zone" {
  name          = "${local.project_prefix}-crawler-processed-zone"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.data.id}/processed/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
