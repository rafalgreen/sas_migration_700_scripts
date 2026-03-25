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
# Secrets Manager access (for JDBC credentials)
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "glue_secrets" {
  count = (var.enable_db2 || var.enable_mssql) ? 1 : 0
  name  = "secrets-manager-access"
  role  = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
        ]
        Resource = "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:${local.project_prefix}/*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Glue JDBC Connections
# -----------------------------------------------------------------------------

resource "aws_glue_connection" "mssql" {
  count           = (var.enable_mssql && var.enable_vpc) ? 1 : 0
  name            = "${local.project_prefix}-mssql"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:sqlserver://${var.mssql_host}:${var.mssql_port};databaseName=${var.mssql_database}"
    USERNAME            = var.mssql_username
    PASSWORD            = var.mssql_password
  }

  physical_connection_requirements {
    availability_zone      = var.availability_zones[0]
    subnet_id              = aws_subnet.private[0].id
    security_group_id_list = [aws_security_group.glue[0].id]
  }
}

resource "aws_glue_connection" "db2" {
  count           = (var.enable_db2 && var.enable_vpc) ? 1 : 0
  name            = "${local.project_prefix}-db2"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:db2://${var.db2_host}:${var.db2_port}/${var.db2_database}"
    USERNAME            = var.db2_username
    PASSWORD            = var.db2_password
  }

  physical_connection_requirements {
    availability_zone      = var.availability_zones[0]
    subnet_id              = aws_subnet.private[0].id
    security_group_id_list = [aws_security_group.glue[0].id]
  }
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

  connections = compact([
    var.enable_mssql && var.enable_vpc ? aws_glue_connection.mssql[0].name : "",
    var.enable_db2 && var.enable_vpc ? aws_glue_connection.db2[0].name : "",
  ])

  default_arguments = merge(
    {
      "--job-language"                    = "python"
      "--enable-metrics"                  = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-spark-ui"                 = "true"
      "--spark-event-logs-path"           = "s3://${aws_s3_bucket.scripts.id}/spark-logs/"
      "--extra-py-files"                  = "s3://${aws_s3_bucket.scripts.id}/common/common_utils.zip"
      "--TempDir"                         = "s3://${aws_s3_bucket.scripts.id}/temp/"
    },
    (var.enable_mssql || var.enable_db2) ? {
      "--extra-jars" = "s3://${aws_s3_bucket.scripts.id}/jars/mssql-jdbc-12.4.2.jcc.jar,s3://${aws_s3_bucket.scripts.id}/jars/db2jcc4.jar"
    } : {},
  )
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
