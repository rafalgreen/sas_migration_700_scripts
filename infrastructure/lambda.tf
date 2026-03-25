# -----------------------------------------------------------------------------
# Lambda IAM Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lambda" {
  name = "${local.project_prefix}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  name = "s3-access"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
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

resource "aws_iam_role_policy" "lambda_glue_catalog" {
  name = "glue-catalog-read"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_secrets" {
  count = (var.enable_db2 || var.enable_mssql) ? 1 : 0
  name  = "secrets-manager-access"
  role  = aws_iam_role.lambda.id

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

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  count      = var.enable_vpc ? 1 : 0
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# -----------------------------------------------------------------------------
# Lambda Layer — pandas + s3fs
# -----------------------------------------------------------------------------

resource "aws_lambda_layer_version" "pandas" {
  layer_name          = "${local.project_prefix}-pandas"
  description         = "pandas, numpy, s3fs, and pyarrow for SAS-migrated Lambda jobs"
  compatible_runtimes = ["python3.12"]

  # Build the layer zip as a separate step (CI or local):
  #   pip install pandas pyarrow s3fs -t python/lib/python3.12/site-packages/
  #   zip -r pandas_layer.zip python/
  filename = "${path.module}/layers/pandas_layer.zip"

  lifecycle {
    create_before_destroy = true
  }
}

# -----------------------------------------------------------------------------
# Lambda Layer — database drivers (pymssql, sqlalchemy, openpyxl)
# -----------------------------------------------------------------------------

resource "aws_lambda_layer_version" "db_drivers" {
  count               = (var.enable_db2 || var.enable_mssql) ? 1 : 0
  layer_name          = "${local.project_prefix}-db-drivers"
  description         = "pymssql, sqlalchemy, openpyxl for database + Excel access"
  compatible_runtimes = ["python3.12"]

  # Build: pip install pymssql sqlalchemy openpyxl -t python/lib/python3.12/site-packages/
  #        zip -r db_drivers_layer.zip python/
  filename = "${path.module}/layers/db_drivers_layer.zip"

  lifecycle {
    create_before_destroy = true
  }
}

# -----------------------------------------------------------------------------
# Lambda Function (placeholder — one function per migrated script)
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "sas_job" {
  function_name = "${local.project_prefix}-sas-job"
  role          = aws_iam_role.lambda.arn
  handler       = "handler.handler"
  runtime       = "python3.12"
  timeout       = 900 # 15 min maximum
  memory_size   = var.lambda_memory_size

  s3_bucket = aws_s3_bucket.scripts.id
  s3_key    = "lambda_jobs/handlers/handler.zip"

  layers = compact([
    aws_lambda_layer_version.pandas.arn,
    (var.enable_db2 || var.enable_mssql) ? aws_lambda_layer_version.db_drivers[0].arn : "",
  ])

  dynamic "vpc_config" {
    for_each = var.enable_vpc ? [1] : []
    content {
      subnet_ids         = aws_subnet.private[*].id
      security_group_ids = [aws_security_group.lambda[0].id]
    }
  }

  environment {
    variables = {
      S3_BUCKET  = aws_s3_bucket.data.id
      LOG_LEVEL  = "INFO"
      AWS_REGION = local.region
    }
  }
}
