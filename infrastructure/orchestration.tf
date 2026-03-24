# -----------------------------------------------------------------------------
# Step Functions IAM Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "sfn" {
  name = "${local.project_prefix}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "sfn_glue" {
  name = "glue-access"
  role = aws_iam_role.sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "sfn_lambda" {
  name = "lambda-invoke"
  role = aws_iam_role.sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.project_prefix}-*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "sfn_sns" {
  name = "sns-publish"
  role = aws_iam_role.sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = "*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Sample ETL Pipeline (Extract -> Transform -> Load)
# -----------------------------------------------------------------------------

resource "aws_sfn_state_machine" "sample_etl" {
  name     = "${local.project_prefix}-sample-etl"
  role_arn = aws_iam_role.sfn.arn

  definition = jsonencode({
    Comment = "Sample ETL pipeline: Extract -> Transform -> Load"
    StartAt = "ExtractJob"
    States = {
      ExtractJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "${local.project_prefix}-extract"
          Arguments = {
            "--JOB_NAME"       = "${local.project_prefix}-extract"
            "--source_path.$"  = "$.source_path"
            "--target_path.$"  = "$.target_path"
          }
        }
        ResultPath = "$.extract_result"
        Next       = "TransformJob"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "PipelineFailure"
            ResultPath  = "$.error"
          }
        ]
      }

      TransformJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "${local.project_prefix}-transform"
          Arguments = {
            "--JOB_NAME"      = "${local.project_prefix}-transform"
            "--input_path.$"  = "$.target_path"
          }
        }
        ResultPath = "$.transform_result"
        Next       = "LoadJob"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "PipelineFailure"
            ResultPath  = "$.error"
          }
        ]
      }

      LoadJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "${local.project_prefix}-load"
          Arguments = {
            "--JOB_NAME" = "${local.project_prefix}-load"
          }
        }
        ResultPath = "$.load_result"
        Next       = "PipelineSuccess"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "PipelineFailure"
            ResultPath  = "$.error"
          }
        ]
      }

      PipelineSuccess = {
        Type = "Succeed"
      }

      PipelineFailure = {
        Type  = "Fail"
        Cause = "A Glue job failed"
        Error = "GlueJobError"
      }
    }
  })

  tracing_configuration {
    enabled = true
  }
}
