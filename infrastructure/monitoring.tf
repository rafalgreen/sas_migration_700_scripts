# -----------------------------------------------------------------------------
# SNS Alarm Topic
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "alarms" {
  name         = "${local.project_prefix}-glue-alarms"
  display_name = "SAS Migration Glue Job Alarms"
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "migration" {
  dashboard_name = "${local.project_prefix}-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# SAS Migration Pipeline Dashboard"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 1
        width  = 12
        height = 6
        properties = {
          title  = "Glue Job Runs"
          region = local.region
          metrics = [
            ["Glue", "glue.driver.aggregate.numCompletedStages", "Type", "count", { stat = "Sum", period = 300 }]
          ]
          view = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 1
        width  = 12
        height = 6
        properties = {
          title  = "Glue Job Duration"
          region = local.region
          metrics = [
            ["Glue", "glue.driver.aggregate.elapsedTime", "Type", "gauge", { stat = "Average", period = 300 }]
          ]
          view = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 7
        width  = 12
        height = 6
        properties = {
          title  = "Glue Job Errors"
          region = local.region
          metrics = [
            ["Glue", "glue.driver.aggregate.numFailedTasks", "Type", "count", { stat = "Sum", period = 300 }]
          ]
          view = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 7
        width  = 12
        height = 6
        properties = {
          title  = "Step Functions Executions"
          region = local.region
          metrics = [
            ["AWS/States", "ExecutionsStarted", { stat = "Sum", period = 300 }],
            ["AWS/States", "ExecutionsFailed", { stat = "Sum", period = 300 }],
            ["AWS/States", "ExecutionsSucceeded", { stat = "Sum", period = 300 }]
          ]
          view = "timeSeries"
        }
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "glue_failures" {
  alarm_name          = "${local.project_prefix}-glue-job-failures"
  alarm_description   = "Alert when any Glue job task fails"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1

  namespace   = "Glue"
  metric_name = "glue.driver.aggregate.numFailedTasks"
  statistic   = "Sum"
  period      = 300
  dimensions = {
    Type = "count"
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "sfn_failures" {
  alarm_name          = "${local.project_prefix}-sfn-failures"
  alarm_description   = "Alert when Step Functions execution fails"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1

  namespace   = "AWS/States"
  metric_name = "ExecutionsFailed"
  statistic   = "Sum"
  period      = 300

  alarm_actions = [aws_sns_topic.alarms.arn]
}
