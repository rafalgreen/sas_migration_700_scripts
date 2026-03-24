output "data_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data.id
}

output "scripts_bucket_name" {
  description = "Name of the Glue scripts S3 bucket"
  value       = aws_s3_bucket.scripts.id
}

output "glue_database_name" {
  description = "Name of the main Glue catalog database"
  value       = aws_glue_catalog_database.main.name
}

output "glue_role_arn" {
  description = "ARN of the Glue job IAM role"
  value       = aws_iam_role.glue.arn
}

output "sample_pipeline_arn" {
  description = "ARN of the sample ETL Step Functions state machine"
  value       = aws_sfn_state_machine.sample_etl.arn
}

output "alarm_topic_arn" {
  description = "ARN of the SNS alarm topic"
  value       = aws_sns_topic.alarms.arn
}

output "dashboard_name" {
  description = "Name of the CloudWatch dashboard"
  value       = aws_cloudwatch_dashboard.migration.dashboard_name
}

output "lambda_role_arn" {
  description = "ARN of the Lambda job IAM role"
  value       = aws_iam_role.lambda.arn
}

output "lambda_function_name" {
  description = "Name of the Lambda SAS job function"
  value       = aws_lambda_function.sas_job.function_name
}

output "pandas_layer_arn" {
  description = "ARN of the pandas Lambda layer"
  value       = aws_lambda_layer_version.pandas.arn
}
