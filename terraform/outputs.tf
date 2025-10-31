# outputs.tf - Output values for the Flight Data Analysis infrastructure

# ============================================
# S3 Outputs
# ============================================

output "s3_bucket_name" {
  description = "Name of S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "ARN of S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "s3_raw_data_path" {
  description = "S3 path for raw flight data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/raw/"
}

output "s3_processed_data_path" {
  description = "S3 path for processed flight data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/processed/"
}

output "s3_archive_data_path" {
  description = "S3 path for archived flight data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/archive/"
}

output "s3_analytics_data_path" {
  description = "S3 path for analytics results"
  value       = "s3://${aws_s3_bucket.data_lake.id}/analytics/"
}

# ============================================
# DynamoDB Outputs
# ============================================

output "dynamodb_table_name" {
  description = "Name of DynamoDB table"
  value       = aws_dynamodb_table.flights_realtime.name
}

output "dynamodb_table_arn" {
  description = "ARN of DynamoDB table"
  value       = aws_dynamodb_table.flights_realtime.arn
}

# ============================================
# SQS Outputs
# ============================================

output "sqs_queue_url" {
  description = "URL of SQS queue"
  value       = aws_sqs_queue.flight_queue.url
}

output "sqs_queue_arn" {
  description = "ARN of SQS queue"
  value       = aws_sqs_queue.flight_queue.arn
}

output "sqs_queue_name" {
  description = "Name of SQS queue"
  value       = aws_sqs_queue.flight_queue.name
}

# ============================================
# IAM Role Outputs
# ============================================

output "lambda_role_arn" {
  description = "ARN of Lambda IAM role"
  value       = aws_iam_role.lambda_role.arn
}

output "lambda_role_name" {
  description = "Name of Lambda IAM role"
  value       = aws_iam_role.lambda_role.name
}

output "glue_role_arn" {
  description = "ARN of Glue IAM role"
  value       = aws_iam_role.glue_role.arn
}

output "glue_role_name" {
  description = "Name of Glue IAM role"
  value       = aws_iam_role.glue_role.name
}

# ============================================
# Infrastructure Summary
# ============================================

output "infrastructure_summary" {
  description = "Summary of deployed infrastructure"
  value = {
    s3_bucket              = aws_s3_bucket.data_lake.id
    s3_raw_path            = "s3://${aws_s3_bucket.data_lake.id}/raw/"
    s3_processed_path      = "s3://${aws_s3_bucket.data_lake.id}/processed/"
    s3_archive_path        = "s3://${aws_s3_bucket.data_lake.id}/archive/"
    s3_analytics_path      = "s3://${aws_s3_bucket.data_lake.id}/analytics/"
    dynamodb_table         = aws_dynamodb_table.flights_realtime.name
    sqs_queue_url          = aws_sqs_queue.flight_queue.url
    sqs_queue_name         = aws_sqs_queue.flight_queue.name
    lambda_role            = aws_iam_role.lambda_role.name
    glue_role              = aws_iam_role.glue_role.name
    region                 = var.aws_region
    project_name           = var.project_name
    environment            = var.environment
  }
}

# ============================================
# Verification Commands
# ============================================

output "verification_commands" {
  description = "Commands to verify the infrastructure"
  value = {
    check_s3       = "aws s3 ls s3://${aws_s3_bucket.data_lake.id}/ --region ${var.aws_region}"
    check_sqs      = "aws sqs list-queues --region ${var.aws_region}"
    check_dynamodb = "aws dynamodb list-tables --region ${var.aws_region}"
    check_lambda   = "aws lambda list-functions --region ${var.aws_region}"
  }
}