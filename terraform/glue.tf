# ============================================
# FILE: glue.tf
# AWS Glue Job Configuration
# ============================================

# ============================================
# Upload Glue Script to S3
# ============================================

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "glue-scripts/flights_glue_job.py"
  source = "${path.module}/flights_glue_job.py"
  etag   = filemd5("${path.module}/flights_glue_job.py")

  tags = {
    Name        = "Glue Job Script"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}

# ============================================
# Glue Job - Process S3 Raw to Processed
# ============================================

resource "aws_glue_job" "flights_job" {
  name              = "${var.project_name}-glue-job-${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/glue-scripts/flights_glue_job.py"
    python_version  = "3"
  }

  # Default arguments
  default_arguments = {
  "--TempDir"             = "s3://${aws_s3_bucket.data_lake.id}/temp/"
  "--job-language"        = "python"
  "--job-bookmark-option" = "job-bookmark-disable"   # ← FIXED
  "--S3_INPUT_PATH"       = "s3://${aws_s3_bucket.data_lake.id}/raw/"
  "--S3_OUTPUT_PATH"      = "s3://${aws_s3_bucket.data_lake.id}/processed/"
}

  # Job configuration
  glue_version       = "4.0"
  worker_type        = "G.1X"
  number_of_workers  = 2
  timeout            = 2880  # 48 minutes
  max_retries        = 0

  tags = {
    Name        = "Flight Glue Job"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy_attachment.glue_s3,
    aws_s3_object.glue_script
  ]
}

# ============================================
# Glue Trigger (Optional - For Scheduling)
# ============================================

# Uncomment this if you want to trigger on schedule
# For now, we'll trigger manually or via Airflow

# resource "aws_glue_trigger" "flights_trigger" {
#   name            = "${var.project_name}-trigger-${var.environment}"
#   type            = "SCHEDULED"
#   schedule        = "cron(0 3 * * ? *)"  # Daily at 3 AM UTC
#   start_on_creation = true
#
#   actions {
#     job_name = aws_glue_job.flights_job.name
#   }
#
#   depends_on = [aws_glue_job.flights_job]
# }

# ============================================
# CloudWatch Alarms for Glue Job
# ============================================

resource "aws_cloudwatch_metric_alarm" "glue_job_failures" {
  alarm_name          = "${var.project_name}-glue-failures-${var.environment}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alert when Glue job has failures"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = aws_glue_job.flights_job.name
  }

  tags = {
    Name        = "Glue Job Failure Alarm"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_job_timeout" {
  alarm_name          = "${var.project_name}-glue-timeout-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.executorRunTime"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Maximum"
  threshold           = 2400000  # 40 minutes in milliseconds
  alarm_description   = "Alert when Glue job execution time is too long"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = aws_glue_job.flights_job.name
  }

  tags = {
    Name        = "Glue Job Timeout Alarm"
    Project     = var.project_name
    Environment = var.environment
    CreatedBy   = "Terraform"
  }
}

# ============================================
# Outputs for Glue Job
# ============================================

output "glue_job_name" {
  description = "Name of the Glue job"
  value       = aws_glue_job.flights_job.name
}

output "glue_job_arn" {
  description = "ARN of the Glue job"
  value       = aws_glue_job.flights_job.arn
}

output "glue_script_path" {
  description = "S3 path to Glue script"
  value       = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_script.key}"
}
